/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{
  GreaterPartialRangeJoin, LessPartialRangeJoin, PointInRangeJoin, RangeJoin}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._

/**
 * Unit tests for [[RangeIndex]] itself: event construction, sweep-line build, and the
 * `intersect` probe. These do not need a `SparkSession`; operator-level behavior
 * (bound checks, residual) is covered by `RangeJoinSuite`.
 */
class RangeIndexSuite extends SparkFunSuite {

  private val intOrdering: Ordering[Any] = PhysicalDataType.ordering(IntegerType)

  private def row(v: Int): InternalRow = InternalRow(v)

  private def interval(lo: Int, hi: Int, idx: Int): Seq[RangeIndex.RangeEvent] =
    RangeIndex.RangeEvent(lo, 1, row(lo), idx) ::
      RangeIndex.RangeEvent(hi, -1, row(lo), idx) :: Nil

  private def point(v: Int, idx: Int): Seq[RangeIndex.RangeEvent] =
    RangeIndex.RangeEvent(v, 0, row(v), idx) :: Nil

  private def buildIndex(
      events: Seq[RangeIndex.RangeEvent],
      rangeJoin: RangeJoin = PointInRangeJoin,
      buildSide: BuildSide = BuildLeft): RangeIndex =
    RangeIndex.build(intOrdering, events.toArray, rangeJoin, buildSide)

  private def probeIds(index: RangeIndex, low: Any, high: Any): List[Int] =
    index.intersect(low, high).map(_._2).toList

  test("toRangeEvent: point, interval, reversed, and null") {
    val getters = List(RangeIndex.getValue(IntegerType, 0), RangeIndex.getValue(IntegerType, 1))
    val identity = new Projection { override def apply(input: InternalRow): InternalRow = input }
    val eventifier = RangeIndex.toRangeEvent(getters, identity, intOrdering)

    assert(eventifier(InternalRow(5, 5), 0) ==
      Seq(RangeIndex.RangeEvent(5, 0, InternalRow(5, 5), 0)))
    assert(eventifier(InternalRow(1, 3), 1) ==
      Seq(
        RangeIndex.RangeEvent(1, 1, InternalRow(1, 3), 1),
        RangeIndex.RangeEvent(3, -1, InternalRow(1, 3), 1)))
    assert(eventifier(InternalRow(3, 1), 2).isEmpty)
    assert(eventifier(InternalRow(null, 1), 3).isEmpty)
  }

  test("getValue: orderable atomic types") {
    assert(RangeIndex.getValue(IntegerType, 0)(InternalRow(7)) == 7)
    assert(RangeIndex.getValue(LongType, 0)(InternalRow(7L)) == 7L)
    assert(RangeIndex.getValue(DateType, 0)(InternalRow(100)) == 100)
    assert(RangeIndex.getValue(StringType, 0)(
      InternalRow(org.apache.spark.unsafe.types.UTF8String.fromString("a"))) ==
      org.apache.spark.unsafe.types.UTF8String.fromString("a"))
    assert(RangeIndex.getValue(IntegerType, 0)(InternalRow(null)) == null)
  }

  test("build sorts unsorted events before sweeping") {
    val events = Seq(
      RangeIndex.RangeEvent(10, 1, row(10), 0),
      RangeIndex.RangeEvent(0, 1, row(0), 1),
      RangeIndex.RangeEvent(5, -1, row(0), 1),
      RangeIndex.RangeEvent(20, -1, row(10), 0))
    val index = buildIndex(events)
    assert(probeIds(index, 3, 3) == List(1))
    assert(probeIds(index, 15, 15) == List(0))
  }

  test("intersect returns spanning and activated rows") {
    val events = interval(0, 100, 0) ++ interval(50, 200, 1)
    val index = buildIndex(events)
    assert(probeIds(index, 50, 50).toSet == Set(0, 1))
    assert(probeIds(index, 25, 25) == List(0))
    assert(probeIds(index, 150, 150) == List(1))
  }

  test("intersect on empty index returns no rows") {
    val index = buildIndex(Seq.empty)
    assert(probeIds(index, 1, 1).isEmpty)
  }

  test("intersect treats null bounds as unbounded") {
    val events = interval(0, 10, 0) ++ point(5, 1)
    val index = buildIndex(events)
    assert(probeIds(index, null, 5).toSet == Set(0, 1))
    assert(probeIds(index, 5, null).toSet == Set(0, 1))
  }

  test("partial-range direction: LessPartialRangeJoin vs GreaterPartialRangeJoin") {
    val events = interval(0, 10, 0) ++ interval(20, 30, 1)
    val lessLeft = buildIndex(events, LessPartialRangeJoin, BuildLeft)
    val greaterRight = buildIndex(events, GreaterPartialRangeJoin, BuildRight)
    // LessPartialRangeJoin + BuildLeft: build is the low bound, probe scans from 0.
    assert(probeIds(lessLeft, 5, 5).nonEmpty)
    // GreaterPartialRangeJoin + BuildRight: build is the high bound, probe scans to the end.
    assert(probeIds(greaterRight, 25, 25).nonEmpty)
  }

  test("closestLowerKey boundary: point equal to a build key returns all matching rows") {
    // intersect() always widens to a superset around a probe point that lands exactly on a
    // build-side key (50); the caller is responsible for applying the actual equality
    // semantics (rangeCheck). Both of the following rows must therefore come back as
    // candidates for a probe of 50:
    // RowA [0, 100) spans key 50 and is returned via the active-rows path.
    // RowB [50, 200) is activated at key 50.
    val rowA = InternalRow(0, 100)
    val rowB = InternalRow(50, 200)
    val events = Seq(
      RangeIndex.RangeEvent(0, 1, rowA, 0),
      RangeIndex.RangeEvent(100, -1, rowA, 0),
      RangeIndex.RangeEvent(50, 1, rowB, 1),
      RangeIndex.RangeEvent(200, -1, rowB, 1))
    val index = buildIndex(events)
    val result = probeIds(index, 50, 50)
    assert(result.contains(0), s"expected rowA(0) for spanning range, got $result")
    assert(result.contains(1), s"expected rowB(1) for activated-at-key match, got $result")
  }

  test("sizeInBytes sums activated rows") {
    val events = interval(0, 10, 0) ++ point(5, 1)
    val index = buildIndex(events)
    // Two non-Unsafe rows, one field each.
    assert(index.sizeInBytes() == 16L)
  }
}
