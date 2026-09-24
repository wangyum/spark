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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._

/**
 * Unit tests for the two range-join indexes. [[IntervalIndex]] is the sweep used by
 * point-in-range. [[PointIndex]] is the sorted array used by a single inequality.
 * Operator-level behavior is covered by `RangeJoinSuite`.
 */
class RangeIndexSuite extends SparkFunSuite {

  private val intOrdering: Ordering[Any] = PhysicalDataType.ordering(IntegerType)

  private def row(id: Int): InternalRow = InternalRow(id)

  private def interval(lo: Int, hi: Int, id: Int): Seq[RangeIndex.RangeEvent] = {
    val stored = row(id)
    RangeIndex.RangeEvent(lo, RangeIndex.RangeEvent.Start, stored, id) ::
      RangeIndex.RangeEvent(hi, RangeIndex.RangeEvent.End, stored, id) :: Nil
  }

  private def point(v: Int, id: Int): Seq[RangeIndex.RangeEvent] =
    RangeIndex.RangeEvent(v, RangeIndex.RangeEvent.Point, row(id), id) :: Nil

  private def buildIntervals(events: Seq[RangeIndex.RangeEvent]): IntervalIndex =
    IntervalIndex.build(intOrdering, events.toArray)

  private def ids(rows: Iterator[InternalRow]): List[Int] = rows.map(_.getInt(0)).toList

  private def probeIds(index: IntervalIndex, low: Any, high: Any): List[Int] =
    ids(index.overlapping(low, high))

  test("toRangeEvent: point, interval, reversed, and null") {
    val getters = List(RangeIndex.getValue(IntegerType, 0), RangeIndex.getValue(IntegerType, 1))
    val identity = new Projection { override def apply(input: InternalRow): InternalRow = input }
    val eventifier = RangeIndex.toRangeEvent(getters, identity, intOrdering)

    assert(eventifier(InternalRow(5, 5), 0) ==
      Seq(RangeIndex.RangeEvent(5, RangeIndex.RangeEvent.Point, InternalRow(5, 5), 0)))
    assert(eventifier(InternalRow(1, 3), 1) ==
      Seq(
        RangeIndex.RangeEvent(1, RangeIndex.RangeEvent.Start, InternalRow(1, 3), 1),
        RangeIndex.RangeEvent(3, RangeIndex.RangeEvent.End, InternalRow(1, 3), 1)))
    assert(eventifier(InternalRow(3, 1), 2).isEmpty)
    assert(eventifier(InternalRow(null, 1), 3).isEmpty)
  }

  test("getValue rejects a type the index does not compare") {
    val thrown = intercept[SparkException] {
      RangeIndex.getValue(ArrayType(IntegerType), 0)
    }
    assert(thrown.getCondition == "INTERNAL_ERROR")
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

  test("interval index sorts unsorted events before sweeping") {
    // id 0 covers [10, 20], id 1 covers [0, 5]. Events arrive out of order.
    val events = Seq(
      RangeIndex.RangeEvent(10, RangeIndex.RangeEvent.Start, row(0), 0),
      RangeIndex.RangeEvent(0, RangeIndex.RangeEvent.Start, row(1), 1),
      RangeIndex.RangeEvent(5, RangeIndex.RangeEvent.End, row(1), 1),
      RangeIndex.RangeEvent(20, RangeIndex.RangeEvent.End, row(0), 0))
    val index = buildIntervals(events)
    assert(probeIds(index, 3, 3) == List(1))
    assert(probeIds(index, 15, 15) == List(0))
  }

  test("interval index returns spanning and activated rows") {
    val index = buildIntervals(interval(0, 100, 0) ++ interval(50, 200, 1))
    assert(probeIds(index, 50, 50).toSet == Set(0, 1))
    assert(probeIds(index, 25, 25) == List(0))
    assert(probeIds(index, 150, 150) == List(1))
  }

  test("interval index returns each nested interval once") {
    val events = interval(0, 100, 0) ++ interval(10, 20, 1) ++
      interval(10, 50, 2) ++ interval(90, 110, 3)
    val index = buildIntervals(events)
    def assertOnce(low: Int, expected: Set[Int]): Unit = {
      val actual = probeIds(index, low, low)
      assert(actual.toSet == expected, s"probe $low -> $actual")
      assert(actual.size == expected.size, s"probe $low duplicated $actual")
    }
    assertOnce(0, Set(0))
    assertOnce(10, Set(0, 1, 2))
    assertOnce(15, Set(0, 1, 2))
    assertOnce(30, Set(0, 2))
    assertOnce(95, Set(0, 3))
    assertOnce(105, Set(3))
    assertOnce(200, Set.empty)
  }

  test("interval index on empty input returns no rows") {
    assert(probeIds(buildIntervals(Seq.empty), 1, 1).isEmpty)
  }

  test("interval index treats null bounds as non-matching") {
    val index = buildIntervals(interval(0, 10, 0) ++ point(5, 1))
    assert(probeIds(index, null, 5).isEmpty)
    assert(probeIds(index, 5, null).isEmpty)
  }

  test("interval index keeps both the spanning row and the row activated at the probe") {
    // Probe 50 lands on a build key. Row 0 spans that key; row 1 starts there.
    // Both are candidates. The join condition decides inclusivity.
    val events = Seq(
      RangeIndex.RangeEvent(0, RangeIndex.RangeEvent.Start, row(0), 0),
      RangeIndex.RangeEvent(100, RangeIndex.RangeEvent.End, row(0), 0),
      RangeIndex.RangeEvent(50, RangeIndex.RangeEvent.Start, row(1), 1),
      RangeIndex.RangeEvent(200, RangeIndex.RangeEvent.End, row(1), 1))
    val result = probeIds(buildIntervals(events), 50, 50)
    assert(result.contains(0), s"expected spanning row 0, got $result")
    assert(result.contains(1), s"expected row 1 activated at 50, got $result")
  }

  test("chained intervals: start key is a candidate once, end key is a candidate once") {
    // (0,1), (1,2), (2,3). Probe at an interior endpoint must see the range that
    // ends there and the range that starts there, each once.
    val index = buildIntervals(interval(0, 1, 0) ++ interval(1, 2, 1) ++ interval(2, 3, 2))
    assert(probeIds(index, 0, 0) == List(0))
    assert(probeIds(index, 1, 1).sorted == List(0, 1))
    assert(probeIds(index, 2, 2).sorted == List(1, 2))
    assert(probeIds(index, 3, 3) == List(2))
  }

  test("interval index size counts each activated row once") {
    val index = buildIntervals(interval(0, 10, 0) ++ point(5, 1))
    assert(index.sizeInBytes() == 16L)
  }

  test("point index answers both sides of a bound, including equals") {
    // Unsorted, with two rows on key 5. Equals stay, in input order.
    val keyed = Array[(Any, InternalRow)](
      (8, row(8)),
      (1, row(1)),
      (5, row(5)),
      (5, row(50)),
      (3, row(3)),
      (null, row(-1)))
    val index = PointIndex.build(intOrdering, keyed)
    assert(ids(index.upTo(5)) == List(1, 3, 5, 50))
    assert(ids(index.upTo(4)) == List(1, 3))
    assert(ids(index.upTo(0)).isEmpty)
    assert(ids(index.upTo(8)) == List(1, 3, 5, 50, 8))
    assert(ids(index.from(5)) == List(5, 50, 8))
    assert(ids(index.from(6)) == List(8))
    assert(ids(index.from(9)).isEmpty)
    assert(ids(index.from(1)) == List(1, 3, 5, 50, 8))
    assert(index.upTo(null).isEmpty)
    assert(index.from(null).isEmpty)
    assert(index.sizeInBytes() == 40L)
  }

  test("point index on empty input returns no rows") {
    val index = PointIndex.build(intOrdering, Array.empty)
    assert(index.upTo(1).isEmpty)
    assert(index.from(1).isEmpty)
    assert(index.sizeInBytes() == 0L)
  }
}
