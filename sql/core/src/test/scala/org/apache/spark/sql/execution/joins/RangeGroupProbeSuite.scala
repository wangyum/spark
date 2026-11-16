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

import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType}

class RangeGroupProbeSuite extends SparkFunSuite with LocalSparkContext {

  private val streamId = AttributeReference("e_id", LongType)()
  private val streamLo = AttributeReference("lo", LongType)()
  private val streamHi = AttributeReference("hi", LongType)()
  private val bufferId = AttributeReference("s_id", LongType)()
  private val bufferP = AttributeReference("s_p", LongType)()

  private def pointInRangeSpec(threshold: Int): RangeGroupProbe.Spec = {
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, threshold)
    val condition = Some(bufferP >= streamLo && bufferP <= streamHi)
    SortMergeJoinRangePhysical.createProbeSpec(
      condition,
      Inner,
      Seq(streamId),
      Seq(bufferId),
      Seq(streamId, streamLo, streamHi),
      Seq(bufferId, bufferP),
      conf).getOrElse(fail("expected a recognized point-in-range spec"))
  }

  private def partialRangeSpec(threshold: Int): RangeGroupProbe.Spec = {
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, threshold)
    val condition = Some(streamLo < bufferP)
    SortMergeJoinRangePhysical.createProbeSpec(
      condition,
      Inner,
      Seq(streamId),
      Seq(bufferId),
      Seq(streamId, streamLo),
      Seq(bufferId, bufferP),
      conf).getOrElse(fail("expected a recognized partial-range spec"))
  }

  /** Builds an array with `totalRows` identical rows, switching to spill mode after
   * `inMemoryRows` rows. */
  private def withRowArray(inMemoryRows: Int, totalRows: Int)
      (f: (ExternalAppendOnlyUnsafeRowArray, TaskContext) => Unit): Unit = {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)
    try {
      val array = new ExternalAppendOnlyUnsafeRowArray(
        taskContext.taskMemoryManager(),
        SparkEnv.get.blockManager,
        SparkEnv.get.serializerManager,
        taskContext,
        1024,
        SparkEnv.get.memoryManager.pageSizeBytes,
        inMemoryRows,
        Long.MaxValue,
        1,
        Long.MaxValue)
      try {
        val proj = UnsafeProjection.create(Array[DataType](LongType, LongType))
        (0 until totalRows).foreach { i =>
          array.add(proj(InternalRow(1L, i.toLong * 10L)))
        }
        f(array, taskContext)
      } finally {
        array.clear()
      }
    } finally {
      TaskContext.unset()
    }
  }

  test("build skips the index for a spilled group even above the row threshold") {
    withRowArray(inMemoryRows = 4, totalRows = 10) { (array, tc) =>
      assert(array.isSpillBacked, "test setup must force a real spill")
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(
        array, pointInRangeSpec(threshold = 2), tc.taskMemoryManager(), consumer)
      assert(handle == null,
        "a spilled group must never be materialized into an in-memory range index")
    }
  }

  test("build indexes an in-memory group above the row threshold and probes correctly") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(
        array, pointInRangeSpec(threshold = 2), tc.taskMemoryManager(), consumer)
      assert(handle != null)
      assert(handle.index.length == 10)

      // Probe [25, 55]: should match points 30, 40, 50 (3 matches)
      val candidates = handle.candidates(InternalRow(1L, 25L, 55L)).toSeq
      assert(candidates.length == 3)
      assert(candidates.map(_.getLong(1)) == Seq(30L, 40L, 50L))

      RangeGroupProbe.release(handle, tc.taskMemoryManager(), consumer)
    }
  }

  test("partial range probe operates correctly") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(
        array, partialRangeSpec(threshold = 2), tc.taskMemoryManager(), consumer)
      assert(handle != null)

      // streamLo < bufferP: for streamLo = 65, buffer points > 65 are 70, 80, 90 (3 matches)
      val candidates = handle.candidates(InternalRow(1L, 65L)).toSeq
      assert(candidates.length == 3)
      assert(candidates.map(_.getLong(1)) == Seq(70L, 80L, 90L))

      RangeGroupProbe.release(handle, tc.taskMemoryManager(), consumer)
    }
  }
}
