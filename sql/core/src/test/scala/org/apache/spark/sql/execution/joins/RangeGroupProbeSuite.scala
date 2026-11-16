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
  private val streamP = AttributeReference("e_p", LongType)()
  private val bufferId = AttributeReference("s_id", LongType)()
  private val bufferLo = AttributeReference("lo", LongType)()
  private val bufferHi = AttributeReference("hi", LongType)()

  private def pointInRangeSpec(threshold: Int): RangeGroupProbe.Spec = {
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, threshold)
    val condition = Some(streamP >= bufferLo && streamP <= bufferHi)
    SortMergeJoinRangePhysical.createProbeSpec(
      condition,
      Inner,
      Seq(streamId),
      Seq(bufferId),
      Seq(streamId, streamP),
      Seq(bufferId, bufferLo, bufferHi),
      conf).getOrElse(fail("expected a recognized point-in-range spec"))
  }

  /** Builds an array with `totalRows` identical rows, switching to spill mode after
   * `inMemoryRows` rows. */
  private def withRowArray(inMemoryRows: Int, totalRows: Int)
      (f: ExternalAppendOnlyUnsafeRowArray => Unit): Unit = {
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
        val proj = UnsafeProjection.create(Array[DataType](LongType, LongType, LongType))
        (0 until totalRows).foreach { _ =>
          array.add(proj(InternalRow(1L, 0L, 100L)))
        }
        f(array)
      } finally {
        array.clear()
      }
    } finally {
      TaskContext.unset()
    }
  }

  test("build skips the index for a spilled group even above the row threshold") {
    // inMemoryRows (4) is well below totalRows (10), so this array is really spill-backed;
    // threshold (2) is below totalRows too, so without the isSpillBacked gate, build() would
    // materialize an index over that spilled data.
    withRowArray(inMemoryRows = 4, totalRows = 10) { array =>
      assert(array.isSpillBacked, "test setup must force a real spill")
      val handle = RangeGroupProbe.build(array, pointInRangeSpec(threshold = 2))
      assert(handle.relation.isEmpty,
        "a spilled group must never be re-materialized into an in-memory range index")
      assert(handle.candidateFn.isEmpty)
    }
  }

  test("build still indexes an in-memory group above the row threshold") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { array =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val handle = RangeGroupProbe.build(array, pointInRangeSpec(threshold = 2))
      assert(handle.relation.isDefined)
      assert(handle.candidateFn.isDefined)
    }
  }
}
