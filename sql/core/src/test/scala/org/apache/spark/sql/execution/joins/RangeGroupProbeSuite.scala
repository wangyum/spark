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
import org.apache.spark.memory.{MemoryTestingUtils, TaskMemoryManager, TestMemoryManager}
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
  private val bufferLo = AttributeReference("b_lo", LongType)()
  private val bufferHi = AttributeReference("b_hi", LongType)()

  private def pointInRangeSpec(
      threshold: Int,
      maxRows: Option[Int] = None,
      maxBytes: Option[Long] = None): RangeGroupProbe.Spec = {
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, threshold)
    maxRows.foreach(conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_MAX_ROWS, _))
    maxBytes.foreach(conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_MAX_BYTES, _))
    val condition = Some(bufferP >= streamLo && bufferP <= streamHi)
    RangeGroupProbe.createSpec(
      condition,
      Inner,
      Seq(streamId),
      Seq(bufferId),
      Seq(streamId, streamLo, streamHi),
      Seq(bufferId, bufferP),
      conf).getOrElse(fail("expected a recognized point-in-range spec"))
  }

  /**
   * A standalone `RangeIndexMemoryConsumer` backed by a `TestMemoryManager` capped at `budget`
   * bytes, independent of the real `SparkContext`-backed TaskMemoryManager used for the row
   * array, so memory-pressure tests can constrain the index's own budget precisely.
   */
  private def constrainedConsumer(budget: Long): RangeIndexMemoryConsumer = {
    val mm = new TestMemoryManager(new SparkConf(false))
    mm.limit(budget)
    new RangeIndexMemoryConsumer(new TaskMemoryManager(mm, 0))
  }

  private def pointInIntervalSpec(threshold: Int): RangeGroupProbe.Spec = {
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, threshold)
    val condition = Some(streamLo >= bufferLo && streamLo <= bufferHi)
    RangeGroupProbe.createSpec(
      condition,
      Inner,
      Seq(streamId),
      Seq(bufferId),
      Seq(streamId, streamLo),
      Seq(bufferId, bufferLo, bufferHi),
      conf).getOrElse(fail("expected a recognized point-in-interval spec"))
  }

  private def partialRangeSpec(threshold: Int): RangeGroupProbe.Spec = {
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, threshold)
    val condition = Some(streamLo < bufferP)
    RangeGroupProbe.createSpec(
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
      val handle = RangeGroupProbe.build(array, pointInRangeSpec(threshold = 2), consumer)
      assert(handle == null,
        "a spilled group must never be materialized into an in-memory range index")
    }
  }

  test("build indexes an in-memory group above the row threshold and probes correctly") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, pointInRangeSpec(threshold = 2), consumer)
      assert(handle != null)
      assert(handle.relation.estimatedSize > 0)
      assert(handle.allocatedBytes >= handle.relation.estimatedSize,
        "the TaskMemoryManager allocation must cover the relation's real footprint")

      // Probe [25, 55]: should match points 30, 40, 50 (3 matches)
      val candidates = handle.candidates(InternalRow(1L, 25L, 55L)).toSeq
      assert(candidates.length == 3)
      assert(candidates.map(_.getLong(1)) == Seq(30L, 40L, 50L))

      RangeGroupProbe.release(handle, consumer)
    }
  }

  test("build indexes buffered intervals and probes point-in-interval correctly") {
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
        1000,
        Long.MaxValue,
        1000,
        Long.MaxValue)
      val proj = UnsafeProjection.create(Array[DataType](LongType, LongType, LongType))
      // Add 10 non-overlapping spans: [0, 9], [10, 19], ..., [90, 99]
      (0 until 10).foreach { i =>
        array.add(proj(InternalRow(1L, i.toLong * 10L, i.toLong * 10L + 9L)))
      }
      val consumer = new RangeIndexMemoryConsumer(taskContext.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, pointInIntervalSpec(threshold = 2), consumer)
      assert(handle != null)
      assert(handle.relation.estimatedSize > 0)

      // Probe point 25: falls in [20, 29]
      val candidates = handle.candidates(InternalRow(1L, 25L)).toSeq
      assert(candidates.length == 1)
      assert(candidates.head.getLong(1) == 20L)
      assert(candidates.head.getLong(2) == 29L)

      RangeGroupProbe.release(handle, consumer)
      array.clear()
    } finally {
      TaskContext.unset()
    }
  }

  test("partial range probe operates correctly") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, partialRangeSpec(threshold = 2), consumer)
      assert(handle != null)

      // streamLo < bufferP: for streamLo = 65, buffer points > 65 are 70, 80, 90 (3 matches)
      val candidates = handle.candidates(InternalRow(1L, 65L)).toSeq
      assert(candidates.length == 3)
      assert(candidates.map(_.getLong(1)) == Seq(70L, 80L, 90L))

      RangeGroupProbe.release(handle, consumer)
    }
  }

  test("build skips the index when the group exceeds maxRows even though in-memory") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val spec = pointInRangeSpec(threshold = 2, maxRows = Some(5))
      val handle = RangeGroupProbe.build(array, spec, consumer)
      assert(handle == null,
        "a group exceeding maxRows must fall back to a linear scan rather than being indexed")
    }
  }

  test("build skips the index when the nominal estimate exceeds maxBytes") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val spec = pointInRangeSpec(threshold = 2, maxBytes = Some(1L))
      val handle = RangeGroupProbe.build(array, spec, consumer)
      assert(handle == null,
        "a group whose nominal per-row estimate exceeds maxBytes must fall back to a linear scan")
    }
  }

  test("build releases and falls back when the real footprint exceeds maxBytes") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val spec = pointInRangeSpec(threshold = 2)
      // First build with a generous budget to learn the relation's real (SizeEstimator-based)
      // footprint, which the cheap nominal per-row estimate does not predict exactly.
      val probeConsumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val probeHandle = RangeGroupProbe.build(array, spec, probeConsumer)
      assert(probeHandle != null)
      val actualBytes = probeHandle.relation.estimatedSize
      RangeGroupProbe.release(probeHandle, probeConsumer)

      // Rebuild with maxBytes capped just below the real footprint: even with plenty of
      // TaskMemoryManager budget available, the post-build cap check must reject it.
      val cappedSpec = pointInRangeSpec(threshold = 2, maxBytes = Some(actualBytes - 1))
      val consumer = new RangeIndexMemoryConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, cappedSpec, consumer)
      assert(handle == null,
        "a relation whose real footprint exceeds maxBytes must be released and fall back, " +
          "even when the nominal pre-admission estimate did not predict the overage")
    }
  }

  test("build releases and falls back when TaskMemoryManager cannot grant the true-up") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, _) =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val spec = pointInRangeSpec(threshold = 2)
      val nominalBytes = spec.estimateMemoryBytes(array.length)
      // Budget exactly covers the nominal estimate, so the first acquireMemory succeeds, but
      // leaves nothing available for the true-up once the real footprint is measured.
      val consumer = constrainedConsumer(budget = nominalBytes)
      val handle = RangeGroupProbe.build(array, spec, consumer)
      assert(handle == null,
        "when the real relation footprint exceeds the nominal estimate and the consumer has " +
          "no more memory to grant, the index must be released and the group must fall back")
    }
  }
}
