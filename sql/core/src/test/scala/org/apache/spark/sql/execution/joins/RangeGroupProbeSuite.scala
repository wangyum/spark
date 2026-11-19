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
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, MemoryTestingUtils, TaskMemoryManager,
  TestMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, RowOrdering, UnsafeProjection}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, RowIterator}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType, StringType}

class RangeGroupProbeSuite extends SparkFunSuite with LocalSparkContext {

  private val streamId = AttributeReference("e_id", LongType)()
  private val streamLo = AttributeReference("lo", LongType)()
  private val streamHi = AttributeReference("hi", LongType)()
  private val bufferId = AttributeReference("s_id", LongType)()
  private val bufferP = AttributeReference("s_p", LongType)()
  private val bufferLo = AttributeReference("b_lo", LongType)()
  private val bufferHi = AttributeReference("b_hi", LongType)()

  /** Extra buffered-side columns a narrow fixture doesn't have, to simulate a wide schema. */
  private val wideExtraCols: Seq[Attribute] =
    (1 to 20).map(i => AttributeReference(s"w$i", StringType)())

  private def pointInRangeSpecWithBufferOutput(
      threshold: Int,
      bufferOutput: Seq[Attribute],
      maxBytes: Option[Long] = None): RangeGroupProbe.Spec = {
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, threshold)
    maxBytes.foreach(conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_MAX_BYTES, _))
    val condition = Some(bufferP >= streamLo && bufferP <= streamHi)
    RangeGroupProbe.createSpec(
      condition,
      Inner,
      Seq(streamId),
      Seq(bufferId),
      Seq(streamId, streamLo, streamHi),
      bufferOutput,
      BuildRight,
      conf).getOrElse(fail("expected a recognized point-in-range spec"))
  }

  private def pointInRangeSpec(
      threshold: Int,
      maxBytes: Option[Long] = None): RangeGroupProbe.Spec =
    pointInRangeSpecWithBufferOutput(threshold, Seq(bufferId, bufferP), maxBytes)

  /** Probes `array` `PROBE_THRESHOLD` times, leaving `session` with an indexed group. */
  private def probeThroughGate(
      session: RangeGroupProbe.Session,
      array: ExternalAppendOnlyUnsafeRowArray): Unit = {
    var i = 0
    while (i < RangeGroupProbe.PROBE_THRESHOLD) {
      session.onProbe(array)
      i += 1
    }
  }

  /**
   * A `MemoryConsumer` that never voluntarily releases memory on a `spill()` request, standing
   * in for "some other part of the task" competing for `TaskMemoryManager` budget. Every test
   * below that needs a consumer to pass to `RangeGroupProbe.build`/`release` - or to occupy
   * budget `Session`'s own index can't reclaim - only cares that it holds memory, never that it
   * spills, so a single never-spill stub covers all of them.
   */
  private def neverSpillConsumer(taskMemoryManager: TaskMemoryManager): MemoryConsumer =
    new MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) {
      override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
    }

  /**
   * A `neverSpillConsumer` backed by a `TestMemoryManager` capped at `budget` bytes, independent
   * of the real `SparkContext`-backed TaskMemoryManager used for the row array, so memory-
   * pressure tests can constrain the index's own budget precisely.
   */
  private def constrainedConsumer(budget: Long): MemoryConsumer = {
    val mm = new TestMemoryManager(new SparkConf(false))
    mm.limit(budget)
    neverSpillConsumer(new TaskMemoryManager(mm, 0))
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
      BuildRight,
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
      BuildRight,
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

  test("createSpec returns None when a range-predicate key is also an equi-join key") {
    // e_id = s_id AND e_id BETWEEN b_lo AND b_hi: e_id is both the equi-join key and a
    // range-predicate key, so the range index must not engage (its probe key would double
    // count an already equi-bound attribute).
    val conf = new SQLConf
    conf.setConf(SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD, 2)
    val condition = Some(streamId >= bufferLo && streamId <= bufferHi)
    val spec = RangeGroupProbe.createSpec(
      condition,
      Inner,
      Seq(streamId),
      Seq(bufferId),
      Seq(streamId),
      Seq(bufferId, bufferLo, bufferHi),
      BuildRight,
      conf)
    assert(spec.isEmpty, "expected no spec: the range key overlaps an equi-join key")
  }

  test("build skips the index for a spilled group even above the row threshold") {
    // Unit-level on purpose: driving this through SortMergeJoinExec would need the group
    // probed past the gate, and each probe of a spill-backed group allocates a spill reader
    // with at least a 1 MB read-ahead buffer - more churn than the test JVM's heap
    // tolerates reliably. The attempted-then-rejected wiring that counts a fallback metric
    // is covered end to end by SortMergeJoinRangeIndexSuite's maxBytes test.
    withRowArray(inMemoryRows = 4, totalRows = 10) { (array, tc) =>
      assert(array.isSpillBacked, "test setup must force a real spill")
      val consumer = neverSpillConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, pointInRangeSpec(threshold = 2), consumer)
      assert(handle == null,
        "a spilled group must never be materialized into an in-memory range index")
    }
  }

  test("build skips the index for a group one row short of the threshold") {
    withRowArray(inMemoryRows = 1000, totalRows = 9) { (array, tc) =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val consumer = neverSpillConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, pointInRangeSpec(threshold = 10), consumer)
      assert(handle == null,
        "a group of threshold - 1 rows must fall back to a linear scan")
    }
  }

  test("build indexes a group exactly at the threshold") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val consumer = neverSpillConsumer(tc.taskMemoryManager())
      // The config's doc says "minimum ... row count", so a group of exactly `threshold` rows
      // (not threshold + 1) must already be eligible.
      val handle = RangeGroupProbe.build(array, pointInRangeSpec(threshold = 10), consumer)
      assert(handle != null, "a group of exactly threshold rows must be indexed")
      RangeGroupProbe.release(handle, consumer)
    }
  }

  test("build indexes an in-memory group above the row threshold and probes correctly") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")
      val consumer = neverSpillConsumer(tc.taskMemoryManager())
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
      val consumer =
        neverSpillConsumer(taskContext.taskMemoryManager())
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
      val consumer = neverSpillConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, partialRangeSpec(threshold = 2), consumer)
      assert(handle != null)

      // streamLo < bufferP: for streamLo = 65, buffer points > 65 are 70, 80, 90 (3 matches)
      val candidates = handle.candidates(InternalRow(1L, 65L)).toSeq
      assert(candidates.length == 3)
      assert(candidates.map(_.getLong(1)) == Seq(70L, 80L, 90L))

      RangeGroupProbe.release(handle, consumer)
    }
  }

  test("estimateMemoryBytes scales with the buffered side's schema width") {
    val narrowOutput = Seq(bufferId, bufferP)
    val wideOutput = narrowOutput ++ wideExtraCols
    val narrowSpec = pointInRangeSpecWithBufferOutput(threshold = 2, narrowOutput)
    val wideSpec = pointInRangeSpecWithBufferOutput(threshold = 2, wideOutput)

    assert(narrowSpec.bufferRowSize == EstimationUtils.getSizePerRow(narrowOutput).toLong)
    assert(wideSpec.bufferRowSize == EstimationUtils.getSizePerRow(wideOutput).toLong)
    assert(wideSpec.bufferRowSize > narrowSpec.bufferRowSize,
      "test setup must actually widen the buffered schema")

    val n = 100L
    assert(wideSpec.estimateMemoryBytes(n) > narrowSpec.estimateMemoryBytes(n),
      "a wider buffered schema must raise the pre-admission memory estimate, since each index " +
        "entry retains a full copy of the buffered row rather than a fixed per-row constant")
  }

  test("build rejects a wide buffered schema whose estimate exceeds maxBytes, without " +
    "ever reaching the point a flat per-row estimate would have wrongly admitted") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val wideOutput = Seq(bufferId, bufferP) ++ wideExtraCols
      // What the old, schema-oblivious 64-bytes/row (IntervalIndexKind) estimate would have
      // reported as the admission cost for this row count - a flat heuristic would have
      // admitted this group as long as maxBytes covers that number, regardless of how wide
      // the buffered side's actual schema is.
      val oldFlatEstimate = array.length * 64L
      val maxBytes = oldFlatEstimate + 1L
      val spec = pointInRangeSpecWithBufferOutput(
        threshold = 2, wideOutput, maxBytes = Some(maxBytes))

      assert(spec.estimateMemoryBytes(array.length) > maxBytes,
        "test setup must pick a schema wide enough that the schema-aware estimate exceeds " +
          "what the old flat per-row estimate would have reported")

      val consumer = neverSpillConsumer(tc.taskMemoryManager())
      val handle = RangeGroupProbe.build(array, spec, consumer)
      assert(handle == null,
        "a wide buffered schema must be rejected by the pre-admission check, even though the " +
          "old flat per-row estimate would have wrongly admitted it into a build attempt")
    }
  }

  test("build skips the index when the nominal estimate exceeds maxBytes") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val consumer = neverSpillConsumer(tc.taskMemoryManager())
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
      val probeConsumer = neverSpillConsumer(tc.taskMemoryManager())
      val probeHandle = RangeGroupProbe.build(array, spec, probeConsumer)
      assert(probeHandle != null)
      val actualBytes = probeHandle.relation.estimatedSize
      RangeGroupProbe.release(probeHandle, probeConsumer)

      // Rebuild with maxBytes capped just below the real footprint: even with plenty of
      // TaskMemoryManager budget available, the post-build cap check must reject it.
      val cappedSpec = pointInRangeSpec(threshold = 2, maxBytes = Some(actualBytes - 1))
      val consumer = neverSpillConsumer(tc.taskMemoryManager())
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

  test("Session construction requires an active TaskContext") {
    // Session.apply() calls TaskContext.get() exactly once and fails fast if there isn't one,
    // rather than silently degrading to a null memoryConsumer the way the pre-refactor code
    // did. Every production call site (SortMergeJoinEvaluatorFactory, whole-stage codegen)
    // only ever constructs a Session from inside a running task, so this should never fire
    // in practice - this test exists purely to pin that contract so a future change can't
    // silently weaken it back to null-tolerant degradation.
    TaskContext.unset()
    try {
      val ex = intercept[IllegalArgumentException] {
        RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      }
      assert(ex.getMessage.contains("active Spark task"))
    } finally {
      TaskContext.unset()
    }
  }

  test("Session defers the index build until the probe threshold is reached") {
    // The shipped admission policy: a group must be probed often enough that its accumulated
    // linear rescans approach the index's one-time build cost before the build is attempted.
    // A group reused below that point stays at parity with range indexing disabled.
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, _) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      val gate = RangeGroupProbe.PROBE_THRESHOLD

      var probes = 0
      while (probes < gate - 1) {
        session.onProbe(array)
        probes += 1
        assert(session.currentRelationOrNull == null,
          s"a group probed $probes times, below the gate of $gate, must not build")
      }

      session.onProbe(array)
      assert(session.currentRelationOrNull != null,
        s"the probe reaching the gate of $gate must trigger the deferred build")
    }
  }

  test("Session.candidates falls back to a linear scan below the probe threshold, then " +
    "uses the index from it onward") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, _) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))

      // Every probe below the threshold: candidates must come straight from the array, not
      // an index.
      var probes = 0
      while (probes < RangeGroupProbe.PROBE_THRESHOLD - 1) {
        val linear = session.candidates(array, InternalRow(1L, 25L, 55L)).toSeq
        assert(linear.length == array.length,
          "a probe below the gate must linearly scan every row")
        probes += 1
      }
      assert(session.currentRelationOrNull == null)

      // The next probe reaches the threshold, so the build fires and is answered from
      // the index from now on.
      session.onProbe(array)
      assert(session.currentRelationOrNull != null)
      val indexed = session.candidates(array, InternalRow(1L, 25L, 55L)).toSeq
      assert(indexed.length == 3, "the index-backed probe must only return in-range rows")
    }
  }

  test("a spill-backed group's fallback is recorded on its first probe, not after " +
    "PROBE_THRESHOLD probes") {
    withRowArray(inMemoryRows = 4, totalRows = 10) { (array, _) =>
      assert(array.isSpillBacked, "test setup must force a real spill")
      val indexedMetric = SQLMetrics.createMetric(sc, "indexed")
      val fallbackMetric = SQLMetrics.createMetric(sc, "fallback")
      val session = RangeGroupProbe.Session(
        pointInRangeSpec(threshold = 2), indexedMetric, fallbackMetric)

      session.onProbe(array)
      assert(session.currentRelationOrNull == null, "a spill-backed group must never be indexed")
      assert(fallbackMetric.value == 1,
        "row count and spill state are fixed once buffering completes, so a group already " +
        "ineligible on its first probe must be declined immediately instead of waiting for " +
        "PROBE_THRESHOLD probes to reach the same, already-known outcome")

      // Later probes of the same group must stay no-ops: no repeated build attempt, no
      // repeated metric increment.
      probeThroughGate(session, array)
      assert(fallbackMetric.value == 1, "a declined group must not be re-evaluated later")
      assert(indexedMetric.value == 0)
    }
  }

  test("Session.resetGroup clears probe state so the next group starts fresh") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, _) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null, "the group must be indexed by now")

      session.resetGroup()
      assert(session.currentRelationOrNull == null, "resetGroup must release the index")

      // The next group must re-defer its own build, not immediately reuse the prior group's
      // already-passed probeCount/buildAttempted state.
      session.onProbe(array)
      assert(session.currentRelationOrNull == null,
        "a fresh group's first probe must not build an index")
    }
  }

  test("Session.spill() releases an indexed group, falls back to a linear " +
    "scan, counts it as a fallback, and never rebuilds for the rest of the group") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val indexedMetric = SQLMetrics.createMetric(sc, "indexed")
      val fallbackMetric = SQLMetrics.createMetric(sc, "fallback")
      val session = RangeGroupProbe.Session(
        pointInRangeSpec(threshold = 2), indexedMetric, fallbackMetric)
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null, "the group must be indexed by now")
      assert(indexedMetric.value == 1)
      assert(fallbackMetric.value == 0)

      // Simulate what TaskMemoryManager does when it asks this consumer to give memory back,
      // e.g. because another consumer in the same task needs execution memory.
      val otherConsumer = neverSpillConsumer(tc.taskMemoryManager())
      session.spill(1L, otherConsumer)

      assert(session.currentRelationOrNull == null,
        "the index must be released once the consumer is asked to spill")
      assert(fallbackMetric.value == 1,
        "a spilled, previously-indexed group must be counted as a fallback")
      assert(indexedMetric.value == 1, "the earlier indexed count must not be undone")

      // A further probe of the same group must not attempt to rebuild and re-acquire memory.
      session.onProbe(array)
      assert(session.currentRelationOrNull == null, "the group must not rebuild after a spill")
      assert(indexedMetric.value == 1, "a rebuild would have incremented this again")
      assert(fallbackMetric.value == 1, "a rebuild attempt would have incremented this again")
    }
  }

  test("Session.spill() is a no-op on a self-triggered spill") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, _) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null)

      assert(session.spill(1L, session) == 0L,
        "a self-triggered spill must return 0 without releasing the index")
      assert(session.currentRelationOrNull != null,
        "a self-triggered spill must not release the index")
    }
  }

  test("Session.spill() declines to release while a candidate iterator is " +
    "still in-flight, and executes pending spill once probe completes") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null, "the group must be indexed by now")

      // Mirrors a caller like OneSideOuterIterator.rightMatchesIterator: obtain the candidate
      // iterator and partially consume it, holding a live reference across the spill attempt
      // below instead of draining it in one step.
      val candidates = session.candidates(array, InternalRow(1L, 25L, 55L))
      assert(candidates.hasNext, "test setup needs at least one match to keep the iterator open")
      candidates.next()

      val otherConsumer = neverSpillConsumer(tc.taskMemoryManager())
      assert(session.spill(1L, otherConsumer) == 0L,
        "a spill must not release memory that an in-flight candidate iterator can still reach")
      assert(session.currentRelationOrNull != null,
        "the index must survive a spill request while its candidate iterator is in-flight")

      while (candidates.hasNext) candidates.next()
      assert(session.currentRelationOrNull == null,
        "once the in-flight iterator completes, pending spill must be executed")
    }
  }

  test("Session.finishProbe allows immediate spill after an iterator is abandoned early") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null)

      // Iterator abandoned early without being drained to empty (e.g. LeftSemi):
      val candidates = session.candidates(array, InternalRow(1L, 25L, 55L))
      assert(candidates.hasNext)
      candidates.next()
      session.finishProbe()

      val otherConsumer = neverSpillConsumer(tc.taskMemoryManager())
      assert(session.spill(1L, otherConsumer) > 0L,
        "an early-exited probe must not block subsequent spills")
      assert(session.currentRelationOrNull == null)
    }
  }

  test("Session.beginProbe/finishProbe allows an immediate spill when a probe finds no match") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null, "the group must be indexed by now")

      // Mirrors whole-stage codegen's indexed probe path (SortMergeJoinExec's createIterator):
      // beginProbe() marks the probe in-flight before reading the index directly, with no
      // wrapOutstanding wrapper to auto-detect exhaustion. finishProbe() must still be called
      // once the candidate loop ends even when it never found a match - e.g. LeftSemi finding
      // no matching buffered row at all for this streamed row.
      session.beginProbe()
      session.finishProbe()

      val otherConsumer = neverSpillConsumer(tc.taskMemoryManager())
      assert(session.spill(1L, otherConsumer) > 0L,
        "a probe that found no match must not block a subsequent spill")
      assert(session.currentRelationOrNull == null)
    }
  }

  test("SortMergeJoinScanner.finishProbe forwards to the underlying Session") {
    // Unit-level on SortMergeJoinScanner, not just RangeGroupProbe.Session directly: the
    // Session-level mechanism is covered above, but nothing exercises that the Scanner the
    // real join operators call through actually forwards to it -- which is exactly the kind
    // of gap that let LeftSemi/LeftAnti/ExistenceJoin ship without ever calling finishProbe.
    sc = new SparkContext("local", "test", new SparkConf(false))
    val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)
    try {
      val probeSession = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))

      val streamProj = UnsafeProjection.create(Array[DataType](LongType, LongType, LongType))
      val streamRows = (0 until RangeGroupProbe.PROBE_THRESHOLD).map { _ =>
        streamProj(InternalRow(1L, 25L, 55L)).copy()
      }
      val bufferProj = UnsafeProjection.create(Array[DataType](LongType, LongType))
      // 10 buffered points, matching pointInRangeSpec's condition (s_p in [lo, hi]): probing
      // [25, 55] above must match 30, 40, 50 (3 rows), same as the Session-level tests.
      val bufferRows = (0 until 10).map(i => bufferProj(InternalRow(1L, i.toLong * 10L)).copy())

      val scanner = new SortMergeJoinScanner(
        streamedKeyGenerator =
          UnsafeProjection.create(Seq(streamId), Seq(streamId, streamLo, streamHi)),
        bufferedKeyGenerator = UnsafeProjection.create(Seq(bufferId), Seq(bufferId, bufferP)),
        keyOrdering = RowOrdering.createNaturalAscendingOrdering(Seq(LongType)),
        streamedIter = RowIterator.fromScala(streamRows.iterator),
        bufferedIter = RowIterator.fromScala(bufferRows.iterator),
        inMemoryThreshold = 1000,
        spillThreshold = Int.MaxValue,
        sizeInBytesSpillThreshold = Long.MaxValue,
        spillSize = SQLMetrics.createSizeMetric(sc, "spill"),
        eagerCleanupResources = () => (),
        probeSession = probeSession)

      // Drive the scanner through PROBE_THRESHOLD probes of the same equi-key group, draining
      // each candidate iterator fully, to cross the deferred-build gate.
      var i = 0
      while (i < RangeGroupProbe.PROBE_THRESHOLD - 1) {
        assert(scanner.findNextInnerJoinRows())
        val it = scanner.candidatesFor(scanner.getStreamedRow)
        while (it.hasNext) it.next()
        i += 1
      }
      assert(probeSession.currentRelationOrNull == null, "must not build before the gate")
      assert(scanner.findNextInnerJoinRows())

      // The gate-crossing probe: obtain the candidate iterator and abandon it early, exactly
      // as LeftSemi/LeftAnti/ExistenceJoin do on a match, then signal completion via the
      // Scanner's finishProbe -- not RangeGroupProbe.Session's directly.
      val candidates = scanner.candidatesFor(scanner.getStreamedRow)
      assert(candidates.hasNext, "test setup needs at least one match to keep the iterator open")
      candidates.next()
      assert(probeSession.currentRelationOrNull != null, "the gate-crossing probe must build")
      scanner.finishProbe()

      val otherConsumer = neverSpillConsumer(taskContext.taskMemoryManager())
      assert(probeSession.spill(1L, otherConsumer) > 0L,
        "SortMergeJoinScanner.finishProbe must forward to the Session, allowing an immediate " +
          "spill despite the abandoned, undrained candidate iterator")
      assert(probeSession.currentRelationOrNull == null)
    } finally {
      TaskContext.unset()
    }
  }

  test("SortMergeJoinScanner.finishProbe is a no-op when range indexing isn't configured") {
    sc = new SparkContext("local", "test", new SparkConf(false))
    val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)
    try {
      val scanner = new SortMergeJoinScanner(
        streamedKeyGenerator =
          UnsafeProjection.create(Seq(streamId), Seq(streamId, streamLo, streamHi)),
        bufferedKeyGenerator = UnsafeProjection.create(Seq(bufferId), Seq(bufferId, bufferP)),
        keyOrdering = RowOrdering.createNaturalAscendingOrdering(Seq(LongType)),
        streamedIter = RowIterator.fromScala(Iterator.empty),
        bufferedIter = RowIterator.fromScala(Iterator.empty),
        inMemoryThreshold = 1000,
        spillThreshold = Int.MaxValue,
        sizeInBytesSpillThreshold = Long.MaxValue,
        spillSize = SQLMetrics.createSizeMetric(sc, "spill"),
        eagerCleanupResources = () => ())
      scanner.finishProbe() // must not throw when probeSession is null
    } finally {
      TaskContext.unset()
    }
  }

  test("Session.resetGroup releases the index even while a candidate iterator from the " +
    "group just left behind is still outstanding") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, _) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null, "the group must be indexed by now")

      val candidates = session.candidates(array, InternalRow(1L, 25L, 55L))
      assert(candidates.hasNext)
      candidates.next()

      // A normal group transition (or task completion) must reclaim unconditionally: the
      // in-flight iterator guard only applies to an unprompted TaskMemoryManager spill, not
      // to this explicit lifecycle boundary.
      session.resetGroup()
      assert(session.currentRelationOrNull == null,
        "resetGroup must release the index regardless of any in-flight candidate iterator")
    }
  }

  test("Session.resetGroup clears a pending spill flag so the next group's first probe " +
    "is not immediately evicted") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null, "the group must be indexed by now")

      // Hold a live candidate iterator open, then decline a spill against it: this leaves
      // `pendingSpill = true` on the session while the index itself stays in place.
      val candidates = session.candidates(array, InternalRow(1L, 25L, 55L))
      assert(candidates.hasNext, "test setup needs at least one match to keep the iterator open")
      candidates.next()
      val otherConsumer = neverSpillConsumer(tc.taskMemoryManager())
      assert(session.spill(1L, otherConsumer) == 0L,
        "spill must be declined while the candidate iterator is in-flight")

      // A group transition abandons the outstanding iterator instead of draining it. If
      // resetGroup didn't also clear pendingSpill, the stale flag would survive into the next
      // group and evict its brand-new index on the very first probe (see wrapOutstanding).
      session.resetGroup()
      assert(session.currentRelationOrNull == null, "resetGroup must release the index")

      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null, "the new group must be indexed")
      session.candidates(array, InternalRow(1L, 25L, 55L))
      assert(session.currentRelationOrNull != null,
        "a stale pendingSpill left over from the abandoned group must not evict the new " +
          "group's index on its first probe")
    }
  }

  test("abandoned candidate iterator does not permanently block subsequent spills") {
    withRowArray(inMemoryRows = 1000, totalRows = 10) { (array, tc) =>
      val session = RangeGroupProbe.Session(pointInRangeSpec(threshold = 2))
      probeThroughGate(session, array)
      assert(session.currentRelationOrNull != null)

      // Row 1 abandoned early without draining or calling finishProbe:
      val candidates1 = session.candidates(array, InternalRow(1L, 25L, 55L))
      assert(candidates1.hasNext)
      candidates1.next()

      // Row 2 probes and drains to completion:
      val candidates2 = session.candidates(array, InternalRow(1L, 25L, 55L))
      while (candidates2.hasNext) candidates2.next()

      // A spill now must succeed because row 1's abandoned iterator does not leak:
      val otherConsumer = neverSpillConsumer(tc.taskMemoryManager())
      assert(session.spill(1L, otherConsumer) > 0L,
        "abandoned candidate iterator from an earlier row must not permanently block spills")
      assert(session.currentRelationOrNull == null)
    }
  }
}
