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

import java.io.Serializable

import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{NUM_BYTES, NUM_ROWS, THRESHOLD}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{ExtractRangeJoinKeys, PartialRangeJoin}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

private[joins] class RangeIndexMemoryConsumer(
    taskMemoryManager: TaskMemoryManager,
    releaseHandler: () => Long)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) {

  /**
   * `releaseHandler` frees memory this consumer already holds and never acquires any, so a
   * TaskMemoryManager request arriving while it runs cannot re-enter the manager. A
   * self-triggered request - one raised by this consumer's own `acquireMemory` - returns 0
   * instead, rather than freeing the very memory that request is still trying to obtain.
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long =
    if (trigger eq this) 0L else releaseHandler()
}

/**
 * Encapsulates per-equi-key group range index management, budgeting, and candidate probing.
 */
private[joins] object RangeGroupProbe extends Logging {

  /**
   * Probes a buffered group must receive before its index is built. Building is a one-time
   * cost that pays for itself only once the linear rescans it replaces have cost about as
   * much as the build, so a group probed fewer times keeps the plain linear scan at parity
   * with range indexing disabled. SortMergeJoinRangeIndexBenchmark measures the
   * build-versus-scan break-even at roughly 100 probes for a 1024-row group (a group just
   * past a lower gate did not amortize: the 100-probe reuse row read 0.8X-1.0X across runs
   * at a gate of 64), and the constant sits above that point so the index engages only for
   * groups reused well past it - where the benchmark's 1000-probe row wins about 4X and
   * the motivating workload, probed thousands of times per group, wins by more. A cost-model
   * constant rather than a config, like `RangeIndex`'s small-build row cutoff: not a policy
   * a user tunes, and revisitable with benchmark evidence if the admission math changes.
   */
  private[joins] val PROBE_THRESHOLD = 128

  final class Spec(
      val mode: RangeRelationBroadcastMode,
      val boundStreamKeys: Seq[Expression],
      val pointScan: (PointScan, Boolean),
      val threshold: Int,
      val maxBytes: Long,
      val bufferRowSize: Long) extends Serializable {

    /**
     * Whether the build keys map to one order-preserving long (`RangeIndex.longKey`), fixed
     * for the life of the plan. Computed once here instead of per group build, and consulted
     * by codegen (`RangeIndexProbeCodegen`) to pick the concrete index class and key encoding
     * without re-deriving it from `mode.buildKeys` on every probe.
     */
    val usesLongKeys: Boolean = RangeIndex.canMapKeysToLong(mode.buildKeys)

    /**
     * A pre-admission estimate used to reserve memory before the index is built (the real
     * footprint, `RangeRelation.estimatedSize`, only exists once the relation has been
     * constructed). `build()` trues this up against the real size afterward. Each index entry
     * retains a full copy of its buffered row (see `PointIndex`/`IntervalIndex` in
     * `RangeRelation.scala`), so `bufferRowSize` - a schema-based estimate from
     * `EstimationUtils.getSizePerRow`, computed once in `createSpec` - dominates the estimate;
     * `keyOverhead` only accounts for the key value(s) each index stores alongside the row.
     */
    def estimateMemoryBytes(numRows: Long): Long = {
      val keyOverhead = mode.indexKind match {
        case IntervalIndexKind => 32L // two boundary values plus the maxHighIdx array slot
        case PointIndexKind => 16L // one key value
      }
      numRows * (bufferRowSize + keyOverhead)
    }

    def isEligible(numRows: Long, isSpillBacked: Boolean): Boolean = {
      numRows >= threshold && !isSpillBacked && estimateMemoryBytes(numRows) <= maxBytes
    }
  }

  /**
   * Driver-to-executor factory bundling the plan-level range probe specification and its
   * runtime metrics. Used by `SortMergeJoinEvaluatorFactory` and whole-stage codegen to
   * instantiate task-scoped `Session` instances on executors.
   */
  final case class SessionFactory(
      spec: Spec,
      numIndexedMetric: SQLMetric = null,
      numFallbackMetric: SQLMetric = null) extends Serializable {
    def createSession(): Session = new Session(spec, numIndexedMetric, numFallbackMetric)
  }

  final class Handle private[RangeGroupProbe] (
      val relation: RangeRelation,
      val allocatedBytes: Long,
      val candidateFn: InternalRow => Iterator[InternalRow]) {

    def candidates(streamedRow: InternalRow): Iterator[InternalRow] = candidateFn(streamedRow)
  }

  /**
   * Stateful execution controller for a single task partition. Manages memory budgeting,
   * index build/release lifecycle, metrics, and automatic task completion cleanup.
   */
  final class Session(
      val spec: Spec,
      numIndexedMetric: SQLMetric = null,
      numFallbackMetric: SQLMetric = null) extends Logging {

    private[this] val taskContext: TaskContext = TaskContext.get()

    private[joins] val memoryConsumer: RangeIndexMemoryConsumer = {
      if (taskContext != null) {
        new RangeIndexMemoryConsumer(taskContext.taskMemoryManager(), () => releaseForSpill())
      } else {
        null
      }
    }

    private[this] var currentHandle: Handle = _
    private[this] var probeCount: Int = 0
    private[this] var buildAttempted: Boolean = false

    if (taskContext != null) {
      taskContext.addTaskCompletionListener[Unit](_ => resetGroup())
    }

    /**
     * Called once per streamed row probing the current buffered group, before
     * `candidates`/`currentRelationOrNull` are consulted. The index is only worth its
     * one-time build cost once enough probes confirm the group is actually reused: the
     * probes already paid would have amortized a build whose linear rescans they replaced
     * cost about as much as the build itself. [[PROBE_THRESHOLD]] sits well past that
     * amortization point, so a group probed fewer times - e.g. the common one-to-many
     * join shape of a unique dimension key joined to a skewed fact table - pays only the
     * linear scans it would have paid anyway, at parity with range indexing disabled,
     * instead of also building an index too few probes will reuse. `buildAttempted`
     * latches the budget/eligibility decision so a later probe never re-evaluates it for
     * the same group. Note that a group that never reaches the probe threshold never
     * increments `numIndexedMetric` or `numFallbackMetric`: those two metrics count
     * groups whose build was actually attempted, not every group above `threshold`, so
     * their sum is not the total count of above-threshold groups. A streamed row that
     * probes no group at all (an unmatched outer or anti row seeing an empty buffer)
     * also advances `probeCount`; it cannot leak into a later group, since `resetGroup` runs
     * before a new group's rows are buffered, and within one group every probing row shares
     * its key.
     */
    def onProbe(matches: ExternalAppendOnlyUnsafeRowArray): Unit = {
      probeCount += 1
      if (currentHandle == null && !buildAttempted && probeCount >= PROBE_THRESHOLD) {
        buildAttempted = true
        currentHandle = build(matches, spec, memoryConsumer)
        if (currentHandle != null) {
          if (numIndexedMetric != null) numIndexedMetric += 1
        } else if (matches.length >= spec.threshold) {
          if (numFallbackMetric != null) numFallbackMetric += 1
          logDebug(log"range index fallback for a buffered group of " +
            log"${MDC(NUM_ROWS, matches.length)} rows " +
            log"(threshold=${MDC(THRESHOLD, spec.threshold)}); using a linear scan instead")
        }
      }
    }

    def candidates(
        matches: ExternalAppendOnlyUnsafeRowArray,
        streamedRow: InternalRow): Iterator[InternalRow] = {
      onProbe(matches)
      if (currentHandle != null) {
        currentHandle.candidates(streamedRow)
      } else {
        matches.generateIterator()
      }
    }

    /**
     * The current group's materialized index, or `null` when the group hasn't been indexed yet
     * (fewer than `PROBE_THRESHOLD` probes so far), fell back to a linear scan (below
     * `threshold`, over a row/byte budget, or buffered-side spilled), or had its index evicted
     * under memory pressure. Used by `SortMergeJoinExec`'s whole-stage codegen to generate a
     * direct, specialized probe call via `RangeIndexProbeCodegen` instead of going through
     * `candidates`' interpreted dispatch. Callers must call `onProbe` first; this accessor
     * itself has no side effect.
     */
    def currentRelationOrNull: RangeRelation =
      if (currentHandle != null) currentHandle.relation else null

    def resetGroup(): Unit = {
      if (currentHandle != null) {
        release(currentHandle, memoryConsumer)
        currentHandle = null
      }
      probeCount = 0
      buildAttempted = false
    }

    /**
     * Releases the current group's index on a TaskMemoryManager spill request, falling back to
     * the always-available linear scan on the buffered group for the rest of the group. Counts
     * as a fallback: the group was successfully indexed (already counted in `numIndexedMetric`)
     * but now reverts to a linear scan for its remaining probes, matching what
     * `numFallbackMetric` otherwise counts for a group whose build was rejected outright.
     */
    private def releaseForSpill(): Long = {
      if (currentHandle != null) {
        val bytes = currentHandle.allocatedBytes
        release(currentHandle, memoryConsumer)
        currentHandle = null
        // Don't retry the build for the rest of this group: the memory just freed is exactly
        // what a retry would need to re-acquire, and retrying would just spill again.
        buildAttempted = true
        if (numFallbackMetric != null) numFallbackMetric += 1
        logDebug(log"range index of ${MDC(NUM_BYTES, bytes)} bytes evicted under memory " +
          log"pressure for a buffered group; falling back to a linear scan for its " +
          log"remaining probes")
        bytes
      } else {
        0L
      }
    }
  }

  /**
   * `buildSide` names the caller's streamed/buffered split (e.g. `SortMergeJoinExec.buildSide`),
   * passed in instead of re-derived from `joinType` here so the two can't drift apart.
   */
  def createSpec(
      condition: Option[Expression],
      joinType: JoinType,
      equiLeftKeys: Seq[Expression],
      equiRightKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
      buildSide: BuildSide,
      conf: SQLConf): Option[Spec] = {
    val threshold = conf.sortMergeJoinRangeIndexThreshold
    if (threshold < 0) {
      return None
    }
    joinType match {
      case FullOuter => None
      case _ =>
        condition.flatMap { cond =>
          ExtractRangeJoinKeys.fromJoinCondition(cond, leftOutput, rightOutput).flatMap {
            case (leftRange, rightRange, kind) =>
              val equiRefs = AttributeSet(equiLeftKeys ++ equiRightKeys)
              if (AttributeSet(leftRange ++ rightRange).intersect(equiRefs).nonEmpty) {
                None
              } else {
                val (streamOutput, bufferOutput, streamRange, bufferRange) = buildSide match {
                  case BuildRight => (leftOutput, rightOutput, leftRange, rightRange)
                  case BuildLeft => (rightOutput, leftOutput, rightRange, leftRange)
                }
                val boundBufferKeys = BindReferences.bindReferences(bufferRange, bufferOutput)
                val boundStreamKeys = BindReferences.bindReferences(streamRange, streamOutput)
                val mode = RangeRelationBroadcastMode(boundBufferKeys, RangeIndexKind(kind))
                val pointScan = kind match {
                  case _: PartialRangeJoin => PointScan.forJoin(kind, buildSide)
                  // Interval-kind modes never force `pointScan`: both probe paths take it
                  // by name and only read it for a point-index build, so this placeholder
                  // keeps the field non-optional rather than carrying a meaning.
                  case _ => (PointScan.UpTo, false)
                }
                // The index retains a full copy of every buffered row (see `PointIndex`/
                // `IntervalIndex`), so the memory estimate must scale with the buffered side's
                // actual schema rather than a fixed per-row constant.
                val bufferRowSize = EstimationUtils.getSizePerRow(bufferOutput).toLong
                Some(new Spec(
                  mode,
                  boundStreamKeys,
                  pointScan,
                  threshold,
                  conf.sortMergeJoinRangeIndexMaxBytes,
                  bufferRowSize))
              }
          }
        }
    }
  }

  def build(
      matches: ExternalAppendOnlyUnsafeRowArray,
      spec: Spec,
      consumer: RangeIndexMemoryConsumer): Handle = {
    val n = matches.length
    if (!spec.isEligible(n, matches.isSpillBacked)) {
      return null
    }

    val nominalBytes = spec.estimateMemoryBytes(n)
    val acquired = if (consumer != null) consumer.acquireMemory(nominalBytes) else nominalBytes
    if (acquired < nominalBytes) {
      if (consumer != null && acquired > 0) consumer.freeMemory(acquired)
      return null
    }

    // This code was previously only reachable through the opt-in BroadcastRangeJoinExec path;
    // it is now unconditionally on sort-merge join's default-on hot path, so an exception here
    // (e.g. from one of RangeRelation's invariant checks) must not fail the query. Tracks bytes
    // acquired so far in a var, rather than relying on a fixed quantity, so the catch block
    // below releases exactly what this attempt is holding at the point it failed.
    var heldBytes = acquired
    try {
      val relation = spec.mode.transform(matches.generateIterator(), Some(n))
      // The nominal estimate above is schema-based but still approximate (e.g. it uses
      // DataType.defaultSize, not actual value lengths); true it up against the real,
      // SizeEstimator-based footprint of the relation we just built so the TaskMemoryManager
      // budget reflects actual heap usage rather than the pre-admission guess.
      val actualBytes = relation.estimatedSize

      if (actualBytes > spec.maxBytes) {
        // The real footprint exceeds the configured cap even though the pre-admission estimate
        // didn't predict it (e.g. unusually large variable-length values); release and fall
        // back instead of silently exceeding maxBytes.
        if (consumer != null) consumer.freeMemory(heldBytes)
        return null
      }

      val allocatedBytes = if (actualBytes > heldBytes) {
        val extra = actualBytes - heldBytes
        val extraAcquired = if (consumer != null) consumer.acquireMemory(extra) else extra
        if (extraAcquired < extra) {
          if (consumer != null) {
            if (extraAcquired > 0) consumer.freeMemory(extraAcquired)
            consumer.freeMemory(heldBytes)
          }
          return null
        }
        heldBytes += extraAcquired
        heldBytes
      } else if (actualBytes < heldBytes) {
        // We over-reserved relative to the real footprint; release the excess so the consumer's
        // `used` accounting tracks actual heap usage.
        val excess = heldBytes - actualBytes
        if (consumer != null) consumer.freeMemory(excess)
        heldBytes -= excess
        actualBytes
      } else {
        heldBytes
      }

      val candidateFn = RangeRelationProbe.makeCandidateFn(
        relation, spec.boundStreamKeys, spec.pointScan)
      new Handle(relation, allocatedBytes, candidateFn)
    } catch {
      case NonFatal(e) =>
        if (consumer != null && heldBytes > 0) consumer.freeMemory(heldBytes)
        logWarning(log"range index build failed for a buffered group of " +
          log"${MDC(NUM_ROWS, n)} rows; falling back to a linear scan", e)
        null
      case oom: OutOfMemoryError =>
        // A schema-driven estimate (`estimateMemoryBytes`) still cannot predict every row's
        // real size (e.g. unusually large variable-length values), so this is a last-resort
        // safety net: fall back to a linear scan instead of failing the task.
        if (consumer != null && heldBytes > 0) consumer.freeMemory(heldBytes)
        logWarning(log"range index build ran out of memory for a buffered group of " +
          log"${MDC(NUM_ROWS, n)} rows; falling back to a linear scan", oom)
        null
    }
  }

  def release(handle: Handle, consumer: RangeIndexMemoryConsumer): Unit = {
    if (handle != null && consumer != null && handle.allocatedBytes > 0) {
      consumer.freeMemory(handle.allocatedBytes)
    }
  }
}
