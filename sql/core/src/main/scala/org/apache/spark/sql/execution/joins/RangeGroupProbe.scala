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
import org.apache.spark.sql.catalyst.planning.ExtractRangeJoinKeys
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
   * with range indexing disabled. SortMergeJoinRangeIndexBenchmark's reuse sweep (fixed
   * groupSize=1024) shows parity (1.0X) at 100 reuses and below, and a 2.5X win at 1000
   * reuses, so the constant sits above the break-even point between those two measured
   * points - low enough to engage for the motivating workload (a group probed thousands of
   * times) while still keeping lightly-reused groups at the disabled-feature baseline. A
   * cost-model constant rather than a config, like `RangeIndex`'s small-build row cutoff: not
   * a policy a user tunes, and revisitable with benchmark evidence if the admission math
   * changes.
   */
  private[joins] val PROBE_THRESHOLD = 128

  final class Spec(
      val mode: RangeRelationBroadcastMode,
      val boundStreamKeys: Seq[Expression],
      val probeKind: RangeProbeKind,
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
      numFallbackMetric: SQLMetric = null,
      numBuildErrorMetric: SQLMetric = null) extends Serializable {
    def createSession(): Session =
      new Session(spec, numIndexedMetric, numFallbackMetric, numBuildErrorMetric)
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
      numFallbackMetric: SQLMetric = null,
      numBuildErrorMetric: SQLMetric = null) extends Logging {

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

    /**
     * An active candidate iterator handed out from `currentHandle` that a caller may still be
     * reading from (see `wrapOutstanding`). While an iterator is in-flight, `currentHandle`'s
     * relation stays reachable - and thus heap-resident - so `releaseForSpill` declines to free
     * TaskMemoryManager accounting immediately, setting `pendingSpill` instead to reclaim the
     * index as soon as the in-flight probe finishes.
     */
    private[this] var activeIterator: Iterator[InternalRow] = _
    private[this] var pendingSpill: Boolean = false

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
     * the same group.
     *
     * A buffered group is fully materialized before any streamed row probes it, so its row
     * count and spill-backed state - everything `isEligible` needs beyond `spec`'s own
     * fixed fields - never change across a group's probes. A group that already fails
     * `isEligible` on its first probe would fail it identically on every later one, so that
     * case is latched immediately instead of paying `PROBE_THRESHOLD` pointless
     * `probeCount` increments to reach a foregone conclusion; `declineBuild` below fires the
     * same way whether it is called here or after an actual `build()` attempt, so this only
     * changes when a never-eligible group's fallback is recorded, not whether it is. Only
     * TaskMemoryManager's actual free budget at `build()` time - not predictable from the
     * first probe - still requires waiting for `PROBE_THRESHOLD` probes before finding out.
     *
     * Note that a group that never reaches the probe threshold, and was never found
     * ineligible outright either, never increments `numIndexedMetric` or
     * `numFallbackMetric`: those two metrics count groups whose build was actually attempted
     * or declined, not every group above `threshold`, so their sum is not the total count of
     * above-threshold groups. `numFallbackMetric` counts every declined build that didn't
     * produce an index, whether rejected by policy (below `threshold`'s row count stays
     * silent and uncounted; over `maxBytes`, spill-backed, or insufficient TaskMemoryManager
     * budget are counted) or because `build()` hit a genuine error (a non-fatal exception or
     * an `OutOfMemoryError`); `numBuildErrorMetric` additionally counts only the latter,
     * narrower population, so an operator can tell a build that failed outright apart from
     * one the policy simply declined to attempt. On the whole-stage-codegen path, a streamed
     * row that probes no group at all (an unmatched outer or anti row seeing an empty
     * buffer) also advances `probeCount`, since the generated code calls `onProbe`
     * unconditionally for every streamed row. The interpreted path does not: it
     * short-circuits before reaching `onProbe`/`candidates` whenever the buffered group is
     * empty (see `OneSideOuterIterator.advanceStream`'s `getBufferedMatches.isEmpty` branch,
     * and `SortMergeJoinEvaluatorFactory`'s `currentRightMatches.length > 0` guards for
     * LeftSemi/LeftAnti/ExistenceJoin), so `probeCount` progression - and therefore exactly
     * when a group's index gets built - is not guaranteed to match between the two execution
     * paths for the same query. Regardless of path, `probeCount` cannot leak into a later
     * group, since `resetGroup` runs before a new group's rows are buffered, and within one
     * group every probing row shares its key.
     */
    def onProbe(matches: ExternalAppendOnlyUnsafeRowArray): Unit = {
      if (buildAttempted) {
        return
      }
      if (probeCount == 0 && !spec.isEligible(matches.length, matches.isSpillBacked)) {
        declineBuild(matches)
        return
      }
      probeCount += 1
      if (probeCount >= PROBE_THRESHOLD) {
        currentHandle = build(matches, spec, memoryConsumer, numBuildErrorMetric)
        if (currentHandle != null) {
          buildAttempted = true
          if (numIndexedMetric != null) numIndexedMetric += 1
        } else {
          declineBuild(matches)
        }
      }
    }

    /**
     * Latches `buildAttempted` so neither this probe's caller nor a later one re-evaluates
     * the decision for the rest of this group, and records the fallback - unless the group's
     * row count alone already ruled it out, which `onProbe`'s callers treat as never having
     * been a candidate rather than a declined build.
     */
    private def declineBuild(matches: ExternalAppendOnlyUnsafeRowArray): Unit = {
      buildAttempted = true
      if (matches.length >= spec.threshold) {
        if (numFallbackMetric != null) numFallbackMetric += 1
        logDebug(log"range index fallback for a buffered group of " +
          log"${MDC(NUM_ROWS, matches.length)} rows " +
          log"(threshold=${MDC(THRESHOLD, spec.threshold)}); using a linear scan instead")
      }
    }

    def candidates(
        matches: ExternalAppendOnlyUnsafeRowArray,
        streamedRow: InternalRow): Iterator[InternalRow] = {
      onProbe(matches)
      if (currentHandle != null) {
        wrapOutstanding(currentHandle.candidates(streamedRow))
      } else {
        matches.generateIterator()
      }
    }

    /**
     * Wraps a candidate iterator sourced from `currentHandle`'s relation so `releaseForSpill`
     * can tell it is still being read from. Whole-stage codegen's generated probe calls this
     * directly (see `SortMergeJoinExec.genScanner`'s `createIterator`) since it builds its own
     * candidate iterator straight from `currentRelationOrNull` rather than going through
     * `candidates` above; both call sites need the same tracking, since either one can hold a
     * live reference to the relation's backing arrays across a nested `consume()` call that
     * triggers a TaskMemoryManager spill elsewhere in the same task.
     */
    def wrapOutstanding(it: Iterator[InternalRow]): Iterator[InternalRow] = {
      if (pendingSpill && currentHandle != null) {
        pendingSpill = false
        doReleaseForSpill()
      }
      val wrapped = new Iterator[InternalRow] {
        private[this] var drained = false
        override def hasNext: Boolean = {
          val has = it.hasNext
          if (!has && !drained) {
            drained = true
            if (activeIterator eq this) {
              activeIterator = null
            }
            if (pendingSpill && currentHandle != null) {
              pendingSpill = false
              doReleaseForSpill()
            }
          }
          has
        }
        override def next(): InternalRow = it.next()
      }
      activeIterator = wrapped
      wrapped
    }

    /**
     * Marks the completion of a candidate probe for the current streamed row early, e.g. when
     * an operator like `LeftSemi` finds a match and breaks out without draining the iterator.
     * Reclaims the index immediately if a spill request arrived while the probe was in-flight.
     */
    def finishProbe(): Unit = {
      activeIterator = null
      if (pendingSpill && currentHandle != null) {
        pendingSpill = false
        doReleaseForSpill()
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
      activeIterator = null
      pendingSpill = false
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
     *
     * Declines (returns 0, leaving `currentHandle` in place) while a candidate iterator from
     * `wrapOutstanding` is still in-flight (`activeIterator.hasNext` is true): the relation's
     * backing arrays stay reachable on the heap, so freeing TaskMemoryManager accounting
     * immediately would cause a transient accounting divergence. Instead, sets `pendingSpill`
     * so that the index is reclaimed as soon as the in-flight probe completes.
     */
    private def releaseForSpill(): Long = {
      if (currentHandle != null) {
        if (activeIterator != null && activeIterator.hasNext) {
          pendingSpill = true
          logDebug(log"spill declined for a buffered group's range index: a candidate " +
            log"iterator is still in-flight; reclaiming once it completes")
          0L
        } else {
          activeIterator = null
          pendingSpill = false
          doReleaseForSpill()
        }
      } else {
        0L
      }
    }

    private def doReleaseForSpill(): Long = {
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
                val probeKind = RangeProbeKind.forJoin(kind, buildSide)
                val mode = RangeRelationBroadcastMode(boundBufferKeys, probeKind.indexKind)
                // The index retains a full copy of every buffered row (see `PointIndex`/
                // `IntervalIndex`), so the memory estimate must scale with the buffered side's
                // actual schema rather than a fixed per-row constant.
                val bufferRowSize = EstimationUtils.getSizePerRow(bufferOutput).toLong
                Some(new Spec(
                  mode,
                  boundStreamKeys,
                  probeKind,
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
      consumer: RangeIndexMemoryConsumer,
      buildErrorMetric: SQLMetric = null): Handle = {
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
        relation, spec.boundStreamKeys, spec.probeKind)
      new Handle(relation, allocatedBytes, candidateFn)
    } catch {
      case NonFatal(e) =>
        if (consumer != null && heldBytes > 0) consumer.freeMemory(heldBytes)
        if (buildErrorMetric != null) buildErrorMetric += 1
        logWarning(log"range index build failed for a buffered group of " +
          log"${MDC(NUM_ROWS, n)} rows; falling back to a linear scan", e)
        null
      case oom: OutOfMemoryError =>
        // The pre-admission estimate above is schema-driven (DataType.defaultSize) and cannot
        // predict every row's real size (e.g. unusually large variable-length values), so a wide
        // enough buffered group can still exhaust the heap during transform()'s plain (non-
        // TaskMemoryManager-tracked) row copies. This is a last-resort safety net: fall back to
        // a linear scan instead of failing the task. Counted separately via `buildErrorMetric`
        // (distinct from the routine, by-policy fallback counted in `onProbe`) so an operator
        // can tell a genuine build failure apart from a group that was merely too large or
        // too-few-times-reused to index by policy.
        if (consumer != null && heldBytes > 0) consumer.freeMemory(heldBytes)
        if (buildErrorMetric != null) buildErrorMetric += 1
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
