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
import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.ExtractRangeJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

/**
 * Encapsulates per-equi-key group range index management, budgeting, and candidate probing.
 */
private[joins] object RangeGroupProbe extends Logging {

  /**
   * Probes a buffered group must receive before its index is built, so the one-time build
   * cost is paid only once the linear rescans it replaces would have cost about as much.
   * SortMergeJoinRangeIndexBenchmark's reuse sweep (groupSize=1024) shows parity at 100
   * reuses and a 2.9X win at 1000, so 128 sits just above that break-even point. A cost-model
   * constant, not a user-facing config; revisit with new benchmark evidence if it's wrong.
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
     * Pre-admission memory estimate, trued up against the real footprint once `build()`
     * constructs the relation. `bufferRowSize` (a schema-based per-row estimate) dominates,
     * since each index entry retains a full copy of its buffered row; `keyOverhead` only
     * accounts for the key value(s) stored alongside it.
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
    def createSession(): Session = Session(spec, numIndexedMetric, numFallbackMetric)
  }

  final class Handle private[RangeGroupProbe] (
      val relation: RangeRelation,
      val allocatedBytes: Long,
      val candidateFn: InternalRow => Iterator[InternalRow]) {

    def candidates(streamedRow: InternalRow): Iterator[InternalRow] = candidateFn(streamedRow)
  }

  /**
   * Stateful execution controller for a single task partition: memory budgeting, index
   * build/release lifecycle, metrics, and task-completion cleanup. Extends `MemoryConsumer`
   * directly since `spill()` only needs the reentrancy guard below.
   *
   * `taskContext` is a constructor parameter, not a field, because the superclass constructor
   * call (`taskContext.taskMemoryManager()`) runs before any `this`-bound field is
   * initialized.
   */
  final class Session private (
      taskContext: TaskContext,
      val spec: Spec,
      numIndexedMetric: SQLMetric = null,
      numFallbackMetric: SQLMetric = null)
    extends MemoryConsumer(taskContext.taskMemoryManager(), MemoryMode.ON_HEAP) with Logging {

    /**
     * A TaskMemoryManager request arriving while `releaseForSpill()` runs cannot re-enter the
     * manager, since `releaseForSpill()` only frees memory this consumer already holds and
     * never acquires any. A self-triggered request - one raised by this consumer's own
     * `acquireMemory` - returns 0 instead, rather than freeing the very memory that request is
     * still trying to obtain.
     */
    override def spill(size: Long, trigger: MemoryConsumer): Long =
      if (trigger eq this) 0L else releaseForSpill()

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

    taskContext.addTaskCompletionListener[Unit](_ => resetGroup())

    /**
     * Called once per streamed row probing the buffered group, before `candidates`/
     * `currentRelationOrNull` are consulted. Builds the index once `PROBE_THRESHOLD` probes
     * confirm the group is reused enough to amortize the one-time build cost; `buildAttempted`
     * latches the decision - including an outright-ineligible verdict from the first probe,
     * since `isEligible` only depends on fixed, per-group state - so later probes never
     * re-evaluate it.
     *
     * `numIndexedMetric`/`numFallbackMetric` only count groups whose build was actually
     * attempted or declined, not every group above `threshold`, so a group that never
     * reaches the probe threshold leaves both silent.
     *
     * Whole-stage codegen calls this unconditionally for every streamed row, including ones
     * that probe no group at all; the interpreted path short-circuits on an empty buffered
     * group instead (see `OneSideOuterIterator`/`SortMergeJoinEvaluatorFactory`), so
     * `probeCount` progression can differ between the two paths for the same query.
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
        currentHandle = build(matches, spec, this)
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
     * Wraps a candidate iterator so `releaseForSpill` can tell it is still being read from:
     * either caller can hold a live reference to the relation's backing arrays across a
     * nested `consume()` call that triggers a TaskMemoryManager spill elsewhere in the task.
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
     * an operator like `LeftSemi` finds a match and breaks out without draining the iterator,
     * or at the end of a whole-stage-codegen probe loop. Reclaims the index immediately if a
     * spill request arrived while the probe was in-flight.
     */
    def finishProbe(): Unit = {
      activeIterator = null
      if (pendingSpill && currentHandle != null) {
        pendingSpill = false
        doReleaseForSpill()
      }
    }

    /**
     * The current group's materialized index, or `null` before it's built, after it falls
     * back to a linear scan, or once it's evicted under memory pressure. Callers must call
     * `onProbe` first; this accessor itself has no side effect.
     */
    def currentRelationOrNull: RangeRelation =
      if (currentHandle != null) currentHandle.relation else null

    def resetGroup(): Unit = {
      activeIterator = null
      pendingSpill = false
      if (currentHandle != null) {
        release(currentHandle, this)
        currentHandle = null
      }
      probeCount = 0
      buildAttempted = false
    }

    /**
     * Releases the current group's index on a TaskMemoryManager spill request, falling back to
     * a linear scan for the rest of the group; counted in `numFallbackMetric` like any other
     * fallback.
     *
     * Declines (returns 0) while a `wrapOutstanding` iterator is still in-flight, since its
     * backing arrays stay heap-reachable until then; sets `pendingSpill` instead, to reclaim
     * the index as soon as the in-flight probe completes.
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
        release(currentHandle, this)
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

  private[joins] object Session {

    /**
     * The one `TaskContext.get()` lookup for a `Session`: both call sites that mint one
     * (`SortMergeJoinEvaluatorFactory` and whole-stage codegen) only ever do so from within
     * a running task, so a missing `TaskContext` here means a caller is being misused, not
     * a condition to degrade gracefully for - hence `require` instead of a null-safe
     * fallback.
     */
    def apply(
        spec: Spec,
        numIndexedMetric: SQLMetric = null,
        numFallbackMetric: SQLMetric = null): Session = {
      val taskContext = TaskContext.get()
      require(taskContext != null,
        "RangeGroupProbe.Session must be constructed within an active Spark task")
      new Session(taskContext, spec, numIndexedMetric, numFallbackMetric)
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
      consumer: MemoryConsumer,
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
      // The nominal estimate above is schema-based (DataType.defaultSize) and can under-shoot
      // a group with wide variable-length values; check it against the real, SizeEstimator-
      // based footprint so TaskMemoryManager's accounting never under-counts this consumer's
      // usage, which the rest of the task's memory safety depends on.
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
      } else {
        // The nominal estimate already covers the real footprint. Keeping the (bounded, single-
        // group) excess reserved until this group resets is simpler than releasing it now, and
        // only ever over-counts this consumer's usage, never under-counts it.
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
        // The schema-driven estimate can't predict every row's real size, so a wide enough
        // group can still exhaust the heap during transform()'s untracked row copies.
        // Last-resort safety net: fall back to a linear scan, counted via `buildErrorMetric`
        // (distinct from the routine, by-policy fallback `onProbe` counts) so an operator can
        // tell the two apart.
        if (consumer != null && heldBytes > 0) consumer.freeMemory(heldBytes)
        if (buildErrorMetric != null) buildErrorMetric += 1
        logWarning(log"range index build ran out of memory for a buffered group of " +
          log"${MDC(NUM_ROWS, n)} rows; falling back to a linear scan", oom)
        null
    }
  }

  def release(handle: Handle, consumer: MemoryConsumer): Unit = {
    if (handle != null && consumer != null && handle.allocatedBytes > 0) {
      consumer.freeMemory(handle.allocatedBytes)
    }
  }
}
