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
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

/**
 * Encapsulates per-equi-key group range index management, budgeting, and candidate probing.
 */
private[joins] object RangeGroupProbe extends Logging {

  // --- Public API ----------------------------------------------------------------------------

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
                Some(new Spec(
                  mode,
                  boundStreamKeys,
                  probeKind,
                  threshold,
                  conf.sortMergeJoinRangeIndexMaxBytes))
              }
          }
        }
    }
  }

  def build(
      matches: ExternalAppendOnlyUnsafeRowArray,
      spec: Spec,
      consumer: MemoryConsumer): Handle = {
    val n = matches.length
    if (!spec.isEligible(matches)) {
      return null
    }

    // Reserved ahead of transform() to cover the structural index arrays and transient staging
    // allocated during sorting (see `Spec.peakBuildBytes`). Row payloads already buffered in
    // `matches` are referenced zero-copy, so wide rows with 100+ columns add no extra allocation.
    val peakBytes = spec.peakBuildBytes(matches)
    val acquired = if (consumer != null) consumer.acquireMemory(peakBytes) else peakBytes
    if (acquired < peakBytes) {
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
      val relation = spec.mode.transform(matches.generateIterator(), Some(n), copyRows = false)
      // peakBuildBytes above is based on the group's real row bytes, not a schema guess, but
      // remains an approximation of transform()'s actual staging cost; check it against the
      // real, SizeEstimator-based footprint so TaskMemoryManager's accounting never under- (or
      // permanently over-) counts this consumer's usage.
      val actualBytes = relation.estimatedSize

      if (actualBytes > spec.maxBytes) {
        // The real footprint exceeds the configured cap even though the pre-admission estimate
        // didn't predict it; release and fall back instead of silently exceeding maxBytes.
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
        // peakBuildBytes's transient-staging margin (boxed key/row tuples, sort scratch space)
        // is garbage now that transform() has returned; release it instead of permanently
        // over-counting this consumer's usage for the rest of the group's lifetime.
        if (consumer != null) consumer.freeMemory(heldBytes - actualBytes)
        heldBytes = actualBytes
        heldBytes
      } else {
        heldBytes
      }

      val candidateFn = RangeIndexProbe.makeCandidateFn(
        relation, spec.boundStreamKeys, spec.probeKind)
      new Handle(relation, allocatedBytes, candidateFn)
    } catch {
      case NonFatal(e) =>
        if (consumer != null && heldBytes > 0) consumer.freeMemory(heldBytes)
        logWarning(log"range index build failed for a buffered group of " +
          log"${MDC(NUM_ROWS, n)} rows; falling back to a linear scan", e)
        null
    }
  }

  def release(handle: Handle, consumer: MemoryConsumer): Unit = {
    if (handle != null && consumer != null && handle.allocatedBytes > 0) {
      consumer.freeMemory(handle.allocatedBytes)
    }
  }

  // --- Public API: supporting types ------------------------------------------------------------
  // Spec/SessionFactory/Handle/Session below are consumed across files (SortMergeJoinExec's
  // rangeProbeSpec field, SortMergeJoinEvaluatorFactory's SessionFactory param, and Session's
  // methods spliced directly into SortMergeJoinExec's generated codegen) -- not file-private.

  /**
   * Probes a buffered group must receive before its index is built, so the one-time build
   * cost is paid only once the linear rescans it replaces would have cost about as much. See
   * SortMergeJoinRangeIndexBenchmark-results.txt's reuse sweep (groupSize=1024) for the
   * current numbers this threshold is set against, rather than hardcoding them here where
   * they'd silently go stale the next time the benchmark is re-run. A cost-model constant,
   * not a user-facing config; revisit if those numbers no longer support it.
   */
  private[joins] val MIN_PROBES_BEFORE_BUILD = 128

  final class Spec(
      val mode: RangeRelationBroadcastMode,
      val boundStreamKeys: Seq[Expression],
      val probeKind: RangeProbeKind,
      val threshold: Int,
      val maxBytes: Long) extends Serializable {

    /**
     * Whether the build keys map to one order-preserving long (`RangeIndex.longKey`), fixed
     * for the life of the plan. Computed once here instead of per group build, and consulted
     * by codegen (`RangeIndexProbeCodegen`) to pick the concrete index class and key encoding
     * without re-deriving it from `mode.buildKeys` on every probe.
     */
    val usesLongKeys: Boolean = RangeIndex.canMapKeysToLong(mode.buildKeys)

    /**
     * Fixed per-row overhead of the index's own bookkeeping arrays once built - `lows`/`highs`/
     * `maxHighs` for an interval index, or just the key array for a point index - separate
     * from the row data itself, which `estimatedRetainedBytes`/`peakBuildBytes` account for via
     * the group's real, already-measured buffered-row byte count rather than a schema guess, and
     * from the `Array[InternalRow]` reference slot, which those two methods add separately. For
     * `usesLongKeys`, where every bound is a primitive long, this matches
     * `LongIntervalIndex`/`LongPointIndex`'s zero-copy `estimatedSize` up to a small, size-
     * independent constant from the backing arrays' own object headers (which `SizeEstimator`
     * counts and this fixed constant does not) - negligible next to the per-row term; see
     * `perRowKeyBytes` for the boxed-key case.
     */
    private val keyOverhead: Long = mode.indexKind match {
      case IntervalIndexKind => 24L // lows, highs, and maxHighs: one primitive long each
      case PointIndexKind => 8L // one primitive long key
    }

    /** Boxed key values the index owns per row: two bounds for an interval index, one key for
     * a point index - used to project a boxed key's per-row cost from the group's average
     * buffered-row width below. */
    private val boxedKeysPerRow: Int = mode.indexKind match {
      case IntervalIndexKind => 2
      case PointIndexKind => 1
    }

    /**
     * Per-row key storage cost. For `usesLongKeys`, every bound the index stores is a primitive
     * long (`RangeIndex.longKey`) regardless of the original column's declared type, so the
     * fixed `keyOverhead` constant closely approximates it (see its own doc for the small,
     * size-independent gap). Otherwise `IntervalIndex`/`PointIndex` deep-copy
     * the raw key value via `InternalRow.copyValue` - a cost that scales with the key's width
     * (e.g. a `StringType` range bound) - which `keyOverhead` alone does not capture. Since the
     * key is a subset of the row, the group's own already-measured average buffered-row size
     * (`bufferSize / length`) is a conservative, pre-`transform()` proxy for it that
     * never under-counts.
     */
    private def perRowKeyBytes(matches: ExternalAppendOnlyUnsafeRowArray): Long = {
      if (usesLongKeys) {
        keyOverhead
      } else {
        val avgRowBytes = matches.bufferSize / math.max(1L, matches.length)
        math.max(keyOverhead, boxedKeysPerRow * avgRowBytes)
      }
    }

    /**
     * Estimated final, persisted structural footprint of the built index: per-row index
     * bookkeeping (`perRowKeyBytes` for the key array(s), plus 8 bytes for the
     * `Array[InternalRow]` reference slot). Under zero-copy indexing (`copyRows = false`), the
     * index directly references the immutable rows already buffered in
     * `ExternalAppendOnlyUnsafeRowArray`, so row payload data (which can be huge for wide tables
     * with 100+ columns) is not duplicated and does not count toward the index's memory
     * footprint.
     */
    def estimatedRetainedBytes(matches: ExternalAppendOnlyUnsafeRowArray): Long =
      matches.length * (perRowKeyBytes(matches) + 8L)

    /**
     * Budget to request from `TaskMemoryManager` before calling `transform()`, covering the
     * final retained index footprint plus the transient staging `transform()` allocates and
     * discards while sorting and building: 48 bytes/row approximates that transient staging cost.
     * None of it is retained in the final relation; `build()` releases the unused portion back
     * once it measures the real footprint.
     */
    def peakBuildBytes(matches: ExternalAppendOnlyUnsafeRowArray): Long =
      matches.length * (perRowKeyBytes(matches) + 48L)

    /**
     * Whether this group's row count alone already rules out ever being a real index
     * candidate: either permanently empty (no buffered row ever matches the streamed side,
     * reachable when `threshold` is configured down to its minimum legal value of 0 - at any
     * `threshold >= 1` this is already implied by the second clause) or simply below
     * `threshold`. Shared by `isEligible` (which must also reject a permanently empty group)
     * and `Session.declineBuild` (which must *not* count or log a fallback for a group that
     * was never a candidate in the first place), so the two can't drift apart on this
     * question the way they once did.
     */
    def rowCountRulesOut(matches: ExternalAppendOnlyUnsafeRowArray): Boolean =
      matches.length == 0 || matches.length < threshold

    /**
     * `matches.bufferSize`, consulted by `estimatedRetainedBytes` for boxed keys, is
     * only valid while `!isSpillBacked`; short-circuiting on that check first, before it is
     * reached, keeps this safe for a spill-backed group.
     */
    def isEligible(matches: ExternalAppendOnlyUnsafeRowArray): Boolean = {
      !rowCountRulesOut(matches) && !matches.isSpillBacked &&
        estimatedRetainedBytes(matches) <= maxBytes
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
     * `currentRelationOrNull` are consulted. Builds the index once `MIN_PROBES_BEFORE_BUILD` probes
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
      if (probeCount == 0 && !spec.isEligible(matches)) {
        declineBuild(matches)
        return
      }
      probeCount += 1
      if (probeCount >= MIN_PROBES_BEFORE_BUILD) {
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
      if (!spec.rowCountRulesOut(matches)) {
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
}
