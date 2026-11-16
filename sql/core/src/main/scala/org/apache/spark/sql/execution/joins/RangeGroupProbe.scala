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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{NUM_ROWS, THRESHOLD}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{ExtractRangeJoinKeys, PartialRangeJoin}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

private[joins] class RangeIndexMemoryConsumer(taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) {

  /**
   * Set by `Session` once it has a handle to release. `spill()` only frees memory it already
   * holds - it never calls `acquireMemory`, so it cannot re-enter the TaskMemoryManager - and
   * returns 0 on a self-trigger, mirroring `WindowSegmentTree.SegTreeSpiller`'s I2 invariant.
   */
  @volatile private[this] var releaseHandler: () => Long = () => 0L

  private[joins] def onRelease(f: () => Long): Unit = releaseHandler = f

  override def spill(size: Long, trigger: MemoryConsumer): Long =
    if (trigger eq this) 0L else releaseHandler()
}

/**
 * Encapsulates per-equi-key group range index management, budgeting, and candidate probing.
 */
private[joins] object RangeGroupProbe {

  final class Spec(
      val mode: RangeRelationBroadcastMode,
      val boundStreamKeys: Seq[Expression],
      val pointScan: (PointScan, Boolean),
      val threshold: Int,
      val maxRows: Int,
      val maxBytes: Long) extends Serializable {

    /**
     * Whether the build keys map to one order-preserving long (`RangeIndex.longKey`), fixed
     * for the life of the plan. Computed once here instead of per group build, and consulted
     * by codegen (`RangeIndexProbeCodegen`) to pick the concrete index class and key encoding
     * without re-deriving it from `mode.buildKeys` on every probe.
     */
    val usesLongKeys: Boolean = RangeIndex.canMapKeysToLong(mode.buildKeys)

    /**
     * A cheap pre-admission estimate used to reserve memory before the index is built (the
     * real footprint, `RangeRelation.estimatedSize`, only exists once the relation has been
     * constructed). `build()` trues this up against the real size afterward.
     */
    def estimateMemoryBytes(numRows: Long): Long = {
      val bytesPerRow = mode.indexKind match {
        case IntervalIndexKind => 64L
        case PointIndexKind => 32L
      }
      numRows * bytesPerRow
    }

    def isEligible(numRows: Long, isSpillBacked: Boolean): Boolean = {
      numRows >= threshold && numRows <= maxRows && !isSpillBacked &&
        estimateMemoryBytes(numRows) <= maxBytes
    }
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

    private[this] val memoryConsumer: RangeIndexMemoryConsumer = {
      if (taskContext != null) {
        val consumer = new RangeIndexMemoryConsumer(taskContext.taskMemoryManager())
        consumer.onRelease(() => releaseForSpill())
        consumer
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

    def onGroupBuffered(matches: ExternalAppendOnlyUnsafeRowArray): Unit = resetGroup()

    /**
     * Called once per streamed row probing the current buffered group, before
     * `candidates`/`currentRelationOrNull` are consulted. The index is only worth its one-time
     * build cost once a *second* probe confirms the group is actually reused: a group probed
     * exactly once - the common one-to-many join shape, e.g. a unique dimension key joined to
     * a skewed fact table - pays only the linear scan it would have paid anyway, instead of
     * also building an index nothing will reuse. `buildAttempted` latches the budget/
     * eligibility decision so a later probe never re-evaluates it for the same group.
     */
    def onProbe(matches: ExternalAppendOnlyUnsafeRowArray): Unit = {
      probeCount += 1
      if (currentHandle == null && !buildAttempted && probeCount >= 2) {
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
     * (fewer than two probes so far), fell back to a linear scan (below `threshold`, over a
     * row/byte budget, or buffered-side spilled), or had its index evicted under memory
     * pressure. Used by `SortMergeJoinExec`'s whole-stage codegen to generate a direct,
     * specialized probe call via `RangeIndexProbeCodegen` instead of going through
     * `candidates`' interpreted dispatch. Callers must call `onProbe` first; this accessor
     * itself has no side effect.
     */
    def currentRelationOrNull: RangeRelation =
      if (currentHandle != null) currentHandle.relation else null

    /**
     * Releases the current group's index on a TaskMemoryManager spill request, falling back to
     * the always-available linear scan on the buffered group for the rest of the group.
     */
    private def releaseForSpill(): Long = {
      if (currentHandle != null) {
        val bytes = currentHandle.allocatedBytes
        release(currentHandle, memoryConsumer)
        currentHandle = null
        // Don't retry the build for the rest of this group: the memory just freed is exactly
        // what a retry would need to re-acquire, and retrying would just spill again.
        buildAttempted = true
        bytes
      } else {
        0L
      }
    }

    def resetGroup(): Unit = {
      if (currentHandle != null) {
        release(currentHandle, memoryConsumer)
        currentHandle = null
      }
      probeCount = 0
      buildAttempted = false
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
    if (!conf.sortMergeJoinRangeIndexEnabled) {
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
                  case _ => (PointScan.UpTo, false)
                }
                Some(new Spec(
                  mode,
                  boundStreamKeys,
                  pointScan,
                  conf.sortMergeJoinRangeIndexThreshold,
                  conf.sortMergeJoinRangeIndexMaxRows,
                  conf.sortMergeJoinRangeIndexMaxBytes))
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

    val relation = spec.mode.transform(matches.generateIterator(), Some(n), spec.usesLongKeys)
    // The nominal estimate above is a cheap per-row heuristic; true it up against the real,
    // SizeEstimator-based footprint of the relation we just built so the TaskMemoryManager
    // budget reflects actual heap usage rather than a fixed-bytes-per-row guess.
    val actualBytes = relation.estimatedSize

    if (actualBytes > spec.maxBytes) {
      // The real footprint exceeds the configured cap even though the cheap per-row estimate
      // didn't predict it (e.g. a wide buffered-side schema); release and fall back instead of
      // silently exceeding maxBytes.
      if (consumer != null) consumer.freeMemory(acquired)
      return null
    }

    val allocatedBytes = if (actualBytes > acquired) {
      val extra = actualBytes - acquired
      val extraAcquired = if (consumer != null) consumer.acquireMemory(extra) else extra
      if (extraAcquired < extra) {
        if (consumer != null) {
          if (extraAcquired > 0) consumer.freeMemory(extraAcquired)
          consumer.freeMemory(acquired)
        }
        return null
      }
      acquired + extraAcquired
    } else if (actualBytes < acquired) {
      // We over-reserved relative to the real footprint; release the excess so the consumer's
      // `used` accounting tracks actual heap usage.
      if (consumer != null) consumer.freeMemory(acquired - actualBytes)
      actualBytes
    } else {
      acquired
    }

    val candidateFn = RangeRelationProbe.makeCandidateFn(
      relation, spec.boundStreamKeys, spec.pointScan)
    new Handle(relation, allocatedBytes, candidateFn)
  }

  def release(handle: Handle, consumer: RangeIndexMemoryConsumer): Unit = {
    if (handle != null && consumer != null && handle.allocatedBytes > 0) {
      consumer.freeMemory(handle.allocatedBytes)
    }
  }
}
