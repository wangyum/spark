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
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.{ExtractRangeJoinKeys, PartialRangeJoin}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf

private[joins] class RangeIndexMemoryConsumer(taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) {
  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
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
      numRows > threshold && numRows <= maxRows && !isSpillBacked &&
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
      numFallbackMetric: SQLMetric = null) {

    private[this] val memoryConsumer: RangeIndexMemoryConsumer = {
      val tc = TaskContext.get()
      if (tc != null) new RangeIndexMemoryConsumer(tc.taskMemoryManager()) else null
    }

    private[this] var currentHandle: Handle = _

    Option(TaskContext.get()).foreach { tc =>
      tc.addTaskCompletionListener[Unit](_ => resetGroup())
    }

    def onGroupBuffered(matches: ExternalAppendOnlyUnsafeRowArray): Unit = {
      resetGroup()
      currentHandle = build(matches, spec, memoryConsumer)
      if (currentHandle != null) {
        if (numIndexedMetric != null) numIndexedMetric += 1
      } else if (matches.length > spec.threshold) {
        if (numFallbackMetric != null) numFallbackMetric += 1
      }
    }

    def candidates(
        matches: ExternalAppendOnlyUnsafeRowArray,
        streamedRow: InternalRow): Iterator[InternalRow] = {
      if (currentHandle != null) {
        currentHandle.candidates(streamedRow)
      } else {
        matches.generateIterator()
      }
    }

    /**
     * The current group's materialized index, or `null` when the group fell back to a linear
     * scan (below `threshold`, over a row/byte budget, or buffered-side spilled). Used by
     * `SortMergeJoinExec`'s whole-stage codegen to generate a direct, specialized probe call
     * via `RangeIndexProbeCodegen` instead of going through `candidates`' interpreted dispatch.
     */
    def currentRelationOrNull: RangeRelation =
      if (currentHandle != null) currentHandle.relation else null

    def resetGroup(): Unit = {
      if (currentHandle != null) {
        release(currentHandle, memoryConsumer)
        currentHandle = null
      }
    }
  }

  def createSpec(
      condition: Option[Expression],
      joinType: JoinType,
      equiLeftKeys: Seq[Expression],
      equiRightKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
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
                val (streamOutput, bufferOutput, streamRange, bufferRange, buildSide) =
                  joinType match {
                    case _: InnerLike | LeftOuter | LeftExistence(_) =>
                      (leftOutput, rightOutput, leftRange, rightRange, BuildRight)
                    case RightOuter =>
                      (rightOutput, leftOutput, rightRange, leftRange, BuildLeft)
                    case other =>
                      throw new IllegalArgumentException(s"Unsupported join type: $other")
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

    val relation = spec.mode.transform(matches.generateIterator(), Some(n))
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
