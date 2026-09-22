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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.PointInRangeJoin
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{
  BroadcastDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.execution.{ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs an inner range join on two tables. A range join matches rows whose join condition can
 * be reduced to one of the following forms:
 *
 * 1. Point-in-range: a value from one side falls within a `[low, high]` range on the other side,
 *    e.g. an IP-lookup-style join:
 *    {{{
 *      SELECT A.*, B.*
 *      FROM   tableA A JOIN tableB B
 *      ON     A.ip >= B.low AND A.ip <= B.high
 *    }}}
 * 2. Partial range: a single inequality between one column on each side, e.g.:
 *    {{{
 *      SELECT A.*, B.*
 *      FROM   tableA A JOIN tableB B
 *      ON     A.start < B.end
 *    }}}
 *
 * See `ExtractRangeJoinKeys` for the exact set of join-condition shapes that are recognized.
 *
 * The implementation builds a sorted event index from the smaller build side in O(m log m)
 * and broadcasts it. Each streamed row probes the index in O(log m + k), where m is the
 * number of build rows and k is the number of candidates whose events overlap the probe
 * key. Residual and exclusive-bound checks still run on those k rows. When build-side
 * intervals overlap heavily, k can approach m, so the worst-case comparison count is
 * still O(n * m) like nested loop; typical range predicates have k much smaller than m.
 */
@DeveloperApi
case class BroadcastRangeJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    rangeInfo: RangeInfo)
  extends BaseJoinExec {

  override def condition: Option[Expression] = rangeInfo.normalizedRestCondition

  // Range keys on each child, recovered from the build/stream split in `rangeInfo`.
  override def leftKeys: Seq[Expression] = buildSide match {
    case BuildLeft => rangeInfo.normalizedBuildKeys
    case BuildRight => rangeInfo.normalizedStreamedKeys
  }
  override def rightKeys: Seq[Expression] = buildSide match {
    case BuildLeft => rangeInfo.normalizedStreamedKeys
    case BuildRight => rangeInfo.normalizedBuildKeys
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastRangeJoin should not take $x as the JoinType")
    }
  }

  // Only inner-like joins are supported (see `output`), so the streamed side's partitioning
  // and ordering are preserved through the join.
  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = streamedPlan.outputOrdering

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType $buildSide ($opId)".trim
  }

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = condition.map(_.toString).getOrElse("None")
    val rangeJoinStr = rangeInfo.rangeJoin.toString
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
       |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
       |${ExplainUtils.generateFieldString("Join type", joinType.toString)}
       |${ExplainUtils.generateFieldString("Build side", buildSide.toString)}
       |${ExplainUtils.generateFieldString("Range join type", rangeJoinStr)}
       |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
       |""".stripMargin
  }

  private[this] lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  // Both sides project [low, high] once per row in the partition iterator.
  private def keyProjection(
      keys: Seq[Expression],
      output: Seq[Attribute]): () => Projection =
    () => newProjection(keys, output)

  // Type-specialized accessors for a projected [low, high] row. Unlike `keyProjection`
  // (an `InterpretedProjection`, not serializable) these getters are plain
  // `InternalRow => Any` closures capturing only the (serializable) `DataType`, so
  // they are not marked `@transient` and can be captured by the per-partition closure.
  private def keyValueGetters(keys: Seq[Expression]): List[InternalRow => Any] =
    RangeIndex.getValue(keys(0).dataType, 0) ::
      RangeIndex.getValue(keys(1).dataType, 1) :: Nil

  @transient
  private[this] lazy val streamSideKeyGenerator: () => Projection =
    keyProjection(rangeInfo.normalizedStreamedKeys, rangeInfo.normalizedStreamedPlanOutput)

  private[this] lazy val streamSideKeyValueGetter: List[InternalRow => Any] =
    keyValueGetters(rangeInfo.normalizedStreamedKeys)

  override lazy val requiredChildDistribution: Seq[Distribution] = {
    val mode = RangeBroadcastMode(rangeInfo)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  @transient private lazy val boundRestCondition = {
    if (rangeInfo.normalizedRestCondition.isDefined) {
      Predicate.create(
        rangeInfo.normalizedRestCondition.get, rangeInfo.normalizedAllOutput).eval _
    } else {
      (r: InternalRow) => true
    }
  }

  // Build-side key extraction for the range-condition post-filter. The range
  // index returns a superset; these extractors let innerJoin reject false
  // positives (e.g. rows whose `hi` falls short of the query high when the
  // high bound is exclusive) by re-checking the actual range predicate.
  @transient
  private[this] lazy val buildSideKeyGenerator: () => Projection =
    keyProjection(rangeInfo.normalizedBuildKeys, rangeInfo.normalizedBuildPlanOutput)

  private[this] lazy val buildSideKeyValueGetter: List[InternalRow => Any] =
    keyValueGetters(rangeInfo.normalizedBuildKeys)

  // Range-condition post-filter metadata. These are plan-time properties derived from
  // `rangeInfo` (`rangeJoin` / `buildSide` / `buildSideIsPoint`), so they are computed
  // once per exec rather than per partition. The range index returns a superset of
  // matching rows; the bound checks below reject the false positives.
  @transient private[this] lazy val allowLowEq: Boolean = rangeInfo.equality.lowInclusive
  @transient private[this] lazy val allowHighEq: Boolean = rangeInfo.equality.highInclusive
  @transient private[this] lazy val rangeOrdering: Ordering[Any] =
    PhysicalDataType.ordering(rangeInfo.normalizedStreamedKeys(0).dataType)

  // Each bound is a single comparison between a stream value and a build value; only the
  // direction and inclusivity vary. `streamLeBuild` selects the direction:
  //   true  => stream <= build,  false => build <= stream
  // and the matching `allow*Eq` flag controls whether equality satisfies the bound.
  // Partial-range joins have a single inequality, so they have no high bound.
  @transient private[this] lazy val lowStreamLeBuild: Boolean = rangeInfo.rangeJoin match {
    case PointInRangeJoin if rangeInfo.buildSideIsPoint => true
    case PointInRangeJoin => false
    case _ => RangeIndex.highRangeOfPartialRangeJoin(rangeInfo.rangeJoin, buildSide)
  }
  @transient private[this] lazy val hasHighBound: Boolean =
    rangeInfo.rangeJoin == PointInRangeJoin
  // The high bound only exists for point-in-range joins, where its direction is always the
  // opposite of the low bound's (the point is checked against both ends of the range). For
  // partial-range joins `hasHighBound` is false and this value is never consulted.
  @transient private[this] lazy val highStreamLeBuild: Boolean = !lowStreamLeBuild

  @inline private[this] def le(a: Any, b: Any, allowEqual: Boolean): Boolean = {
    val cmp = rangeOrdering.compare(a, b)
    cmp < 0 || (allowEqual && cmp == 0)
  }
  private[this] def lowSatisfies(s: Any, b: Any): Boolean =
    if (lowStreamLeBuild) le(s, b, allowLowEq) else le(b, s, allowLowEq)
  private[this] def highSatisfies(s: Any, b: Any): Boolean =
    if (!hasHighBound) true
    else if (highStreamLeBuild) le(s, b, allowHighEq) else le(b, s, allowHighEq)

  protected def newProjection(expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): Projection = {
    new InterpretedProjection(expressions, inputSchema)
  }

  override def doExecute(): RDD[InternalRow] = {
    // Planner only constructs this operator for inner-like joins; `output` enforces
    // the same invariant if a non-inner type is ever copied in.
    val rangeIndex: Broadcast[RangeIndex] = buildPlan.executeBroadcast[RangeIndex]()
    innerJoin(rangeIndex)
  }

  private def innerJoin(rangeIndex: Broadcast[RangeIndex]): RDD[InternalRow] = {
    // Iterate over the streaming relation.
    val resultRdd = streamedPlan.execute().mapPartitions { stream =>
      new Iterator[InternalRow] {
        private[this] val index = rangeIndex.value
        private[this] val streamSideKeys: Projection = streamSideKeyGenerator()
        private[this] val buildSideKeys: Projection = buildSideKeyGenerator()
        private[this] val joinedRow = new JoinedRow
        private[this] var matchIterator: Iterator[(InternalRow, Int)] = Iterator.empty

        // current row from stream side
        private var streamRow: InternalRow = null
        // low/high key values of `streamRow`, computed once per stream row and reused
        // across all candidate build rows for that stream row.
        private var streamLow: Any = null
        private var streamHigh: Any = null
        // the next row to emit, or null if not yet found
        private var resultRow: InternalRow = null

        /**
         * Re-check the range predicate against a candidate build row and the current
         * stream row. Returns true if the build row's [lo, hi] actually satisfies the
         * range condition with the stream row's [low, high]. The index returns a
         * superset, so this rejects false positives (e.g. rows whose `hi` falls short
         * of the query high when the high bound is exclusive). `streamLow`/`streamHigh`
         * are the current stream row's key values, computed once per stream row by the
         * caller rather than per candidate. The bound checks (`lowSatisfies`/
         * `highSatisfies`) are exec-level and partition-invariant.
         */
        private def rangeCheck(buildRow: InternalRow): Boolean = {
          val buildLowHigh = buildSideKeys(buildRow)
          val buildLow = buildSideKeyValueGetter(0)(buildLowHigh)
          val buildHigh = buildSideKeyValueGetter(1)(buildLowHigh)
          buildLow != null && buildHigh != null &&
            lowSatisfies(streamLow, buildLow) && highSatisfies(streamHigh, buildHigh)
        }

        /**
         * Advance through the stream and the current match iterator until a row satisfying
         * the residual condition is found or the stream is exhausted. On success, stores the
         * joined row in `resultRow` and returns true. Implemented as a loop (not recursion) so
         * that streams with many non-matching rows do not overflow the stack.
         */
        private def findNextMatch(): Boolean = {
          while (true) {
            // If we have exhausted the matches for the current stream row, advance to the
            // next stream row, project its [low, high] keys once, and start the match
            // iterator against the range index.
            if (streamRow == null) {
              if (!stream.hasNext) return false
              streamRow = stream.next()
              val lowHigh: InternalRow = streamSideKeys(streamRow)
              streamLow = streamSideKeyValueGetter(0)(lowHigh)
              streamHigh = streamSideKeyValueGetter(1)(lowHigh)
              matchIterator =
                if (streamLow != null && streamHigh != null) index.intersect(streamLow, streamHigh)
                else Iterator.empty
            }

            // Drain the current match iterator, checking the range and residual conditions.
            // `streamLow`/`streamHigh` are fixed for this stream row, so rangeCheck reuses
            // them instead of re-projecting `streamRow` for every candidate build row.
            while (matchIterator.hasNext) {
              val buildRow = matchIterator.next()._1
              if (rangeCheck(buildRow)) {
                val r = buildSide match {
                  case BuildRight => joinedRow(streamRow, buildRow)
                  case BuildLeft => joinedRow(buildRow, streamRow)
                }
                if (boundRestCondition(r)) {
                  resultRow = r
                  return true
                }
              }
            }

            // No match for this stream row; move on to the next one.
            streamRow = null
          }
          // Unreachable, but keeps the compiler happy.
          false
        }

        override def hasNext: Boolean = {
          resultRow != null || findNextMatch()
        }

        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  private[this] def genResultProjection: UnsafeProjection =
    UnsafeProjection.create(
      output, (left.output ++ right.output).map(_.withNullability(true)))

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)

  override def doCanonicalize(): SparkPlan = super.doCanonicalize() match {
    case b: BroadcastRangeJoinExec => b.copy(rangeInfo = rangeInfo.canonicalized())
    case other => other
  }
}
