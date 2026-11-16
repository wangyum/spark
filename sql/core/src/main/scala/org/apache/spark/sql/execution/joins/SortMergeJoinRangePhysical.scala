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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.{ExtractRangeJoinKeys, PartialRangeJoin, PointInRangeJoin, RangeJoinKind}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.internal.SQLConf

/** Physical-layer recognition and binding for sort-merge key-group range indexes. */
private[joins] object SortMergeJoinRangePhysical {

  def createProbeSpec(
      condition: Option[Expression],
      joinType: JoinType,
      equiLeftKeys: Seq[Expression],
      equiRightKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
      conf: SQLConf): Option[RangeGroupProbe.Spec] = {
    if (!conf.sortMergeJoinRangeIndexEnabled) {
      return None
    }
    joinType match {
      case FullOuter => None
      case _ =>
        condition.flatMap { cond =>
          ExtractRangeJoinKeys.fromJoinCondition(cond, leftOutput, rightOutput).flatMap {
            case (leftRange, rightRange, kind) =>
              if (rangeOverlapsEquiKeys(leftRange, rightRange, equiLeftKeys, equiRightKeys)) {
                None
              } else {
                orientAndBind(
                  joinType,
                  leftRange,
                  rightRange,
                  kind,
                  leftOutput,
                  rightOutput,
                  conf)
              }
          }
        }
    }
  }

  private def rangeOverlapsEquiKeys(
      leftRange: Seq[Expression],
      rightRange: Seq[Expression],
      equiLeftKeys: Seq[Expression],
      equiRightKeys: Seq[Expression]): Boolean = {
    val equiRefs = AttributeSet(equiLeftKeys ++ equiRightKeys)
    AttributeSet(leftRange ++ rightRange).intersect(equiRefs).nonEmpty
  }

  private def orientAndBind(
      joinType: JoinType,
      leftRange: Seq[Expression],
      rightRange: Seq[Expression],
      kind: RangeJoinKind,
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
      conf: SQLConf): Option[RangeGroupProbe.Spec] = {
    val (streamOutput, bufferOutput, streamRange, bufferRange, buildSide) = joinType match {
      case _: InnerLike | LeftOuter | LeftExistence(_) =>
        (leftOutput, rightOutput, leftRange, rightRange, BuildRight)
      case RightOuter =>
        (rightOutput, leftOutput, rightRange, leftRange, BuildLeft)
      case other =>
        throw new IllegalArgumentException(
          s"SortMergeJoinRangePhysical does not support $other")
    }

    // Only scalar point range keys on the buffered side are indexed
    if (bufferRange.length != 1 ||
        !RangeIndex.canMapKeysToLong(bufferRange) ||
        !RangeIndex.canMapKeysToLong(streamRange)) {
      return None
    }

    val pointScanOpt = kind match {
      case _: PartialRangeJoin if streamRange.length == 1 =>
        val (scan, strict) = PointScan.forJoin(kind, buildSide)
        val compactScan = scan match {
          case PointScan.UpTo => CompactPointScan.UpTo
          case PointScan.From => CompactPointScan.From
        }
        Some((compactScan, strict))
      case PointInRangeJoin if streamRange.length == 2 =>
        Some((CompactPointScan.Between, false))
      case _ =>
        None
    }

    pointScanOpt.map { case (pointScan, strict) =>
      val boundBufferKey = BindReferences.bindReference(bufferRange.head, bufferOutput)
      val boundStreamKeys = BindReferences.bindReferences(streamRange, streamOutput)
      new RangeGroupProbe.Spec(
        boundBufferKey,
        boundStreamKeys,
        bufferRange.head.dataType,
        pointScan,
        strict,
        conf.sortMergeJoinRangeIndexThreshold,
        conf.sortMergeJoinRangeIndexMaxRows,
        conf.sortMergeJoinRangeIndexMaxBytes)
    }
  }
}
