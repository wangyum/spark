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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, PointInRangeJoin, RangeJoin, RangePredicate}
import org.apache.spark.sql.types.DataType

/**
 * Keys for an [[IntervalIndex]]-backed probe of a sort merge join's buffered match group.
 * `probeKeys` are evaluated against the streamed row and `buildKeys` against the buffered rows;
 * both are unbound (reference the respective side's output attributes).
 */
private[joins] case class RangeResidualKeys(
    buildKeys: Seq[Expression],
    probeKeys: Seq[Expression],
    shape: RangeJoin)

/**
 * Recognizes a point-in-range or interval-overlap shape (see [[RangePredicate]]) within a sort
 * merge join's residual condition, so [[SortMergeJoinEvaluatorFactory]] can probe an
 * [[IntervalIndex]] over a large buffered match group instead of scanning it linearly.
 *
 * This is a physical-plan analog of
 * [[org.apache.spark.sql.catalyst.planning.ExtractRangeJoinKeys]]: the same predicate matching,
 * but placement is decided by attribute-set membership against the streamed/buffered side's
 * output, instead of `canEvaluate` against a [[LogicalPlan]] child.
 *
 * The index only narrows candidates; the original join condition is still evaluated on every
 * candidate it returns, so a conservative (or even wrong) match here can only cost performance,
 * never correctness, provided the index never drops a row the condition would have accepted.
 */
private[joins] object SortMergeJoinRangeResidual extends PredicateHelper {

  def extract(
      condition: Expression,
      streamedOutput: Seq[Attribute],
      bufferedOutput: Seq[Attribute]): Option[RangeResidualKeys] = {
    val streamedSet = AttributeSet(streamedOutput)
    val bufferedSet = AttributeSet(bufferedOutput)
    val rangePreds = splitConjunctivePredicates(condition).collect {
      case RangePredicate(dt, low, high) if RangePredicate.supportedType(dt) => (dt, low, high)
    }
    if (rangePreds.isEmpty) {
      None
    } else {
      pointInRange(rangePreds, streamedSet, bufferedSet)
        .orElse(intervalOverlap(rangePreds, streamedSet, bufferedSet))
    }
  }

  /**
   * Puts `exprsA` on the streamed side when it can evaluate all of them and the buffered side can
   * evaluate `exprsB`. Otherwise swaps, when the opposite assignment fits. A key is a column of
   * one input, so at most one assignment succeeds. Returns `(probeKeys, buildKeys)`.
   */
  private def place(
      exprsA: Seq[Expression],
      exprsB: Seq[Expression],
      streamedSet: AttributeSet,
      bufferedSet: AttributeSet): Option[(Seq[Expression], Seq[Expression])] = {
    if (exprsA.forall(_.references.subsetOf(streamedSet)) &&
        exprsB.forall(_.references.subsetOf(bufferedSet))) {
      Some((exprsA, exprsB))
    } else if (exprsA.forall(_.references.subsetOf(bufferedSet)) &&
        exprsB.forall(_.references.subsetOf(streamedSet))) {
      Some((exprsB, exprsA))
    } else {
      None
    }
  }

  private def pointInRange(
      rangePreds: Seq[(DataType, Expression, Expression)],
      streamedSet: AttributeSet,
      bufferedSet: AttributeSet): Option[RangeResidualKeys] = {
    rangePreds.combinations(2).flatMap {
      case Seq((d1, l1, h1), (d2, l2, h2)) if d1 == d2 =>
        val pointCandidate =
          if (h1.semanticEquals(l2) && !l1.semanticEquals(h2) && l1.dataType == h2.dataType) {
            place(Seq(l2), Seq(l1, h2), streamedSet, bufferedSet)
          } else if (l1.semanticEquals(h2) && !h1.semanticEquals(l2) &&
              l2.dataType == h1.dataType) {
            place(Seq(l1), Seq(l2, h1), streamedSet, bufferedSet)
          } else {
            None
          }
        pointCandidate.collect {
          case (probeKeys, buildKeys) if supportedKeys(probeKeys, buildKeys) =>
            RangeResidualKeys(buildKeys, probeKeys, PointInRangeJoin)
        }
      case _ => None
    }.nextOption()
  }

  private def intervalOverlap(
      rangePreds: Seq[(DataType, Expression, Expression)],
      streamedSet: AttributeSet,
      bufferedSet: AttributeSet): Option[RangeResidualKeys] = {
    rangePreds.combinations(2).flatMap {
      case Seq((d1, l1, h1), (d2, l2, h2)) if d1 == d2 =>
        // `(l1, h2)` is one interval and `(l2, h1)` is the other.
        val overlapCandidate = place(Seq(l1, h2), Seq(l2, h1), streamedSet, bufferedSet)
        overlapCandidate.collect {
          case (probeKeys, buildKeys)
              if !probeKeys.head.semanticEquals(probeKeys(1)) &&
                !buildKeys.head.semanticEquals(buildKeys(1)) &&
                probeKeys.head.dataType == probeKeys(1).dataType &&
                buildKeys.head.dataType == buildKeys(1).dataType &&
                supportedKeys(probeKeys, buildKeys) =>
            RangeResidualKeys(buildKeys, probeKeys, IntervalOverlapJoin)
        }
      case _ => None
    }.nextOption()
  }

  private def supportedKeys(keys: Seq[Expression]*): Boolean = {
    keys.flatten.forall { key =>
      key.references.nonEmpty && key.deterministic &&
        !SubqueryExpression.hasSubquery(key) && RangePredicate.supportedType(key.dataType)
    }
  }
}
