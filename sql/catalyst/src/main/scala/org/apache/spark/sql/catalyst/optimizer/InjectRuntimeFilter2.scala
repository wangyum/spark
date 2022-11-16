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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Insert a filter on one side of the join if the other side has a selective predicate.
 * The filter could be an IN subquery (converted to a semi join), a bloom filter, or something
 * else in the future.
 */
object InjectRuntimeFilter2 extends Rule[LogicalPlan]
  with PredicateHelper with JoinSelectionHelper {

  // Wraps `expr` with a hash function if its byte size is larger than an integer.
  private def mayWrapWithHash(expr: Expression): Expression = {
    if (expr.dataType.defaultSize > IntegerType.defaultSize) {
      new Murmur3Hash(Seq(expr))
    } else {
      expr
    }
  }

  private def injectFilter(
      filterApplicationSideExp: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSideExp: Expression,
      filterCreationSidePlan: LogicalPlan,
      joinKeys: Seq[Expression]): LogicalPlan = {
    injectBloomFilter(
      filterApplicationSideExp,
      filterApplicationSidePlan,
      filterCreationSideExp,
      filterCreationSidePlan,
      joinKeys
    )
  }

  private def injectBloomFilter(
      filterApplicationSideExp: Expression,
      filterApplicationSidePlan: LogicalPlan,
      filterCreationSideExp: Expression,
      filterCreationSidePlan: LogicalPlan,
      joinKeys: Seq[Expression]): LogicalPlan = {
    val filter = DynamicPruningSubquery(filterApplicationSideExp,
      filterCreationSidePlan, joinKeys, joinKeys.indexOf(filterCreationSideExp),
      onlyInBroadcast = false)
    Filter(filter, filterApplicationSidePlan)
  }

  private def isSimpleExpression(e: Expression): Boolean = {
    !e.containsAnyPattern(PYTHON_UDF, SCALA_UDF, INVOKE, JSON_TO_STRUCT, LIKE_FAMLIY,
      REGEXP_EXTRACT_FAMILY, REGEXP_REPLACE)
  }

  private def isProbablyShuffleJoin(left: LogicalPlan,
      right: LogicalPlan, hint: JoinHint): Boolean = {
    !hintToBroadcastLeft(hint) && !hintToBroadcastRight(hint) &&
      !canBroadcastBySize(left, conf) && !canBroadcastBySize(right, conf)
  }

  // Returns the max scan byte size in the subtree rooted at `filterApplicationSide`.
  private def maxScanByteSize(filterApplicationSide: LogicalPlan): BigInt = {
    val defaultSizeInBytes = conf.getConf(SQLConf.DEFAULT_SIZE_IN_BYTES)
    filterApplicationSide.collect({
      case leaf: LeafNode => leaf
    }).map(scan => {
      // DEFAULT_SIZE_IN_BYTES means there's no byte size information in stats. Since we avoid
      // creating a Bloom filter when the filter application side is very small, so using 0
      // as the byte size when the actual size is unknown can avoid regression by applying BF
      // on a small table.
      if (scan.stats.sizeInBytes == defaultSizeInBytes) BigInt(0) else scan.stats.sizeInBytes
    }).max
  }

  // Returns true if `filterApplicationSide` satisfies the byte size requirement to apply a
  // Bloom filter; false otherwise.
  private def satisfyByteSizeRequirement(filterApplicationSide: LogicalPlan): Boolean = {
    // In case `filterApplicationSide` is a union of many small tables, disseminating the Bloom
    // filter to each small task might be more costly than scanning them itself. Thus, we use max
    // rather than sum here.
    val maxScanSize = maxScanByteSize(filterApplicationSide)
    maxScanSize >=
      conf.getConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD)
  }

  private def rowCounts(plan: LogicalPlan): BigInt = {
    plan.stats.rowCount
      .getOrElse(plan.stats.sizeInBytes / EstimationUtils.getSizePerRow(plan.output))
  }

  /**
   * Check that:
   * - The filterApplicationSideJoinExp can be pushed down through joins, aggregates and windows
   *   (ie the expression references originate from a single leaf node)
   * - The filter creation side has a selective predicate
   * - The current join is a shuffle join or a broadcast join that has a shuffle below it
   * - The max filterApplicationSide scan size is greater than a configurable threshold
   */
  private def filteringHasBenefit(
      filterApplicationSide: LogicalPlan,
      filterCreationSide: LogicalPlan,
      filterApplicationSideExp: Expression,
      hint: JoinHint): Boolean = {
    probablyPushThroughShuffle(filterApplicationSideExp, filterApplicationSide) &&
      satisfyByteSizeRequirement(filterApplicationSide) &&
      filterCreationSide.stats.sizeInBytes < conf.runtimeFilterCreationSideThreshold &&
      filterCreationSide.exists {
        case f: Filter => isLikelySelective(f.condition)
        case _ => false
    }
  }

  def hasRuntimeFilter(left: LogicalPlan, right: LogicalPlan, leftKey: Expression,
      rightKey: Expression): Boolean = {
    if (conf.runtimeFilterBloomFilterEnabled) {
      hasBloomFilter(left, right, leftKey, rightKey)
    } else {
      hasInSubquery(left, right, leftKey, rightKey)
    }
  }

  // This checks if there is already a DPP filter, as this rule is called just after DPP.
  def hasDynamicPruningSubquery(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKey: Expression,
      rightKey: Expression): Boolean = {
    (left, right) match {
      case (Filter(DynamicPruningSubquery(pruningKey, _, _, _, _, _), plan), _) =>
        pruningKey.fastEquals(leftKey) || hasDynamicPruningSubquery(plan, right, leftKey, rightKey)
      case (_, Filter(DynamicPruningSubquery(pruningKey, _, _, _, _, _), plan)) =>
        pruningKey.fastEquals(rightKey) ||
          hasDynamicPruningSubquery(left, plan, leftKey, rightKey)
      case _ => false
    }
  }

  def hasBloomFilter(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKey: Expression,
      rightKey: Expression): Boolean = {
    findBloomFilterWithExp(left, leftKey) || findBloomFilterWithExp(right, rightKey)
  }

  private def findBloomFilterWithExp(plan: LogicalPlan, key: Expression): Boolean = {
    plan.exists {
      case Filter(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case BloomFilterMightContain(_, XxHash64(Seq(valueExpression), _))
            if valueExpression.fastEquals(key) => true
          case _ => false
        }
      case _ => false
    }
  }

  def hasInSubquery(left: LogicalPlan, right: LogicalPlan, leftKey: Expression,
      rightKey: Expression): Boolean = {
    (left, right) match {
      case (Filter(InSubquery(Seq(key),
      ListQuery(Aggregate(Seq(Alias(_, _)), Seq(Alias(_, _)), _), _, _, _, _)), _), _) =>
        key.fastEquals(leftKey) || key.fastEquals(new Murmur3Hash(Seq(leftKey)))
      case (_, Filter(InSubquery(Seq(key),
      ListQuery(Aggregate(Seq(Alias(_, _)), Seq(Alias(_, _)), _), _, _, _, _)), _)) =>
        key.fastEquals(rightKey) || key.fastEquals(new Murmur3Hash(Seq(rightKey)))
      case _ => false
    }
  }

  // Make sure injected filters could push through Shuffle, see PushPredicateThroughNonJoin
  private def probablyPushThroughShuffle(exp: Expression, plan: LogicalPlan): Boolean = {
    plan match {
      case j: Join if !canPlanAsBroadcastHashJoin(j, conf) => true
      case a @ Aggregate(groupingExps, aggExps, child)
        if aggExps.forall(_.deterministic) && groupingExps.nonEmpty &&
          replaceAlias(exp, getAliasMap(a)).references.subsetOf(child.outputSet) => true
      case w: Window
        if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) &&
          exp.references.subsetOf(AttributeSet(w.partitionSpec.flatMap(_.references))) => true
      case p: Project =>
        probablyPushThroughShuffle(replaceAlias(exp, getAliasMap(p)), p.child)
      case other =>
        other.children.exists { p =>
          if (exp.references.subsetOf(p.outputSet)) probablyPushThroughShuffle(exp, p) else false
        }
    }
  }

  private def tryInjectRuntimeFilter(plan: LogicalPlan): LogicalPlan = {
    var filterCounter = 0
    val numFilterThreshold = conf.getConf(SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD)
    plan transformUp {
      case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, hint) =>
        var newLeft = left
        var newRight = right
        (leftKeys, rightKeys).zipped.foreach((l, r) => {
          // Check if:
          // 1. There is already a DPP filter on the key
          // 2. There is already a runtime filter (Bloom filter or IN subquery) on the key
          // 3. The keys are simple cheap expressions
          if (filterCounter < numFilterThreshold &&
            !hasDynamicPruningSubquery(left, right, l, r) &&
            !hasRuntimeFilter(newLeft, newRight, l, r) &&
            isSimpleExpression(l) && isSimpleExpression(r)) {
            val oldLeft = newLeft
            val oldRight = newRight
            if (canPruneLeft(joinType) && filteringHasBenefit(left, right, l, hint)) {
              newLeft = injectFilter(l, newLeft, r, right, rightKeys)
            }
            // Did we actually inject on the left? If not, try on the right
            if (newLeft.fastEquals(oldLeft) && canPruneRight(joinType) &&
              filteringHasBenefit(right, left, r, hint)) {
              newRight = injectFilter(r, newRight, l, left, leftKeys)
            }
            if (!newLeft.fastEquals(oldLeft) || !newRight.fastEquals(oldRight)) {
              filterCounter = filterCounter + 1
            }
          }
        })
        join.withNewChildren(Seq(newLeft, newRight))
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case s: Subquery if s.correlated => plan
    case _ => tryInjectRuntimeFilter(plan)
  }

}
