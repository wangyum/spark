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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{
  And, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, PointInRangeJoin, RangeJoin}
import org.apache.spark.sql.catalyst.plans.{
  Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

class RangeJoinSuite extends QueryTest with SharedSparkSession {
  private lazy val intervals1: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(-1, 0),
      Row(0, 1),
      Row(0, 2),
      Row(1, 5)
    )), new StructType().add("low1", IntegerType).add("high1", IntegerType))

  private lazy val points: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(-3),
      Row(1),
      Row(3),
      Row(6)
    )), new StructType().add("point", IntegerType))

  /**
   * Operator-level helper. Keys are the original expressions from each child.
   * `condition` is the original join predicate and the only accept/reject check.
   */
  private def rangeJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      buildKeys: Seq[Expression],
      streamedKeys: Seq[Expression],
      condition: Expression,
      rangeJoin: RangeJoin = PointInRangeJoin): BroadcastRangeJoinExec = {
    val (leftKeys, rightKeys) = buildSide match {
      case BuildLeft => (buildKeys, streamedKeys)
      case BuildRight => (streamedKeys, buildKeys)
    }
    BroadcastRangeJoinExec(
      leftKeys, rightKeys, joinType, buildSide, Some(condition), rangeJoin, left, right)
  }

  private def pointCondition(
      point: Expression,
      low: Expression,
      high: Expression,
      lowInclusive: Boolean,
      highInclusive: Boolean,
      highFirst: Boolean = false): Expression = {
    val lowPred = if (lowInclusive) GreaterThanOrEqual(point, low) else GreaterThan(point, low)
    val highPred = if (highInclusive) LessThanOrEqual(point, high) else LessThan(point, high)
    if (highFirst) And(highPred, lowPred) else And(lowPred, highPred)
  }

  test("interval-point range join") {
    /* low1 <= point && point < high1 */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, Inner,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = pointCondition(
            right.output.head, left.output(0), left.output(1),
            lowInclusive = true, highInclusive = false))
    },
      Seq(
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    /* Same predicate with the high conjunct written first. */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, Inner,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = pointCondition(
            right.output.head, left.output(0), left.output(1),
            lowInclusive = true, highInclusive = false, highFirst = true))
    },
      Seq(
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    /* low1 < point && point < high1 */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, Inner,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = pointCondition(
            right.output.head, left.output(0), left.output(1),
            lowInclusive = false, highInclusive = false))
    },
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    /* low <= point && point <= high1 */
    checkAnswer2(points, intervals1, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, Inner,
        buildKeys = right.output,
        streamedKeys = left.output.head :: left.output.head :: Nil,
        condition = pointCondition(
            left.output.head, right.output(0), right.output(1),
            lowInclusive = true, highInclusive = true))
    },
      Seq(
        (1, 0, 1),
        (1, 0, 2),
        (1, 1, 5),
        (3, 1, 5)
      ).map(Row.fromTuple))

    /* low1 < point && point < high1, build side is the left (interval) relation */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildLeft, Inner,
        buildKeys = left.output,
        streamedKeys = right.output.head :: right.output.head :: Nil,
        condition = pointCondition(
            right.output.head, left.output(0), left.output(1),
            lowInclusive = false, highInclusive = false))
    },
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    /* low1 <= point && point < high1, BuildLeft. */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildLeft, Inner,
        buildKeys = left.output,
        streamedKeys = right.output.head :: right.output.head :: Nil,
        condition = pointCondition(
            right.output.head, left.output(0), left.output(1),
            lowInclusive = true, highInclusive = false))
    },
      Seq(
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))
  }

  test("left outer, right outer, semi, and anti preserve the streamed side") {
    // low <= point && point < high. Intervals (-1, 0) and (0, 1) contain no point.
    val condition = (left: SparkPlan, right: SparkPlan) => pointCondition(
      right.output.head, left.output(0), left.output(1),
      lowInclusive = true, highInclusive = false)

    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, LeftOuter,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = condition(left, right))
    },
      Seq(
        (-1, 0, null),
        (0, 1, null),
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildLeft, RightOuter,
        buildKeys = left.output,
        streamedKeys = right.output.head :: right.output.head :: Nil,
        condition = condition(left, right))
    },
      Seq(
        (null, null, -3),
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3),
        (null, null, 6)
      ).map(Row.fromTuple))

    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, LeftSemi,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = condition(left, right))
    },
      Seq((0, 2), (1, 5)).map(Row.fromTuple))

    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, LeftAnti,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = condition(left, right))
    },
      Seq((-1, 0), (0, 1)).map(Row.fromTuple))
  }

  test("a null streamed bound is not a match") {
    val nullInterval = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(null, 5))),
      new StructType().add("low1", IntegerType).add("high1", IntegerType))
    val condition = (left: SparkPlan, right: SparkPlan) => pointCondition(
      right.output.head, left.output(0), left.output(1),
      lowInclusive = true, highInclusive = false)

    checkAnswer2(nullInterval, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, LeftOuter,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = condition(left, right))
    }, Seq(Row(null, 5, null)))

    checkAnswer2(nullInterval, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, LeftSemi,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = condition(left, right))
    }, Seq.empty)

    checkAnswer2(nullInterval, points, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, LeftAnti,
        buildKeys = right.output.head :: right.output.head :: Nil,
        streamedKeys = left.output,
        condition = condition(left, right))
    }, Seq(Row(null, 5)))
  }

  test("interval overlap probes both bounds") {
    val other = spark.createDataFrame(
      sparkContext.parallelize(Seq(
        Row(-2, -1),
        Row(1, 3),
        Row(5, 7)
      )), new StructType().add("low2", IntegerType).add("high2", IntegerType))
    val condition = (left: SparkPlan, right: SparkPlan) => And(
      LessThan(left.output(0), right.output(1)),
      LessThan(right.output(0), left.output(1)))
    checkAnswer2(intervals1, other, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, Inner,
        buildKeys = right.output,
        streamedKeys = left.output,
        condition = condition(left, right),
        rangeJoin = IntervalOverlapJoin)
    },
      Seq(
        (0, 2, 1, 3),
        (1, 5, 1, 3)
      ).map(Row.fromTuple))
  }

  test("condition and keys stay on the operator") {
    val leftPlan = intervals1.queryExecution.executedPlan
    val rightPlan = points.queryExecution.executedPlan
    val cond = pointCondition(
      rightPlan.output.head, leftPlan.output(0), leftPlan.output(1),
      lowInclusive = true, highInclusive = false)
    val exec = rangeJoinExec(
      leftPlan, rightPlan, BuildRight, Inner,
      buildKeys = rightPlan.output.head :: rightPlan.output.head :: Nil,
      streamedKeys = leftPlan.output,
      condition = cond)
    assert(exec.expressions.exists(_.semanticEquals(cond)))
    assert(exec.leftKeys == leftPlan.output)
    assert(!exec.verboseStringWithOperatorId().contains("none#"))
    assert(exec.verboseStringWithOperatorId().contains("low1"))
  }
}
