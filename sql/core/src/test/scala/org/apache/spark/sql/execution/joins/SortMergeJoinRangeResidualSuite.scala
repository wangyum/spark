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
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, PointInRangeJoin}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

/**
 * Tests for [[SortMergeJoinRangeResidual]] (shape detection) and for the index-accelerated
 * residual filtering it enables in [[SortMergeJoinEvaluatorFactory]]'s interpreted Inner join
 * path. Operator-level broadcast range join behavior is covered by `RangeJoinSuite`; the index
 * itself by `RangeIndexSuite`.
 */
class SortMergeJoinRangeResidualSuite extends QueryTest with SharedSparkSession {

  // ---------------------------------------------------------------------------------------
  // SortMergeJoinRangeResidual.extract: shape detection
  // ---------------------------------------------------------------------------------------

  private val point = AttributeReference("point", IntegerType)()
  private val low = AttributeReference("low", IntegerType)()
  private val high = AttributeReference("high", IntegerType)()
  private val lowA = AttributeReference("lowA", IntegerType)()
  private val highA = AttributeReference("highA", IntegerType)()
  private val lowB = AttributeReference("lowB", IntegerType)()
  private val highB = AttributeReference("highB", IntegerType)()

  private def pointInRangeCondition(
      point: Expression, low: Expression, high: Expression): Expression =
    And(GreaterThanOrEqual(point, low), LessThanOrEqual(point, high))

  private def overlapCondition(
      lowA: Expression, highA: Expression, lowB: Expression, highB: Expression): Expression =
    And(LessThan(lowA, highB), LessThan(lowB, highA))

  test("extract recognizes point-in-range with the point on the streamed side") {
    val condition = pointInRangeCondition(point, low, high)
    val result = SortMergeJoinRangeResidual.extract(condition, point :: Nil, low :: high :: Nil)
    assert(result.contains(RangeResidualKeys(low :: high :: Nil, point :: Nil, PointInRangeJoin)))
  }

  test("extract recognizes point-in-range with the point on the buffered side") {
    val condition = pointInRangeCondition(point, low, high)
    val result = SortMergeJoinRangeResidual.extract(condition, low :: high :: Nil, point :: Nil)
    assert(result.contains(RangeResidualKeys(point :: Nil, low :: high :: Nil, PointInRangeJoin)))
  }

  test("extract recognizes interval overlap in either build orientation") {
    val condition = overlapCondition(lowA, highA, lowB, highB)
    val streamedFirst =
      SortMergeJoinRangeResidual.extract(condition, lowA :: highA :: Nil, lowB :: highB :: Nil)
    assert(streamedFirst.contains(
      RangeResidualKeys(lowB :: highB :: Nil, lowA :: highA :: Nil, IntervalOverlapJoin)))

    val bufferedFirst =
      SortMergeJoinRangeResidual.extract(condition, lowB :: highB :: Nil, lowA :: highA :: Nil)
    assert(bufferedFirst.contains(
      RangeResidualKeys(lowA :: highA :: Nil, lowB :: highB :: Nil, IntervalOverlapJoin)))
  }

  test("extract rejects a condition with no range predicate") {
    val condition = EqualTo(point, low)
    assert(SortMergeJoinRangeResidual.extract(condition, point :: Nil, low :: Nil).isEmpty)
  }

  test("extract rejects a single inequality") {
    val condition = LessThan(point, low)
    assert(SortMergeJoinRangeResidual.extract(condition, point :: Nil, low :: Nil).isEmpty)
  }

  test("extract rejects a point-in-range pair with mismatched predicate types") {
    val mismatched = AttributeReference("point2", LongType)()
    val condition = pointInRangeCondition(mismatched, low, high)
    assert(SortMergeJoinRangeResidual.extract(
      condition, mismatched :: Nil, low :: high :: Nil).isEmpty)
  }

  // ---------------------------------------------------------------------------------------
  // End-to-end: SortMergeJoinExec inner join, interpreted path, enabled vs. disabled
  // ---------------------------------------------------------------------------------------

  private val EnsureRequirements = new EnsureRequirements()

  private def makeSortMergeJoin(
      leftKey: Expression,
      rightKey: Expression,
      boundCondition: Expression,
      leftPlan: SparkPlan,
      rightPlan: SparkPlan): SparkPlan = {
    val smj = SortMergeJoinExec(
      leftKey :: Nil, rightKey :: Nil, Inner, Some(boundCondition), leftPlan, rightPlan)
    EnsureRequirements.apply(smj)
  }

  /**
   * Executes `plan` with the index disabled to establish ground truth (byte-for-byte the
   * pre-existing linear-scan behavior), then asserts the index-enabled run produces the same
   * rows. Forces whole-stage codegen off throughout, since the index only applies to the
   * interpreted path.
   */
  private def checkRangeResidual(
      left: DataFrame,
      right: DataFrame,
      plan: (SparkPlan, SparkPlan) => SparkPlan,
      minMatchSize: Int = 4): Unit = {
    withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FALLBACK.key -> "false",
        SQLConf.SORT_MERGE_JOIN_EXEC_RANGE_RESIDUAL_MIN_MATCH_SIZE.key -> minMatchSize.toString) {
      val expected = withSQLConf(
          SQLConf.SORT_MERGE_JOIN_EXEC_RANGE_RESIDUAL_ENABLED.key -> "false") {
        QueryTest.executePlan(
          plan(left.queryExecution.sparkPlan, right.queryExecution.sparkPlan), spark.sqlContext)
      }
      withSQLConf(SQLConf.SORT_MERGE_JOIN_EXEC_RANGE_RESIDUAL_ENABLED.key -> "true") {
        checkAnswer2(left, right, plan, expected)
      }
    }
  }

  private def pointInRangeFrames(counts: Seq[Int]): (DataFrame, DataFrame) = {
    val probeRows = counts.zipWithIndex.flatMap { case (c, k) => (0 until c).map(i => Row(k, i)) }
    val buildRows =
      counts.zipWithIndex.flatMap { case (c, k) => (0 until c).map(i => Row(k, i, i + 1)) }
    val probe = spark.createDataFrame(
      sparkContext.parallelize(probeRows),
      new StructType().add("k", IntegerType).add("point", IntegerType))
    val build = spark.createDataFrame(
      sparkContext.parallelize(buildRows),
      new StructType().add("k", IntegerType).add("low", IntegerType).add("high", IntegerType))
    (probe, build)
  }

  test("point-in-range: skewed key with a large buffered group") {
    val (probe, build) = pointInRangeFrames(Seq(200))
    checkRangeResidual(probe, build, (l, r) =>
      makeSortMergeJoin(
        l.output(0), r.output(0), pointInRangeCondition(l.output(1), r.output(1), r.output(2)),
        l, r))
  }

  test("interval-overlap: skewed key with a large buffered group") {
    val n = 150
    val probe = spark.createDataFrame(
      sparkContext.parallelize((0 until n).map(i => Row(0, i, i + 2))),
      new StructType().add("k", IntegerType).add("lowA", IntegerType).add("highA", IntegerType))
    val build = spark.createDataFrame(
      sparkContext.parallelize((0 until n).map(i => Row(0, i, i + 3))),
      new StructType().add("k", IntegerType).add("lowB", IntegerType).add("highB", IntegerType))
    checkRangeResidual(probe, build, (l, r) =>
      makeSortMergeJoin(
        l.output(0), r.output(0),
        overlapCondition(l.output(1), l.output(2), r.output(1), r.output(2)), l, r))
  }

  test("group sizes straddling the index threshold") {
    // Below, at, and above minMatchSize = 10, all probed in the same run so a
    // threshold-crossing group boundary also exercises the match-generation reset.
    val (probe, build) = pointInRangeFrames(Seq(5, 10, 20))
    checkRangeResidual(probe, build, (l, r) =>
      makeSortMergeJoin(
        l.output(0), r.output(0), pointInRangeCondition(l.output(1), r.output(1), r.output(2)),
        l, r),
      minMatchSize = 10)
  }

  test("nulls in range columns are not matches") {
    val n = 40
    val probeRows = (0 until n).map { i =>
      val p: Any = if (i % 4 == 0) null else i
      Row(0, p)
    }
    val buildRows = (0 until n).map { i =>
      val lo: Any = if (i % 6 == 0) null else i
      Row(0, lo, i + 1)
    }
    val probe = spark.createDataFrame(
      sparkContext.parallelize(probeRows),
      new StructType().add("k", IntegerType).add("point", IntegerType))
    val build = spark.createDataFrame(
      sparkContext.parallelize(buildRows),
      new StructType().add("k", IntegerType).add("low", IntegerType).add("high", IntegerType))
    checkRangeResidual(probe, build, (l, r) =>
      makeSortMergeJoin(
        l.output(0), r.output(0), pointInRangeCondition(l.output(1), r.output(1), r.output(2)),
        l, r))
  }

  test("an extra non-range conjunct narrows the match further") {
    val n = 60
    val probeRows = (0 until n).map(i => Row(0, i, i % 2))
    val buildRows = (0 until n).map(i => Row(0, i, i + 2, (i / 10) % 2))
    val probe = spark.createDataFrame(
      sparkContext.parallelize(probeRows),
      new StructType().add("k", IntegerType).add("point", IntegerType).add("tag", IntegerType))
    val build = spark.createDataFrame(
      sparkContext.parallelize(buildRows),
      new StructType().add("k", IntegerType).add("low", IntegerType).add("high", IntegerType)
        .add("tag", IntegerType))
    checkRangeResidual(probe, build, (l, r) => {
      val range = pointInRangeCondition(l.output(1), r.output(1), r.output(2))
      val extraTag = EqualTo(l.output(2), r.output(3))
      makeSortMergeJoin(l.output(0), r.output(0), And(range, extraTag), l, r)
    })
  }

  test("an empty buffered match group produces no rows") {
    val probe = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(0, 5), Row(1, 10))),
      new StructType().add("k", IntegerType).add("point", IntegerType))
    val build = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(2, 0, 100))),
      new StructType().add("k", IntegerType).add("low", IntegerType).add("high", IntegerType))
    checkRangeResidual(probe, build, (l, r) =>
      makeSortMergeJoin(
        l.output(0), r.output(0), pointInRangeCondition(l.output(1), r.output(1), r.output(2)),
        l, r))
  }

  test("large skewed group: full cross-match count matches the combinatorial expectation") {
    val n = 300
    val probe = spark.createDataFrame(
      sparkContext.parallelize((0 until n).map(i => Row(0, i))),
      new StructType().add("k", IntegerType).add("point", IntegerType))
    val build = spark.createDataFrame(
      sparkContext.parallelize((0 until n).map(_ => Row(0, 0, n))),
      new StructType().add("k", IntegerType).add("low", IntegerType).add("high", IntegerType))
    withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FALLBACK.key -> "false",
        SQLConf.SORT_MERGE_JOIN_EXEC_RANGE_RESIDUAL_MIN_MATCH_SIZE.key -> "4",
        SQLConf.SORT_MERGE_JOIN_EXEC_RANGE_RESIDUAL_ENABLED.key -> "true") {
      val leftPlan = probe.queryExecution.sparkPlan
      val rightPlan = build.queryExecution.sparkPlan
      val exec = makeSortMergeJoin(
        leftPlan.output(0), rightPlan.output(0),
        pointInRangeCondition(leftPlan.output(1), rightPlan.output(1), rightPlan.output(2)),
        leftPlan, rightPlan)
      val rows = QueryTest.executePlan(exec, spark.sqlContext)
      assert(rows.length == n.toLong * n.toLong)
    }
  }
}
