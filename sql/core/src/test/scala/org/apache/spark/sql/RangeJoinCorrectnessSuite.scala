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

package org.apache.spark.sql

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scala.collection.mutable

import org.apache.spark.sql.catalyst.optimizer.{
  BuildLeft, BuildRight, BuildSide, GreaterPartialRangeJoin, IntervalOverlapJoin,
  LessPartialRangeJoin, PointInRangeJoin, RangeJoin}
import org.apache.spark.sql.catalyst.plans.{
  Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.{
  BroadcastNestedLoopJoinExec, BroadcastRangeJoinExec}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Differential coverage: the broadcast range join must return exactly what the
 * broadcast nested loop join returns, for every shape, join type, build side,
 * key type, and both execution paths.
 *
 *  - Shapes: point-in-range (point on either side), interval overlap, and both
 *    partial-range directions.
 *  - Join types: inner, left outer, right outer, left semi, left anti, each with
 *    every build side `BroadcastRangeJoinExec.supports` allows.
 *  - Key types: every type `ExtractRangeJoinKeys.supportedType` accepts.
 *  - Execution: whole-stage codegen and the interpreted `doExecute` path.
 *
 * The data deliberately mixes null bounds, inverted intervals, degenerate
 * ranges, and duplicate rows so the index is probed at every layout.
 * Recognition is `ExtractRangeJoinKeysSuite`, the index is `RangeIndexSuite`,
 * the operator is `RangeJoinSuite`, and the planner is `RangeJoinSQLSuite`.
 */
class RangeJoinCorrectnessSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  // Sample levels read as integer codes; each type casts the same codes so a
  // failure points at the type's ordering, not at different data.
  private val sampleCodes: Seq[Int] = Seq(-5, -1, 0, 1, 2, 3, 5, 7)
  private val codes: Seq[Any] = sampleCodes.map(Int.box)

  private val pointPos: Seq[Seq[Int]] = Seq(0, 2, 4, 6, -1).map(Seq(_))
  private val aPos: Seq[Seq[Int]] = Seq(
    (1, 3), (3, 1), (0, 6), (6, 0), (-1, 4), (4, -1), (5, 5), (1, 3))
    .map(p => Seq(p._1, p._2))
  private val bPos: Seq[Seq[Int]] = Seq(
    (2, 5), (5, 2), (4, 7), (7, 4), (-1, 3), (6, -1), (0, 0))
    .map(p => Seq(p._1, p._2))

  // Zero, negative zero, the infinities, and NaN all reach the index.
  private val specialPointPos: Seq[Seq[Int]] = Seq(0, 3, 4, 6, 8, -1).map(Seq(_))
  private val specialAPos: Seq[Seq[Int]] = Seq(
    (3, 4), (4, 3), (0, 7), (7, 0), (2, 6), (6, 2), (-1, 5), (5, -1), (8, 8), (1, 1))
    .map(p => Seq(p._1, p._2))
  private val specialBPos: Seq[Seq[Int]] = Seq(
    (1, 5), (5, 1), (4, 8), (8, 4), (-1, 6), (6, -1), (7, 3), (3, 7), (0, 0), (2, 2))
    .map(p => Seq(p._1, p._2))

  private val supportedTypes: Seq[(String, DataType)] = Seq(
    "boolean" -> BooleanType,
    "byte" -> ByteType,
    "short" -> ShortType,
    "int" -> IntegerType,
    "long" -> LongType,
    "float" -> FloatType,
    "double" -> DoubleType,
    "decimal" -> DecimalType(10, 2),
    "string" -> StringType,
    "collated string" -> StringType("UNICODE_CI"),
    "binary" -> BinaryType,
    "date" -> DateType,
    "timestamp" -> TimestampType,
    "timestamp ntz" -> TimestampNTZType)

  private val joinTypes: Seq[(String, JoinType)] = Seq(
    "inner" -> Inner,
    "left" -> LeftOuter,
    "right" -> RightOuter,
    "leftsemi" -> LeftSemi,
    "leftanti" -> LeftAnti)

  private case class Shape(
      name: String,
      rangeJoin: RangeJoin,
      sides: (DataFrame, DataFrame, DataFrame) => (DataFrame, DataFrame, Column))

  private val shapes: Seq[Shape] = Seq(
    Shape("point/point-left", PointInRangeJoin,
      (p, a, _) => (p, a, p("p") >= a("aLo") && p("p") <= a("aHi"))),
    Shape("point/point-right", PointInRangeJoin,
      (p, a, _) => (a, p, p("p") >= a("aLo") && p("p") <= a("aHi"))),
    Shape("overlap", IntervalOverlapJoin,
      (p, a, b) => (a, b, a("aLo") < b("bHi") && b("bLo") < a("aHi"))),
    Shape("partial-less", LessPartialRangeJoin,
      (p, a, b) => (a, b, a("aLo") < b("bHi"))),
    Shape("partial-greater", GreaterPartialRangeJoin,
      (p, a, b) => (a, b, a("aLo") > b("bHi"))))

  /** Every build side the operator accepts for `joinType`. */
  private def buildSides(joinType: JoinType): Seq[BuildSide] = joinType match {
    case Inner => Seq(BuildLeft, BuildRight)
    case RightOuter => Seq(BuildLeft)
    case _ => Seq(BuildRight)
  }

  private def frame(
      names: Seq[String],
      positions: Seq[Seq[Int]],
      levels: Seq[Any],
      dataType: DataType): DataFrame = {
    def at(pos: Int): Any = if (pos < 0) null else levels(pos)
    val schema = names.foldLeft(new StructType())((s, n) => s.add(n, dataType))
    val rows = positions.map(p => Row.fromSeq(p.map(at)))
    spark.createDataFrame(sparkContext.parallelize(rows, 1), schema)
  }

  /** Cast the sample codes to the key type. Binary has no numeric cast. */
  private def keyColumns(df: DataFrame, dataType: DataType, names: Seq[String]): DataFrame =
    df.select(df.columns.toSeq.zip(names).map { case (c, n) =>
      val value = df(c)
      val cast = if (dataType == BinaryType) {
        value.cast(StringType).cast(BinaryType)
      } else {
        value.cast(dataType)
      }
      cast.as(n)
    }: _*)

  /**
   * Levels to store and the type to store them in. Date and the timestamps have
   * no INT cast, so they are written as parseable strings and cast to the key
   * type. Everything else stores the integer codes.
   */
  private def levelsFor(dataType: DataType): (Seq[Any], DataType) = dataType match {
    case DateType =>
      (sampleCodes.map(d => LocalDate.ofEpochDay(d.toLong).toString), StringType)
    case TimestampType | TimestampNTZType =>
      (sampleCodes.map { d =>
        val noon = LocalDateTime.ofEpochSecond(d.toLong * 86400L + 43200L, 0, ZoneOffset.UTC)
        noon.toString.replace('T', ' ')
      }, StringType)
    case _ => (codes, IntegerType)
  }

  private def castRelations(dataType: DataType): (DataFrame, DataFrame, DataFrame) = {
    val (levels, stored) = levelsFor(dataType)
    (
      keyColumns(frame(Seq("c"), pointPos, levels, stored), dataType, Seq("p")),
      keyColumns(frame(Seq("d1", "d2"), aPos, levels, stored), dataType, Seq("aLo", "aHi")),
      keyColumns(frame(Seq("d1", "d2"), bPos, levels, stored), dataType, Seq("bLo", "bHi")))
  }

  private def specialLevels(dataType: DataType): Seq[Any] = dataType match {
    case FloatType => Seq(
      Float.NegativeInfinity, -7.0f, -1.0f, -0.0f, 0.0f, 1.5f, 3.0f,
      Float.PositiveInfinity, Float.NaN)
    case _ => Seq(
      Double.NegativeInfinity, -7.0, -1.0, -0.0, 0.0, 1.5, 3.0,
      Double.PositiveInfinity, Double.NaN)
  }

  private def specialRelations(dataType: DataType): (DataFrame, DataFrame, DataFrame) = {
    val levels = specialLevels(dataType)
    (
      frame(Seq("p"), specialPointPos, levels, dataType),
      frame(Seq("aLo", "aHi"), specialAPos, levels, dataType),
      frame(Seq("bLo", "bHi"), specialBPos, levels, dataType))
  }

  private def rangeJoins(plan: SparkPlan): Seq[BroadcastRangeJoinExec] = collect(plan) {
    case j: BroadcastRangeJoinExec => j
  }

  private def nestedLoopJoins(plan: SparkPlan): Seq[BroadcastNestedLoopJoinExec] = collect(plan) {
    case j: BroadcastNestedLoopJoinExec => j
  }

  /**
   * Build the join. `buildLeft` is the broadcast hint: `Some(true)` hints the
   * left side, `Some(false)` the right side, `None` leaves the planner free.
   */
  private def buildJoin(
      shape: Shape,
      pts: DataFrame,
      a: DataFrame,
      b: DataFrame,
      joinType: String,
      buildLeft: Option[Boolean]): DataFrame = {
    val (left, right, condition) = shape.sides(pts, a, b)
    val hintedLeft = if (buildLeft.contains(true)) broadcast(left) else left
    val hintedRight = if (buildLeft.contains(false)) broadcast(right) else right
    hintedLeft.join(hintedRight, condition, joinType)
  }

  /** The same join planned as a broadcast nested loop join. */
  private def reference(
      shape: Shape,
      pts: DataFrame,
      a: DataFrame,
      b: DataFrame,
      joinType: String,
      buildLeft: Option[Boolean]): Seq[Row] = {
    withSQLConf(SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> "false") {
      val df = buildJoin(shape, pts, a, b, joinType, buildLeft)
      val plan = df.queryExecution.executedPlan
      assert(rangeJoins(plan).isEmpty, s"reference planned a range join\n$plan")
      assert(nestedLoopJoins(plan).size == 1, s"reference is not a nested loop join\n$plan")
      df.collect().toSeq
    }
  }

  private def checkOne(
      label: String,
      shape: Shape,
      pts: DataFrame,
      a: DataFrame,
      b: DataFrame,
      joinType: String,
      join: JoinType,
      buildLeft: Option[Boolean],
      codegen: Boolean,
      expected: Seq[Row]): Unit = {
    withSQLConf(
        SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
      val df = buildJoin(shape, pts, a, b, joinType, buildLeft)
      val plan = df.queryExecution.executedPlan
      val planned = rangeJoins(plan)
      assert(planned.size == 1, s"$label: expected one range join, got ${planned.size}\n$plan")
      assert(planned.head.rangeJoin == shape.rangeJoin, s"$label: ${planned.head.rangeJoin}\n$plan")
      buildLeft.foreach { left =>
        val expectedSide = if (left) BuildLeft else BuildRight
        assert(planned.head.buildSide == expectedSide, s"$label: ${planned.head.buildSide}\n$plan")
      }
      assert(
        planned.head.joinType == join,
        s"$label: ${planned.head.joinType} != $join\n$plan")
      withClue(label) {
        checkAnswer(df, expected)
      }
    }
  }

  /**
   * Run every shape x join type x allowed build side x execution path against
   * the nested loop join. A broadcast hint names the build side: these rows are
   * `ExistingRDD` scans with no statistics, so the unhinted planner cannot pick
   * a broadcast side. That path is `RangeJoinSQLSuite`, on views that have one.
   */
  private def checkMatrix(
      label: String,
      rel: (DataFrame, DataFrame, DataFrame),
      requireNonEmpty: Boolean = true): Unit = {
    val (pts, a, b) = rel
    val expected = mutable.Map.empty[(String, String, Option[Boolean]), Seq[Row]]
    var nonEmpty = 0

    for (shape <- shapes; (joinType, join) <- joinTypes) {
      buildSides(join).map(side => Some(side == BuildLeft)).foreach { buildLeft =>
        val key = (shape.name, joinType, buildLeft)
        val rows = expected.getOrElseUpdate(
          key, reference(shape, pts, a, b, joinType, buildLeft))
        // Two values cannot fill every shape, so only guard against a matrix
        // that compares nothing at all.
        if (requireNonEmpty && join == Inner && rows.nonEmpty) nonEmpty += 1
        Seq(true, false).foreach { codegen =>
          val side = if (buildLeft.contains(true)) "build left" else "build right"
          val name = s"$label / ${shape.name} / $joinType / $side / codegen=$codegen"
          checkOne(name, shape, pts, a, b, joinType, join, buildLeft, codegen, rows)
        }
      }
    }
    if (requireNonEmpty) {
      assert(nonEmpty > 0, s"$label: every inner reference was empty")
    }
  }

  supportedTypes.foreach { case (typeName, dataType) =>
    test(s"every shape and join type matches nested loop for $typeName") {
      checkMatrix(typeName, castRelations(dataType))
    }
  }

  test("zero, negative zero, and NaN match nested loop on float and double") {
    Seq("float" -> FloatType, "double" -> DoubleType).foreach { case (typeName, dataType) =>
      checkMatrix(typeName, specialRelations(dataType))
    }
  }

  test("empty relations match nested loop") {
    val (pts, a, b) = castRelations(IntegerType)
    checkMatrix("empty stream", (pts.where(pts("p") === 999), a, b), requireNonEmpty = false)
    checkMatrix("empty build", (pts, a.where(a("aLo") === 999), b), requireNonEmpty = false)
  }

  test("an unsupported key type stays a nested loop join") {
    val arrays = spark.createDataFrame(
      sparkContext.parallelize(
        Seq(Row(Seq(1, 2)), Row(Seq(3)), Row(null), Row(Seq(0, 9))), 1),
      new StructType().add("arr", ArrayType(IntegerType)))
    val other = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(Seq(2, 4)), Row(Seq(1))), 1),
      new StructType().add("arr2", ArrayType(IntegerType)))
    Seq("inner", "left", "leftsemi", "leftanti").foreach { joinType =>
      withSQLConf(SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> "true") {
        val df = arrays.join(broadcast(other), arrays("arr") < other("arr2"), joinType)
        val plan = df.queryExecution.executedPlan
        assert(rangeJoins(plan).isEmpty, s"$joinType planned a range join\n$plan")
        assert(nestedLoopJoins(plan).size == 1, s"$joinType is not a nested loop join\n$plan")
        withClue(joinType) {
          checkAnswer(
            df,
            withSQLConf(SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> "false") {
              arrays.join(
                broadcast(other), arrays("arr") < other("arr2"), joinType).collect().toSeq
            })
        }
      }
    }
  }
}
