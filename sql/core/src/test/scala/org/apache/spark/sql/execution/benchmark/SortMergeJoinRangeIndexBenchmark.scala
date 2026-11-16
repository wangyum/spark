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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Benchmark sort-merge join's per-equi-key-group range index against the plain linear
 * rescan of the buffered group it replaces (SPARK-24020).
 *
 * Every case joins on an equi key plus one residual range predicate -- point-in-range,
 * interval overlap, or a single partial inequality -- so the planner always picks
 * [[SortMergeJoinExec]]; the two benchmarked cases only flip
 * `spark.sql.join.sortMergeJoin.rangeIndex.enabled`. Both sides are small enough that the
 * win or loss comes entirely from how each streamed row rescans its buffered group, not
 * from broadcast or shuffle cost.
 *
 * The group-size sweep is the main axis: the index is skipped below
 * `spark.sql.join.sortMergeJoin.rangeIndex.threshold` (16 by default), so a group of 8 is a
 * parity check, and larger groups (64 through 4096) show the win growing as the linear
 * rescan it replaces gets more expensive. The reuse sweep holds a group at the largest swept
 * size (1024) and varies how many streamed rows share that one key, since the index is built
 * once per group and amortizes across every streamed row that reuses it. The full-overlap
 * case builds a group whose ranges cover the entire probed domain, so every pair matches and
 * the index cannot prune anything -- the worst case for the index's one-time build cost. The
 * remaining shapes (interval overlap, partial `<`, partial `>`) and join types (left outer,
 * left semi, left anti, right outer) are each measured once at that same representative size.
 * The last case reproduces the motivating workload from the original SPARK-24020 PR
 * (apache/spark#21109) across both the range-index flag and whole-stage codegen, since that
 * PR's non-indexed implementation only sped things up with codegen on.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt \
 *   "sql/Test/runMain org.apache.spark.sql.execution.benchmark.SortMergeJoinRangeIndexBenchmark"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *      "sql/Test/runMain org.apache.spark.sql.execution.benchmark.SortMergeJoinRangeIndexBenchmark"
 *      Results will be written to "benchmarks/SortMergeJoinRangeIndexBenchmark-results.txt".
 * }}}
 */
object SortMergeJoinRangeIndexBenchmark extends SqlBasedBenchmark {

  // An optional regex in the environment variable SMJ_RANGE_BENCH_FILTER runs only the
  // sections whose name matches, e.g. SMJ_RANGE_BENCH_FILTER=".*reuse.*|.*full overlap.*".
  // A workaround for sbt splitting runMain arguments on spaces.
  private val nameFilter: Option[String] = Option(System.getenv("SMJ_RANGE_BENCH_FILTER"))

  private def shouldRun(name: String): Boolean = nameFilter.forall(name.matches)

  private val width = 10
  private val numGroups = 200
  // Streamed rows per equi key for every case except the dedicated reuse sweep.
  private val baseReuse = 50
  // Straddles the default threshold (16) on both sides.
  private val groupSizes = Seq(8, 16, 64, 256, 1024, 4096)
  private val reuseCounts = Seq(1, 10, 100)
  // The representative group size used by the reuse sweep and every non-swept case below.
  private val repGroupSize = 1024

  private def cached(df: DataFrame): DataFrame = {
    val stored = df.cache()
    stored.count()
    stored
  }

  // Buffered side for the point-in-range shape: one equi key with `g` non-overlapping
  // [lo, hi] sub-ranges, so a selective probe hits exactly one of them.
  private def pointBuffered(g: Int): DataFrame = cached(
    spark.range(numGroups.toLong * g).selectExpr(
      s"id % $numGroups as s_k",
      s"(id div $numGroups) * $width as lo",
      s"(id div $numGroups) * $width + ${width - 1} as hi"))

  // Streamed side for the point-in-range shape: `r` probes per equi key, cycling through
  // the `g` sub-ranges of the matching buffered group so every probe hits exactly one.
  private def pointStreamed(g: Int, r: Int): DataFrame =
    cached(spark.range(numGroups.toLong * r).selectExpr(
      s"id % $numGroups as e_k",
      s"((id div $numGroups) % $g) * $width + ${width / 2} as e_p"))

  private lazy val pointBufferedByGroupSize: Map[Int, DataFrame] =
    groupSizes.map(g => g -> pointBuffered(g)).toMap
  private lazy val pointStreamedByGroupSize: Map[Int, DataFrame] =
    groupSizes.map(g => g -> pointStreamed(g, baseReuse)).toMap
  private lazy val pointStreamedByReuse: Map[Int, DataFrame] =
    reuseCounts.map(r => r -> pointStreamed(repGroupSize, r)).toMap

  // Above every sub-range of the representative group, so a point-in-range probe matches
  // nothing in its equi-key group.
  private lazy val pointMisses: DataFrame =
    cached(spark.range(numGroups.toLong * baseReuse).selectExpr(
      s"id % $numGroups as e_k",
      s"${repGroupSize * width} as e_p"))

  private def pointCond: org.apache.spark.sql.Column =
    col("e_k") === col("s_k") && col("e_p") >= col("lo") && col("e_p") <= col("hi")

  // Interval-overlap shape at the representative group size: short, staggered spans on
  // both sides, each probe overlapping only the neighboring buffered spans in its group.
  private lazy val overlapBuffered: DataFrame =
    cached(spark.range(numGroups.toLong * repGroupSize).selectExpr(
      s"id % $numGroups as s_k",
      s"(id div $numGroups) * $width as blo",
      s"(id div $numGroups) * $width + ${width + width / 2} as bhi"))
  private lazy val overlapStreamed: DataFrame =
    cached(spark.range(numGroups.toLong * baseReuse).selectExpr(
      s"id % $numGroups as e_k",
      s"((id div $numGroups) % $repGroupSize) * $width as alo",
      s"((id div $numGroups) % $repGroupSize) * $width + ${width + width / 2} as ahi"))

  private def overlapCond: org.apache.spark.sql.Column =
    col("e_k") === col("s_k") && col("alo") <= col("bhi") && col("blo") <= col("ahi")

  // Partial-range shape at the representative group size: the buffered side holds one
  // bound per row (0 until repGroupSize), the streamed side probes at the median bound
  // so each inequality matches about half of its equi-key group.
  private lazy val partialBuffered: DataFrame =
    cached(spark.range(numGroups.toLong * repGroupSize).selectExpr(
      s"id % $numGroups as s_k",
      s"(id div $numGroups) as s_b"))
  private lazy val partialStreamed: DataFrame =
    cached(spark.range(numGroups.toLong * baseReuse).selectExpr(
      s"id % $numGroups as e_k",
      s"${repGroupSize / 2} as e_a"))

  private def lessCond: org.apache.spark.sql.Column =
    col("e_k") === col("s_k") && col("e_a") < col("s_b")
  private def greaterCond: org.apache.spark.sql.Column =
    col("e_k") === col("s_k") && col("e_a") > col("s_b")

  // Every buffered range in a (smaller) group covers the whole probed domain, so every
  // streamed row matches every buffered row in its group and the index prunes nothing.
  private val fullGroups = 50
  private val fullGroupSize = 100
  private val fullReuse = 40
  private lazy val fullBuffered: DataFrame =
    cached(spark.range(fullGroups.toLong * fullGroupSize).selectExpr(
      s"id % $fullGroups as s_k",
      "cast(0 as bigint) as lo",
      s"${fullGroupSize * width} as hi"))
  private lazy val fullStreamed: DataFrame =
    cached(spark.range(fullGroups.toLong * fullReuse).selectExpr(
      s"id % $fullGroups as e_k",
      s"(id div $fullGroups) % ${fullGroupSize * width} as e_p"))

  // Reproduces the motivating workload from the original SPARK-24020 PR (apache/spark#21109):
  // two cartesian products of 1..m by 1..n joined on the first column plus a +/-3 band on the
  // second, so every equi-key group holds all n rows of one table and a selective band probes
  // each of the n rows of the other.
  private val originalPrN = 2 << 11
  private val originalPrM = 100
  private def cartesianDf(m: Int, n: Int, aName: String, bName: String): DataFrame = {
    val rdd = spark.sparkContext.parallelize(1 to m)
      .cartesian(spark.sparkContext.parallelize(1 to n))
      .map { case (a, b) => Row(a, b) }
    val schema = StructType(Seq(
      StructField(aName, IntegerType),
      StructField(bName, IntegerType)))
    spark.createDataFrame(rdd, schema)
  }
  private lazy val originalPrDf1: DataFrame =
    cached(cartesianDf(originalPrM, originalPrN, "col1a", "col1b"))
  private lazy val originalPrDf2: DataFrame =
    cached(cartesianDf(originalPrM, originalPrN, "col2a", "col2b"))

  private def originalPrCond: org.apache.spark.sql.Column =
    col("col1a") === col("col2a") &&
      col("col1b") < col("col2b") + lit(3) &&
      col("col1b") > col("col2b") - lit(3)

  private def prepare(): Unit = {
    pointBufferedByGroupSize
    pointStreamedByGroupSize
    pointStreamedByReuse
    pointMisses
    overlapBuffered
    overlapStreamed
    partialBuffered
    partialStreamed
    fullBuffered
    fullStreamed
    originalPrDf1
    originalPrDf2
  }

  private def run(df: => DataFrame, indexEnabled: Boolean): Unit = {
    withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> indexEnabled.toString) {
      val planned = df
      val plan = planned.queryExecution.executedPlan
      assert(plan.exists(_.isInstanceOf[SortMergeJoinExec]), plan)
      planned.noop()
    }
  }

  private def runOriginalPr(indexEnabled: Boolean, wholeStageEnabled: Boolean): Unit = {
    withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStageEnabled.toString,
        SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> indexEnabled.toString) {
      val joined = originalPrDf1.join(originalPrDf2, originalPrCond)
      val plan = joined.queryExecution.executedPlan
      assert(plan.exists(_.isInstanceOf[SortMergeJoinExec]), plan)
      joined.noop()
    }
  }

  private def originalPrCase(): Unit = {
    val name = s"sort merge inner range join, PR-21109 shape (n=$originalPrN, m=$originalPrM)"
    if (!shouldRun(name)) return
    val rows = originalPrN.toLong * originalPrM
    val benchmark = new Benchmark(name, rows, output = output)
    benchmark.addCase("Range index disabled, wholestage off") { _ =>
      runOriginalPr(indexEnabled = false, wholeStageEnabled = false)
    }
    benchmark.addCase("Range index disabled, wholestage on") { _ =>
      runOriginalPr(indexEnabled = false, wholeStageEnabled = true)
    }
    benchmark.addCase("Range index enabled, wholestage off") { _ =>
      runOriginalPr(indexEnabled = true, wholeStageEnabled = false)
    }
    benchmark.addCase("Range index enabled, wholestage on") { _ =>
      runOriginalPr(indexEnabled = true, wholeStageEnabled = true)
    }
    benchmark.run()
  }

  private def compare(name: String, rows: Long)(df: => DataFrame): Unit = {
    if (!shouldRun(name)) return
    val benchmark = new Benchmark(name, rows, output = output)
    benchmark.addCase("Range index disabled") { _ =>
      run(df, indexEnabled = false)
    }
    benchmark.addCase("Range index enabled") { _ =>
      run(df, indexEnabled = true)
    }
    benchmark.run()
  }

  private def groupSizeSweepCases(): Unit = {
    groupSizes.foreach { g =>
      val n = s"probe=${numGroups * baseReuse}, groups=$numGroups, groupSize=$g"
      compare(s"point-in-range inner, group size sweep ($n)", (numGroups * baseReuse).toLong) {
        pointStreamedByGroupSize(g).join(pointBufferedByGroupSize(g), pointCond, "inner")
      }
    }
  }

  private def reuseSweepCases(): Unit = {
    reuseCounts.foreach { r =>
      val n = s"probe=${numGroups * r}, groups=$numGroups, groupSize=$repGroupSize"
      compare(s"point-in-range inner, reuse sweep ($n)", (numGroups * r).toLong) {
        pointStreamedByReuse(r).join(pointBufferedByGroupSize(repGroupSize), pointCond, "inner")
      }
    }
  }

  private def fullOverlapCase(): Unit = {
    val n = s"probe=${fullGroups * fullReuse}, groups=$fullGroups, groupSize=$fullGroupSize"
    val rows = fullGroups.toLong * fullReuse * fullGroupSize
    compare(s"point-in-range inner, full overlap ($n)", rows) {
      fullStreamed.join(fullBuffered, pointCond, "inner")
    }
  }

  private def otherShapeCases(): Unit = {
    val n = s"probe=${numGroups * baseReuse}, groups=$numGroups, groupSize=$repGroupSize"
    compare(s"interval overlap inner ($n)", (numGroups * baseReuse).toLong) {
      overlapStreamed.join(overlapBuffered, overlapCond, "inner")
    }
    compare(s"partial < inner ($n)", (numGroups * baseReuse).toLong) {
      partialStreamed.join(partialBuffered, lessCond, "inner")
    }
    compare(s"partial > inner ($n)", (numGroups * baseReuse).toLong) {
      partialStreamed.join(partialBuffered, greaterCond, "inner")
    }
  }

  private def joinTypeCases(): Unit = {
    val n = s"probe=${numGroups * baseReuse}, groups=$numGroups, groupSize=$repGroupSize"
    val buffered = pointBufferedByGroupSize(repGroupSize)
    val streamed = pointStreamedByGroupSize(repGroupSize)
    compare(s"point-in-range left outer ($n)", (numGroups * baseReuse).toLong) {
      streamed.join(buffered, pointCond, "left")
    }
    compare(s"point-in-range left semi ($n)", (numGroups * baseReuse).toLong) {
      streamed.join(buffered, pointCond, "leftsemi")
    }
    compare(s"point-in-range left anti ($n)", (numGroups * baseReuse).toLong) {
      pointMisses.join(buffered, pointCond, "leftanti")
    }
    compare(s"point-in-range right outer ($n)", (numGroups * baseReuse).toLong) {
      buffered.join(streamed, pointCond, "right")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    if (nameFilter.isEmpty) prepare()
    runBenchmark("Sort Merge Join Range Index Benchmark") {
      groupSizeSweepCases()
      reuseSweepCases()
      fullOverlapCase()
      otherShapeCases()
      joinTypeCases()
      originalPrCase()
    }
  }
}
