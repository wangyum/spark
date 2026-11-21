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
import org.apache.spark.internal.config.MAX_RESULT_SIZE
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField,
  StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark sort-merge join's per-equi-key-group range index against the plain linear
 * rescan of the buffered group it replaces (SPARK-24020).
 *
 * Every case joins on an equi key plus one residual range predicate -- point-in-range,
 * interval overlap, or a single partial inequality -- so the planner always picks
 * [[SortMergeJoinExec]]; the two benchmarked cases only flip
 * `spark.sql.join.sortMergeJoin.rangeIndex.threshold` between its default and `-1` (disabled).
 * Both sides are small enough that the
 * win or loss comes entirely from how each streamed row rescans its buffered group, not
 * from broadcast or shuffle cost.
 *
 * Two admission gates decide whether a group is indexed at all: the row threshold
 * (`spark.sql.join.sortMergeJoin.rangeIndex.threshold`, 16 by default) and the probe
 * gate (`RangeGroupProbe.MIN_PROBES_BEFORE_BUILD`, a fixed amortization constant of 128, set
 * above the measured point where a group's accumulated linear rescans approach the
 * index's one-time build cost). The group-size sweep holds reuse at 50 probes per
 * group -- below the probe gate -- so its rows measure that groups the policy never
 * admits pay no overhead over the plain linear scan. The reuse sweep holds a group at
 * the largest swept size (1024) and spans the probe gate: 1, 10, and 100 probes per
 * group stay unadmitted at parity, while 1000 and 10000 progressively amortize the
 * build cost over more post-threshold probes, climbing toward -- but not reaching --
 * the steady-state ratio a group this size tops out at. The large-group case pushes a
 * single group 16X bigger (16384 rows) and reused enough (5000) to sit solidly in that
 * steady-state regime, measuring how the win actually scales with group size (an O(log
 * groupSize) lookup against an O(groupSize) rescan) rather than how fast it amortizes.
 * The full-overlap case, whose ranges cover the entire probed domain so a probe matches
 * every row, and the remaining shapes (interval overlap, partial `<`, partial `>`)
 * and join types (left outer, left semi, left anti, right outer) are each measured
 * once at that same 50-probe representative shape -- further parity checks under the
 * default policy. The last two cases reproduce the motivating workload from the original
 * SPARK-24020 PR (apache/spark#21109), where each group is probed thousands of times, so
 * the default policy does admit it and the win shows; one runs with whole-stage codegen
 * on and the other with it off, since that PR's non-indexed implementation only sped
 * things up with codegen on.
 *
 * The `maxBytes` sweep answers a different question than every case above: not whether a
 * group should be indexed, but how large `spark.sql.join.sortMergeJoin.rangeIndex.maxBytes`
 * needs to be to actually admit realistic groups. `RangeGroupProbe.build()` trues its
 * admission decision up against `RangeRelation.estimatedSize`'s real, `SizeEstimator`-based
 * measurement, which recurses into every retained row regardless of whether `transform()`
 * copied it or (as of the `copyRows = false` optimization) merely referenced the row
 * `ExternalAppendOnlyUnsafeRowArray` already owns -- so `maxBytes` scales with the buffered
 * side's actual row *width*, not just its row count, independent of that optimization. Two
 * schemas at the large-group steady-state size/reuse above isolate that effect: "narrow"
 * (the same 3-long-column shape as every other case here) stays far under every maxBytes
 * candidate swept below, including the config's own 32MB default; "wide" adds one 500-byte
 * STRING column unrelated to the join condition, pushing the same group's real footprint
 * past some of those candidates, so the sweep shows where (if anywhere) the default actually
 * costs realistic, wide-schema workloads the win every narrow-row case above already gets.
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

  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, 1)
      .config(UI_ENABLED.key, true)
      .config(MAX_RESULT_SIZE.key, "3g")
      .getOrCreate()
  }

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
  // Spans the probe gate (`RangeGroupProbe.MIN_PROBES_BEFORE_BUILD`, 128) on both sides: below it
  // the deferred build keeps a group on the linear scan at parity with the feature
  // disabled, above it the index engages and amortizes. 10000 pushes well past the gate,
  // so the one-time build cost is amortized over enough probes to approach the steady-
  // state ratio: each probe trades an O(groupSize) rescan for an O(log groupSize) lookup.
  private val reuseCounts = Seq(1, 10, 100, 1000, 10000)
  // The representative group size used by the reuse sweep and every non-swept case below.
  private val repGroupSize = 1024

  private def cached(df: DataFrame): DataFrame = {
    val stored = df.cache()
    stored.count()
    stored
  }

  // Buffered side for the point-in-range shape: one equi key with `g` non-overlapping
  // [lo, hi] sub-ranges, so a selective probe hits exactly one of them. `extraCols` appends
  // columns unrelated to the join condition, for the maxBytes width sweeps below.
  private def pointBuffered(g: Int, extraCols: Seq[String] = Nil): DataFrame = cached(
    spark.range(numGroups.toLong * g).selectExpr(
      (Seq(
        s"id % $numGroups as s_k",
        s"(id div $numGroups) * $width as lo",
        s"(id div $numGroups) * $width + ${width - 1} as hi") ++ extraCols): _*))

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

  // A group 16X larger than repGroupSize, reused enough (5000, well past MIN_PROBES_BEFORE_BUILD)
  // to amortize the one-time build: the steady-state regime the reuse sweep above only
  // approaches. Sized to keep the "disabled" baseline (a full O(groupSize) rescan per
  // probe) within roughly a minute rather than letting the win grow without bound.
  private val largeGroupSize = 16384
  private val largeGroupReuse = 5000
  private lazy val largeGroupBuffered: DataFrame = pointBuffered(largeGroupSize)
  private lazy val largeGroupStreamed: DataFrame = pointStreamed(largeGroupSize, largeGroupReuse)

  // A buffered-side column unrelated to the join condition, wide enough that a single
  // largeGroupSize-row group's real footprint (per-row UnsafeRow content, not row count
  // alone) crosses the default maxBytes -- see `maxBytesSweepCases` below.
  private val wideColumnBytes = 500
  private def pointBufferedWide(g: Int): DataFrame =
    pointBuffered(g, Seq(s"repeat('x', $wideColumnBytes) as s_wide"))
  private lazy val largeGroupBufferedWide: DataFrame = pointBufferedWide(largeGroupSize)

  // A genuinely wide *schema* instead of one huge column: dozens to a hundred moderate-width
  // STRING columns, approximating a real fact table's buffered side (name/code/category-style
  // columns, not one giant blob). manyFieldsWidthBytes is a typical such column's length.
  private val manyFieldsWidthBytes = 20
  private val manyFieldsCounts = Seq(30, 50, 100)
  private def pointBufferedManyFields(g: Int, numFields: Int): DataFrame =
    pointBuffered(g, (0 until numFields).map(i => s"repeat('x', $manyFieldsWidthBytes) as f$i"))
  private lazy val largeGroupBufferedManyFields: Map[Int, DataFrame] =
    manyFieldsCounts.map(n => n -> pointBufferedManyFields(largeGroupSize, n)).toMap

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
    largeGroupBuffered
    largeGroupStreamed
    largeGroupBufferedWide
    largeGroupBufferedManyFields
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

  // The range index has no separate on/off switch: -1 disables it, and any non-negative
  // threshold (the config's own default when enabled) turns it on.
  private def rangeIndexThresholdConf(indexEnabled: Boolean): (String, String) =
    SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD.key -> (
      if (indexEnabled) SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD.defaultValueString else "-1")

  private def rangeIndexMaxBytesConf(maxBytes: String): (String, String) =
    SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_MAX_BYTES.key -> maxBytes

  // Shared by every case below: force sort-merge join and disable AQE so the measured plan
  // never drifts to a broadcast or shuffle-cost-dominated alternative.
  private val baseSortMergeJoinConf: Seq[(String, String)] = Seq(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false")

  private def runSortMergeJoin(df: => DataFrame, extraConf: (String, String)*): Unit = {
    withSQLConf(baseSortMergeJoinConf ++ extraConf: _*) {
      val planned = df
      val plan = planned.queryExecution.executedPlan
      assert(plan.exists(_.isInstanceOf[SortMergeJoinExec]), plan)
      planned.noop()
    }
  }

  private def run(df: => DataFrame, indexEnabled: Boolean, extraConf: (String, String)*): Unit =
    runSortMergeJoin(df, (rangeIndexThresholdConf(indexEnabled) +: extraConf): _*)

  // Builds a benchmark with a "disabled" baseline case plus one case per (label, extraConf)
  // variant -- the shape shared by every comparison below, whether it's a plain on/off
  // `compare`, a maxBytes sweep, or the original-PR wholestage-codegen split. `commonConf`
  // applies to the disabled baseline as well as every variant, for config (like wholestage
  // codegen) that must hold constant across a whole comparison, not just distinguish variants.
  private def variantCase(name: String, rows: Long, disabledLabel: String,
      variants: Seq[(String, Seq[(String, String)])], commonConf: Seq[(String, String)] = Nil)
      (df: => DataFrame): Unit = {
    if (!shouldRun(name)) return
    val benchmark = new Benchmark(name, rows, output = output)
    benchmark.addCase(disabledLabel) { _ =>
      run(df, indexEnabled = false, commonConf: _*)
    }
    variants.foreach { case (label, extraConf) =>
      benchmark.addCase(label) { _ =>
        run(df, indexEnabled = true, (commonConf ++ extraConf): _*)
      }
    }
    benchmark.run()
  }

  private def compare(name: String, rows: Long)(df: => DataFrame): Unit =
    variantCase(name, rows, "Range index disabled", Seq("Range index enabled" -> Nil))(df)

  private def originalPrCases(): Unit = {
    Seq(true, false).foreach { wholeStageEnabled =>
      val onOff = if (wholeStageEnabled) "on" else "off"
      val codegen = if (wholeStageEnabled) "with codegen" else "without codegen"
      val name = s"sort merge inner range join $codegen, (n=$originalPrN, m=$originalPrM)"
      variantCase(name, originalPrN.toLong * originalPrM,
          s"Range index disabled, wholestage $onOff",
          Seq(s"Range index enabled, wholestage $onOff" -> Nil),
          commonConf = Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStageEnabled.toString)) {
        originalPrDf1.join(originalPrDf2, originalPrCond)
      }
    }
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

  private def largeGroupSteadyStateCase(): Unit = {
    val n = s"probe=${numGroups * largeGroupReuse}, groups=$numGroups, groupSize=$largeGroupSize"
    compare(s"point-in-range inner, large group steady state ($n)",
        (numGroups * largeGroupReuse).toLong) {
      largeGroupStreamed.join(largeGroupBuffered, pointCond, "inner")
    }
  }

  // Candidate ceilings for spark.sql.join.sortMergeJoin.rangeIndex.maxBytes: 8m and 16m sit
  // below the config's own 32m default (to show a tighter cap would reject this group), 32m
  // is the default itself, and 64m shows headroom above it, bracketing where (if anywhere)
  // the default is the binding constraint.
  private val maxBytesCandidates = Seq("8m", "16m", "32m", "64m")
  // A real fact-table-style wide schema (dozens to a hundred moderate string columns) needs
  // a much larger ceiling than one single wide column of similar total width -- see
  // `logMaxBytesCalibration`'s per-schema byte estimates -- so this sweep starts where the
  // single-wide-column sweep above left off and climbs further.
  private val manyFieldsMaxBytesCandidates = Seq("16m", "32m", "64m", "128m", "256m")

  // scalastyle:off println
  private def logLine(msg: String): Unit = {
    println(msg)
    output.foreach(o => o.write((msg + "\n").getBytes))
  }
  // scalastyle:on println

  private def logMaxBytesCalibration(): Unit = {
    val narrowRowBytes = UnsafeProjection.create(Array[DataType](LongType, LongType, LongType))(
      InternalRow(0L, 0L, 0L)).getSizeInBytes
    val wideRowBytes = UnsafeProjection.create(
      Array[DataType](LongType, LongType, LongType, StringType))(
      InternalRow(0L, 0L, 0L, UTF8String.fromString("x" * wideColumnBytes))).getSizeInBytes
    def groupMb(rowBytes: Int): Double = rowBytes.toLong * largeGroupSize / 1024.0 / 1024.0
    logLine(s"[maxBytes calibration] narrow row (3 longs): $narrowRowBytes raw " +
      f"UnsafeRow bytes/row; a $largeGroupSize-row group's raw content alone is about " +
      f"${groupMb(narrowRowBytes)}%.1f MB.")
    logLine(s"[maxBytes calibration] wide row (3 longs + $wideColumnBytes-char " +
      s"string): $wideRowBytes raw UnsafeRow bytes/row; a $largeGroupSize-row group's raw " +
      f"content alone is about ${groupMb(wideRowBytes)}%.1f MB -- RangeRelation." +
      "estimatedSize (what maxBytes is actually checked against) adds JVM object/array " +
      "overhead and the index's own per-row bookkeeping on top of this.")
    manyFieldsCounts.foreach { n =>
      val types = Array[DataType](LongType, LongType, LongType) ++
        Array.fill[DataType](n)(StringType)
      val values = Seq(0L, 0L, 0L) ++
        Seq.fill(n)(UTF8String.fromString("x" * manyFieldsWidthBytes))
      val manyFieldsRowBytes =
        UnsafeProjection.create(types)(InternalRow.fromSeq(values)).getSizeInBytes
      logLine(s"[maxBytes calibration] wide schema ($n x $manyFieldsWidthBytes-char string " +
        s"columns): $manyFieldsRowBytes raw UnsafeRow bytes/row; a $largeGroupSize-row " +
        f"group's raw content alone is about ${groupMb(manyFieldsRowBytes)}%.1f MB.")
    }
  }

  private def maxBytesSweepCase(name: String, rows: Long, candidates: Seq[String])
      (df: => DataFrame): Unit =
    variantCase(name, rows, "Range index disabled", candidates.map(mb =>
      s"Range index enabled, maxBytes=$mb" -> Seq(rangeIndexMaxBytesConf(mb))))(df)

  private def maxBytesSweepCases(): Unit = {
    logMaxBytesCalibration()
    val n = s"probe=${numGroups * largeGroupReuse}, groups=$numGroups, groupSize=$largeGroupSize"
    maxBytesSweepCase(s"point-in-range inner, narrow row, maxBytes sweep ($n)",
        (numGroups * largeGroupReuse).toLong, maxBytesCandidates) {
      largeGroupStreamed.join(largeGroupBuffered, pointCond, "inner")
    }
    maxBytesSweepCase(s"point-in-range inner, wide row ($wideColumnBytes-char string), " +
        s"maxBytes sweep ($n)", (numGroups * largeGroupReuse).toLong, maxBytesCandidates) {
      largeGroupStreamed.join(largeGroupBufferedWide, pointCond, "inner")
    }
    manyFieldsCounts.foreach { numFields =>
      maxBytesSweepCase(s"point-in-range inner, wide schema ($numFields x " +
          s"$manyFieldsWidthBytes-char string columns), maxBytes sweep ($n)",
          (numGroups * largeGroupReuse).toLong, manyFieldsMaxBytesCandidates) {
        largeGroupStreamed.join(largeGroupBufferedManyFields(numFields), pointCond, "inner")
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
      largeGroupSteadyStateCase()
      maxBytesSweepCases()
      fullOverlapCase()
      otherShapeCases()
      joinTypeCases()
      originalPrCases()
    }
  }
}
