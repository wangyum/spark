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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark for the index-accelerated intra-group residual range filtering in
 * `SortMergeJoinExec`'s interpreted (whole-stage-codegen-off) Inner join path: when the
 * residual condition is a point-in-range or interval-overlap predicate and an equi-join group
 * is large, an `IntervalIndex` probe replaces the default linear scan of the buffered group.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *      "benchmarks/SortMergeJoinRangeResidualBenchmark-results.txt".
 * }}}
 */
object SortMergeJoinRangeResidualBenchmark extends SqlBasedBenchmark {

  /**
   * Runs `df` (an inner sort merge join, interpreted path) with the range-residual index
   * disabled and enabled, under one `Benchmark`. Asserts up front that the plan is actually an
   * interpreted `SortMergeJoinExec`, so a planner change can't silently turn this into a no-op
   * comparison between two identical plans.
   */
  private def compare(name: String, rows: Long)(df: => DataFrame): Unit = {
    withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FALLBACK.key -> "false") {
      assert(df.queryExecution.sparkPlan.exists(_.isInstanceOf[SortMergeJoinExec]))

      val benchmark = new Benchmark(name, rows, minNumIters = 3, output = output)
      benchmark.addCase("index disabled") { _ =>
        withSQLConf(SQLConf.SORT_MERGE_JOIN_EXEC_RANGE_RESIDUAL_ENABLED.key -> "false") {
          df.noop()
        }
      }
      benchmark.addCase("index enabled") { _ =>
        withSQLConf(SQLConf.SORT_MERGE_JOIN_EXEC_RANGE_RESIDUAL_ENABLED.key -> "true") {
          df.noop()
        }
      }
      benchmark.run()
    }
  }

  /**
   * A single equi-join key shared by every row on both sides, so the whole build side is one
   * buffered match group (well above the index's default 64-row threshold). Each probe point
   * overlaps only a couple of build intervals, so a linear scan of the group wastes almost all
   * of its work while the index narrows straight to the handful of real candidates.
   */
  private def pointInRangeSkewed(): Unit = {
    val probeRows = 10000
    val buildRows = 10000
    val probe = spark.range(probeRows)
      .selectExpr("cast(0 as int) as k", "cast(id % 2000 as int) as point")
      .cache()
    val build = spark.range(buildRows)
      .selectExpr("cast(0 as int) as k", "cast(id as int) as low", "cast(id + 1 as int) as high")
      .cache()
    probe.count()
    build.count()
    compare("point-in-range: single skewed key", probeRows.toLong * buildRows) {
      probe.join(build, probe("k") === build("k") &&
        probe("point") >= build("low") && probe("point") <= build("high"))
    }
  }

  /** Same skew shape as above, but with an interval-overlap residual instead of point-in-range. */
  private def intervalOverlapSkewed(): Unit = {
    val probeRows = 10000
    val buildRows = 10000
    val probe = spark.range(probeRows)
      .selectExpr(
        "cast(0 as int) as k",
        s"cast((id % 2000) * ${buildRows / 2000} as int) as lowA",
        s"cast((id % 2000) * ${buildRows / 2000} + 1 as int) as highA")
      .cache()
    val build = spark.range(buildRows)
      .selectExpr("cast(0 as int) as k", "cast(id as int) as lowB", "cast(id + 2 as int) as highB")
      .cache()
    probe.count()
    build.count()
    compare("interval-overlap: single skewed key", probeRows.toLong * buildRows) {
      probe.join(build, probe("k") === build("k") &&
        probe("lowA") < build("highB") && build("lowB") < probe("highA"))
    }
  }

  /**
   * No skew: every row has a distinct key, so every equi-join group has exactly one row, well
   * below the index threshold. The index is never built; this measures whether recognizing the
   * residual shape and checking the (never-triggered) threshold adds any regression.
   */
  private def pointInRangeUniform(): Unit = {
    val rows = 200000
    val probe = spark.range(rows)
      .selectExpr("id as k", "cast(id as int) as point")
      .cache()
    val build = spark.range(rows)
      .selectExpr("id as k", "cast(id as int) as low", "cast(id + 1 as int) as high")
      .cache()
    probe.count()
    build.count()
    compare("point-in-range: uniform (no skew) key", rows) {
      probe.join(build, probe("k") === build("k") &&
        probe("point") >= build("low") && probe("point") <= build("high"))
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("SortMergeJoinExec range-residual index") {
      pointInRangeSkewed()
      intervalOverlapSkewed()
      pointInRangeUniform()
    }
  }
}
