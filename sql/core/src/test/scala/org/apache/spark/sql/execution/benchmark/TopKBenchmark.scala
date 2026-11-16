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
import org.apache.spark.sql.internal.SQLConf.{WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED, WINDOW_GROUP_LIMIT_THRESHOLD}

/**
 * Benchmark to measure performance for top-k computation.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/TopKBenchmark-results.txt".
 * }}}
 */
object TopKBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Top-K Computation") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath + "/topk_benchmark_table"
        val N = 1024 * 1024 * 20
        spark.range(0, N, 1, 11)
          .selectExpr("id as a", "id % 1024 as b")
          .write.mode("overwrite")
          .parquet(path)

        def f(rankLikeFunc: String, partition: String): Unit = {
          spark.read.parquet(path)
            .selectExpr(s"$rankLikeFunc() OVER($partition ORDER BY a) AS rn", "a", "b")
            .where("rn > 100 and rn <= 200")
            .noop()
        }

        val benchmark = new Benchmark("Benchmark Top-K", N, minNumIters = 10, output = output)

        Seq("ROW_NUMBER", "RANK", "DENSE_RANK").foreach { function =>
          Seq("", "PARTITION BY b").foreach { partition =>
            benchmark.addCase(
              s"$function (PARTITION: $partition, WindowGroupLimit: false)") { _ =>
              withSQLConf(WINDOW_GROUP_LIMIT_THRESHOLD.key -> "-1") {
                f(function, partition)
              }
            }

            benchmark.addCase(
              s"$function (PARTITION: $partition, WindowGroupLimit: true)") { _ =>
              f(function, partition)
            }
          }
        }

        benchmark.run()
      }
    }

    // Measures the improvement from SPARK-61804's hash-based accumulator path for the dominant
    // real-world pattern -- ROW_NUMBER() OVER (PARTITION BY k ORDER BY o DESC) = 1, i.e. "keep the
    // latest/best row per key" -- against today's sort-based WindowGroupLimitExec. Both cases keep
    // the group-limit optimization itself enabled (WINDOW_GROUP_LIMIT_THRESHOLD at its default);
    // only WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED toggles, isolating this one change.
    runBenchmark("Hash-Based Top-1 ROW_NUMBER Window Group Limit") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath + "/top1_hash_benchmark_table"
        val N = 1024 * 1024 * 20

        def writeData(numKeys: Long): Unit = {
          spark.range(0, N, 1, 11)
            .selectExpr(s"id % $numKeys as k", "id as ts")
            .write.mode("overwrite")
            .parquet(path)
        }

        def dedupLatest(): Unit = {
          spark.read.parquet(path)
            .selectExpr("*", "row_number() over (partition by k order by ts desc) as rn")
            .where("rn = 1")
            .noop()
        }

        // "few large groups" is the skewed-group case the sort-based path pays for most: it must
        // fully sort every ~20K-row group just to keep row 1. "all-unique keys" is the opposite
        // end, where every group has exactly one row.
        Seq(1024L -> "few large groups", N.toLong -> "all-unique keys").foreach {
          case (numKeys, desc) =>
            writeData(numKeys)
            val benchmark = new Benchmark(
              s"Top-1 ROW_NUMBER dedup ($desc, $numKeys distinct keys)",
              N,
              minNumIters = 5,
              output = output)

            benchmark.addCase("hash pushdown: false (sort-based)") { _ =>
              withSQLConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
                dedupLatest()
              }
            }

            benchmark.addCase("hash pushdown: true (default)") { _ =>
              withSQLConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
                dedupLatest()
              }
            }

            benchmark.run()
        }
      }
    }
  }
}
