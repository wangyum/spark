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
import org.apache.spark.sql.internal.SQLConf.{WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED,
  WINDOW_GROUP_LIMIT_THRESHOLD}

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

        // ts = xxhash64(id), not id itself: id increases monotonically with scan order, so every
        // row for a given key would be a new max for that key, forcing a heap replace on every
        // single row instead of the common case where most rows are not the new best for their
        // key and get discarded without touching the arena. Hashing decorrelates the order key
        // from arrival order so the benchmark measures that common case, not the all-replace
        // worst case.
        def writeData(numKeys: Long): Unit = {
          spark.range(0, N, 1, 11)
            .selectExpr(s"id % $numKeys as k", "xxhash64(id) as ts")
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

            benchmark.addCase("hash pushdown: true") { _ =>
              withSQLConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
                dedupLatest()
              }
            }

            benchmark.run()
        }
      }
    }

    // Measures the same hash-based accumulator path, now generalized (see the SPARK-61804
    // follow-up that added ROW_NUMBER() <= K / RANK() <= K / DENSE_RANK() <= K support) against
    // the identical "few large groups" vs "all-unique keys" shapes above, with limit = 5 instead
    // of 1. RANK()/DENSE_RANK() additionally exercise the tie-aware candidate-list path (see
    // HashRankGroupLimitAccumulator), not just the bounded top-K heap ROW_NUMBER() uses.
    runBenchmark("Hash-Based Top-K (limit > 1) Window Group Limit") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath + "/topk_hash_benchmark_table"
        val N = 1024 * 1024 * 20
        val limit = 5

        // See the identical comment in the Top-1 benchmark above for why ts is xxhash64(id) rather
        // than id itself.
        def writeData(numKeys: Long): Unit = {
          spark.range(0, N, 1, 11)
            .selectExpr(s"id % $numKeys as k", "xxhash64(id) as ts")
            .write.mode("overwrite")
            .parquet(path)
        }

        def topK(rankLikeFunc: String): Unit = {
          spark.read.parquet(path)
            .selectExpr("*", s"$rankLikeFunc() over (partition by k order by ts desc) as rk")
            .where(s"rk <= $limit")
            .noop()
        }

        Seq(1024L -> "few large groups", N.toLong -> "all-unique keys").foreach {
          case (numKeys, desc) =>
            writeData(numKeys)
            Seq("row_number", "rank", "dense_rank").foreach { function =>
              val benchmark = new Benchmark(
                s"Top-$limit $function dedup ($desc, $numKeys distinct keys)",
                N,
                minNumIters = 5,
                output = output)

              benchmark.addCase("hash pushdown: false (sort-based)") { _ =>
                withSQLConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
                  topK(function)
                }
              }

              benchmark.addCase("hash pushdown: true") { _ =>
                withSQLConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
                  topK(function)
                }
              }

              benchmark.run()
            }
        }
      }
    }

    // Measures HashRankGroupLimitAccumulator.revalidateTrailingSlots -- the per-tie rescan that
    // keeps a key's slot array correct for RANK() (see its class doc's "Admission ... is not
    // permanent for RANK" section). Each call sums a growing prefix of slot weights on every one
    // of its O(slotCount) iterations, i.e. worst-case O(slotCount^2) per tie, not just O(limit) as
    // a quick read suggests. Neither shape above exercises that worst case: "few large groups"
    // concentrates ties on a single best value, whose weight alone quickly exceeds `limit` and
    // collapses slotCount to 1 (cutting the rescan short after a brief transient); "all-unique
    // keys" has no ties at all. This shape instead spreads ties round-robin across every one of
    // `limit` slots for a medium number of keys, so no single slot's weight runs away and the
    // rescan keeps operating near the full, uncollapsed slot count for the rest of the run.
    // DENSE_RANK is included purely as a near-zero-cost control: its flat per-slot weight never
    // changes, so revalidateTrailingSlots no-ops for it regardless of tie pattern (see the method's
    // doc) -- a large gap between the two here would indicate the RANK-only rescan is the cause.
    runBenchmark("Hash-Based RANK Top-K With Spread Ties (medium cardinality)") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath + "/topk_spread_ties_benchmark_table"
        val N = 1024 * 1024 * 20
        // Matches WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MAX_LIMIT's default exactly, maximizing the
        // slot count revalidateTrailingSlots has to rescan.
        val limit = 100
        // Medium cardinality: enough rows per key (N / numKeys = 2000) to tie against every one
        // of the `limit` slots many times over, unlike "few large groups"'s 1024 keys (~20K
        // rows/key, but ties concentrated on one value) or "all-unique keys"'s complete absence
        // of ties.
        val numKeys = 10000

        // k = id % numKeys assigns rows to keys round-robin. ts = (id DIV numKeys) % limit (integer
        // division, so this is id's 0-based position within its own key's row sequence, mod limit)
        // makes every key's first `limit` rows populate all `limit` slots with distinct values,
        // then cycles 0 until limit again for every row after that -- a tie, round-robin, against
        // an already-kept slot, spreading weight growth evenly across the whole slot array instead
        // of concentrating it on one.
        spark.range(0, N, 1, 11)
          .selectExpr(s"id % $numKeys as k", s"(id DIV $numKeys) % $limit as ts")
          .write.mode("overwrite")
          .parquet(path)

        def topK(rankLikeFunc: String): Unit = {
          spark.read.parquet(path)
            .selectExpr("*", s"$rankLikeFunc() over (partition by k order by ts asc) as rk")
            .where(s"rk <= $limit")
            .noop()
        }

        Seq("rank", "dense_rank").foreach { function =>
          val benchmark = new Benchmark(
            s"Top-$limit $function with spread ties ($numKeys distinct keys)",
            N,
            minNumIters = 5,
            output = output)

          benchmark.addCase("hash pushdown: false (sort-based)") { _ =>
            withSQLConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
              topK(function)
            }
          }

          benchmark.addCase("hash pushdown: true") { _ =>
            withSQLConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
              topK(function)
            }
          }

          benchmark.run()
        }
      }
    }
  }
}
