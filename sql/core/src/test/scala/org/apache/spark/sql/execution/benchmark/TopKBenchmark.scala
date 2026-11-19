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
import org.apache.spark.sql.internal.SQLConf.{WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED}

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
