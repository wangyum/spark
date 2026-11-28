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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession

/**
 * Benchmark reading a Parquet binary column whose per-row values vary in size (5-20MB each,
 * e.g. image blobs of differing resolution), matching the shape that originally motivated
 * GH-3988: `OnHeapColumnVector`'s backing byte[] grows by repeated 2x-doubling-and-copy as each
 * oversized value is appended one page at a time, since the vector starts from a small default
 * capacity and has no prior knowledge of how large a single value will be. A CPU profile of this
 * exact shape attributed ~25-35% of samples to this doubling-and-copy pattern (array allocation
 * zeroing + `System.arraycopy`), dwarfing decompression itself.
 *
 * apache/parquet-java#3990 exposes `PageReader.getTotalUncompressedSize()` (summed alongside the
 * existing `getTotalValueCount()`), letting a reader compute this column chunk's average
 * uncompressed bytes/value *before* decoding any page. This benchmark pairs that with a
 * consumer-side patch to `VectorizedColumnReader.readBatch()` that pre-sizes the backing array
 * once per batch using that average, instead of letting it grow via repeated doubling.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.ParquetLargeBinaryVariableSizeReadBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "sql/Test/runMain org.apache.spark.sql.execution.benchmark.ParquetLargeBinaryVariableSizeReadBenchmark"
 *      Results will be written to
 *      "benchmarks/ParquetLargeBinaryVariableSizeReadBenchmark-results.txt".
 * }}}
 */
object ParquetLargeBinaryVariableSizeReadBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("ParquetLargeBinaryVariableSizeReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "6g")
      .setIfMissing("spark.executor.memory", "6g")

    SparkSession.builder().config(conf).getOrCreate()
  }

  // All rows for this column land in a single vectorized read batch (far below the default
  // 4096-row batch size), so OnHeapColumnVector's backing byte[] has to hold every row's bytes
  // concatenated -- its doubling-and-copy peak scales with the FULL dataset size, not a single
  // row. A first attempt at rows=100/5-20MB (nominal ~1.25GB) OOM'd the baseline pass on a 6g
  // driver heap before it even finished growing (requested 871MB mid-growth) -- confirming the
  // doubling behavior is expensive, but leaving no usable timing comparison. rows=40, 5-10MB/row
  // (nominal ~300MB, worst-case ~600MB doubling peak) keeps well clear of that ceiling while
  // each value is still far above parquet.page.size's 1MB target (one oversized page per row)
  // and sizes still *vary* (5-10MB) -- the varying part matters because a uniform value size can
  // be pre-sized perfectly by any constant-capacity heuristic, whereas this fix relies on the
  // column chunk's *average* bytes/value, which is only meaningful when sizes vary.
  private val rows = 40
  private val minMB = 5
  private val maxMB = 10

  private def readBenchmark(): Unit = {
    val benchmark = new Benchmark(
      s"Large Binary Read (variable 5-10MB/value, rows=$rows)", rows, output = output)

    withTempPath { dir =>
      import spark.implicits._
      val rnd = new Random(42)
      spark.range(rows).mapPartitions { it =>
        val rnd2 = new Random(42)
        it.map { _ =>
          val mb = minMB + rnd2.nextInt(maxMB - minMB + 1)
          val bytes = new Array[Byte](mb * 1024 * 1024)
          rnd2.nextBytes(bytes)
          bytes
        }
      }.toDF("c1").write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(dir.getCanonicalPath)

      benchmark.addCase("SQL Parquet Vectorized") { _ =>
        spark.read.parquet(dir.getCanonicalPath).selectExpr("sum(length(c1))").noop()
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Large Binary Value Scan, variable size (GH-3988 pre-size vs. doubling)") {
      readBenchmark()
    }
  }
}
