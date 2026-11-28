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

  // rows=100, 5-20MB/row (avg ~12.5MB) keeps nominal decompressed data around ~1.25GB, well
  // within a GitHub Actions runner's ~7GB memory limit, while each value is still large enough
  // (well above parquet.page.size's 1MB target) that a single row becomes one oversized page --
  // the shape that makes OnHeapColumnVector's doubling-and-copy dominate. The *varying* size
  // (vs. a fixed value size) also matters: it's what this benchmark is actually about, since a
  // uniform size can be pre-sized perfectly by any constant-capacity heuristic, whereas this
  // fix relies on the column chunk's *average* bytes/value, which is only meaningful when sizes
  // vary.
  private val rows = 100
  private val minMB = 5
  private val maxMB = 20

  private def readBenchmark(): Unit = {
    val benchmark = new Benchmark(
      s"Large Binary Read (variable 5-20MB/value, rows=$rows)", rows, output = output)

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
