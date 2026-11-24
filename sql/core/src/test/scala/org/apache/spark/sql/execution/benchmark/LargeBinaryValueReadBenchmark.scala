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
import org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodec
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark reading a Parquet binary column whose values are large enough (~1MB each) that,
 * absent a chunk-wide uncompressed-size estimate, the vectorized reader's backing byte array
 * must grow and copy itself incrementally as it decodes page after page. See
 * apache/parquet-java#3988 / #3990, which exposes `PageReader.getTotalUncompressedSize()` so
 * a batch's likely byte need can be reserved once, up front, instead.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.LargeBinaryValueReadBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "sql/Test/runMain org.apache.spark.sql.execution.benchmark.LargeBinaryValueReadBenchmark"
 *      Results will be written to "benchmarks/LargeBinaryValueReadBenchmark-results.txt".
 * }}}
 */
object LargeBinaryValueReadBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("LargeBinaryValueReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "6g")
      .setIfMissing("spark.executor.memory", "6g")

    SparkSession.builder().config(conf).getOrCreate()
  }

  private val rows = 2000
  private val valueSizeBytes = 1024 * 1024

  def largeBinaryScanBenchmark(): Unit = {
    val benchmark = new Benchmark(
      s"Large Binary Scan (rows=$rows, bytes/value=$valueSizeBytes)", rows, output = output)

    withTempPath { dir =>
      import spark.implicits._
      // One large, incompressible value per row -- the shape that makes a batch's backing
      // byte array grow and copy itself many times over as it crosses page boundaries.
      spark.range(rows).map { _ =>
        val bytes = new Array[Byte](valueSizeBytes)
        Random.nextBytes(bytes)
        bytes
      }.toDF("c1").write
        .mode("overwrite")
        .option("compression", ParquetCompressionCodec.SNAPPY.lowerCaseName())
        .parquet(dir.getCanonicalPath)

      benchmark.addCase("SQL Parquet Vectorized") { _ =>
        spark.read.parquet(dir.getCanonicalPath).selectExpr("sum(length(c1))").noop()
      }

      benchmark.addCase("SQL Parquet MR") { _ =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
          spark.read.parquet(dir.getCanonicalPath).selectExpr("sum(length(c1))").noop()
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Large Binary Value Scan") {
      largeBinaryScanBenchmark()
    }
  }
}
