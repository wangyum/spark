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

/**
 * Benchmark reading a Parquet binary column whose values are large (~1MB each, e.g. image
 * blobs) through Spark's vectorized reader, across UNCOMPRESSED/SNAPPY/LZ4_RAW.
 *
 * Exercises apache/parquet-java#3996: `NonBlockedDecompressor` grows its scratch `ByteBuffer`
 * by 1.2x per call while `SnappyCodec`/`Lz4RawCodec` feed it in 4K (`io.file.buffer.size`)
 * chunks, so decompressing one large page triggers dozens of buffer reallocations and copies.
 * apache/parquet-java#3997 replaces that path with a single `DirectDecompressor` call per page,
 * backed by a reused buffer. The UNCOMPRESSED case is a control: it shares the same read path
 * minus decompression, so comparing deltas isolates how much of the win is decompression itself
 * versus general I/O/scan overhead.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.ParquetLargeBinaryDecompressBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "sql/Test/runMain org.apache.spark.sql.execution.benchmark.ParquetLargeBinaryDecompressBenchmark"
 *      Results will be written to "benchmarks/ParquetLargeBinaryDecompressBenchmark-results.txt".
 * }}}
 */
object ParquetLargeBinaryDecompressBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("ParquetLargeBinaryDecompressBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "6g")
      .setIfMissing("spark.executor.memory", "6g")

    SparkSession.builder().config(conf).getOrCreate()
  }

  private val rows = 2000
  private val valueSizeBytes = 1024 * 1024
  private val codecs = Seq(
    ParquetCompressionCodec.UNCOMPRESSED,
    ParquetCompressionCodec.SNAPPY,
    ParquetCompressionCodec.LZ4_RAW)

  private def readBenchmark(codec: ParquetCompressionCodec): Unit = {
    val benchmark = new Benchmark(
      s"Large Binary Read ($codec, rows=$rows, bytes/value=$valueSizeBytes)", rows, output = output)

    withTempPath { dir =>
      import spark.implicits._
      // One large, incompressible value per row -- the shape that makes a single page's worth
      // of SNAPPY/LZ4_RAW output large enough for GH-3996 to matter.
      spark.range(rows).map { _ =>
        val bytes = new Array[Byte](valueSizeBytes)
        Random.nextBytes(bytes)
        bytes
      }.toDF("c1").write
        .mode("overwrite")
        .option("compression", codec.lowerCaseName())
        .parquet(dir.getCanonicalPath)

      benchmark.addCase("SQL Parquet Vectorized") { _ =>
        spark.read.parquet(dir.getCanonicalPath).selectExpr("sum(length(c1))").noop()
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Large Binary Value Scan (SNAPPY/LZ4_RAW decompress)") {
      codecs.foreach(readBenchmark)
    }
  }
}