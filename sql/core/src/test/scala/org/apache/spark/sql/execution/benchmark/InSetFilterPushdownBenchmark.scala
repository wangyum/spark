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

import java.io.File

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

/**
 * Benchmark to measure read performance InSet Filter pushdown.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/InSetFilterPushdownBenchmark-results.txt".
 * }}}
 */
object InSetFilterPushdownBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("orc.compression", "snappy")
      .setIfMissing("spark.sql.parquet.compression.codec", "snappy")

    SparkSession.builder().config(conf).getOrCreate()
  }

  private val numRows = 1024 * 1024 * 15
  private val width = 5
  // For Parquet/ORC, we will use the same value for block size and compression size
  private val blockSize = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def prepareTable(dir: File, numRows: Int): Unit = {
    import spark.implicits._
    val selectExpr = (1 to width).map(i => s"CAST(value AS STRING) c$i")
    val df = spark.range(numRows).map(_ => Random.nextLong).selectExpr(selectExpr: _*)
      .withColumn("value", monotonically_increasing_id())
      .sort("value")

    df.write.mode("overwrite")
      .option("orc.compress.size", blockSize)
      .option("orc.stripe.size", blockSize).format("orc").saveAsTable("orcTable")

    df.write.mode("overwrite")
      .option("parquet.block.size", blockSize).format("parquet").saveAsTable("parquetTable")

    df.write.mode("overwrite").format("csv").saveAsTable("csvTable")
  }

  def filterPushDownBenchmark(
       values: Int,
       title: String,
       whereExpr: String,
       selectExpr: String = "*"): Unit = {
    val benchmark = new Benchmark(title, values, minNumIters = 5, output = output)

    Seq(Int.MaxValue, 10).foreach { pushDownEnabled =>
      val name = s"Parquet ${if (pushDownEnabled == 10) s"(Rewrite InSet)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf("spark.sql.optimizer.inSetRewriteMinMaxThreshold" -> s"$pushDownEnabled") {
          spark.sql(s"SELECT $selectExpr FROM parquetTable WHERE $whereExpr").noop()
        }
      }
    }

    Seq(Int.MaxValue, 10).foreach { pushDownEnabled =>
      val name = s"ORC ${if (pushDownEnabled == 10) s"(Rewrite InSet)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf("spark.sql.optimizer.inSetRewriteMinMaxThreshold" -> s"$pushDownEnabled") {
          spark.sql(s"SELECT $selectExpr FROM orcTable WHERE $whereExpr").noop()
        }
      }
    }

    Seq(Int.MaxValue, 10).foreach { pushDownEnabled =>
      val name = s"CSV ${if (pushDownEnabled == 10) s"(Rewrite InSet)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf("spark.sql.optimizer.inSetRewriteMinMaxThreshold" -> s"$pushDownEnabled") {
          spark.sql(s"SELECT $selectExpr FROM csvTable WHERE $whereExpr").noop()
        }
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Pushdown benchmark for rewrite InSet") {
      withTempPath { dir =>
        withTempTable("orcTable", "parquetTable") {
          prepareTable(dir, numRows)
          Seq(50, 1000, 5000, 20000).foreach { count =>
            Seq(1, 10, 50, 90).foreach { distribution =>
              val filter =
                Range(0, count).map(r => scala.util.Random.nextInt(numRows * distribution / 100))
              val whereExpr = s"value in(${filter.mkString(",")})"
              val title = s"Rewrite InSet (values count: $count, distribution: $distribution)"
              filterPushDownBenchmark(numRows, title, whereExpr)
            }
          }
        }
      }
    }
  }
}
