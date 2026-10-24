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

package org.apache.spark.sql.hive.execution

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, InMemoryFileIndex}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class DeferredFileListingSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  // Query stages are leaf nodes wrapping the plans they materialize, so a plain `collect`
  // does not reach the scans they contain.
  private def collectFileSourceScans(plan: SparkPlan): Seq[FileSourceScanExec] = {
    val direct = plan.collect { case f: FileSourceScanExec => f }
    val inStages = plan.collect { case s: QueryStageExec => s.plan }
      .flatMap(collectFileSourceScans)
    direct ++ inStages
  }

  private def findFileSourceScan(sqlStr: String, tableName: String): FileSourceScanExec = {
    val df = sql(sqlStr)
    df.collect()
    val plan = df.queryExecution.executedPlan match {
      case ae: AdaptiveSparkPlanExec => ae.finalPhysicalPlan
      case other => other
    }
    collectFileSourceScans(plan).find {
      f => f.tableIdentifier.exists(_.table == tableName)
    }.get
  }

  // The number of files listed for the scan. A deferred listing reports the final read count
  // through `numFiles` and leaves `staticFilesNum` unset, while an eager listing lists every
  // statically-matching file at planning time (reported through `staticFilesNum`) and
  // re-prunes them at execution time.
  private def listedFilesCount(scan: FileSourceScanExec): Long = {
    scan.driverMetrics.get("staticFilesNum").map(_.value).filter(_ > 0)
      .getOrElse(scan.driverMetrics("numFiles").value)
  }

  test("defer the file listing for a partition filter with a scalar subquery") {
    withTable("a", "b") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(5).write.saveAsTable("b")
      val sqlStr = "SELECT count(*) FROM a WHERE p <= (SELECT MIN(id) + 1 FROM b)"
      checkAnswer(sql(sqlStr), Row(9) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
      assert(scan.selectedPartitions.totalNumberOfFiles == 0)
      assert(listedFilesCount(scan) == 2)
    }
  }

  test("defer the file listing for a self-referencing subquery on the partition column") {
    withTable("a") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      val sqlStr = "SELECT count(*) FROM a WHERE p <= (SELECT min(id) + 1 FROM a)"
      checkAnswer(sql(sqlStr), Row(9) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
      assert(scan.selectedPartitions.totalNumberOfFiles == 0)
      assert(listedFilesCount(scan) == 2)
    }
  }

  test("defer the file listing with multiple partition columns") {
    withTable("a") {
      spark.range(100).selectExpr("id", "id % 5 AS p", "id % 2 AS p1")
        .write.partitionBy("p", "p1").saveAsTable("a")
      val sqlStr = "SELECT count(*) FROM a WHERE p1 = (SELECT min(id) + 1 FROM a)"
      checkAnswer(sql(sqlStr), Row(50) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
      assert(scan.selectedPartitions.totalNumberOfFiles == 0)
      assert(listedFilesCount(scan) == 5)
    }
  }

  test("defer the file listing for a Hive partitioned table") {
    withTable("a", "b") {
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql("CREATE TABLE a (id string) PARTITIONED BY (p string) STORED AS parquet")
        sql("INSERT INTO a VALUES (0, 0), (1, 1)")
        spark.range(2).write.saveAsTable("b")
        val sqlStr = "SELECT count(*) FROM a WHERE p <= (SELECT MIN(id) FROM b)"
        checkAnswer(sql(sqlStr), Row(1) :: Nil)

        val scan = findFileSourceScan(sqlStr, "a")
        assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
        assert(scan.selectedPartitions.totalNumberOfFiles == 0)
        assert(listedFilesCount(scan) == 1)
      }
    }
  }

  test("an IN subquery that becomes a semi-join keeps the deferral until planning") {
    withTable("a", "b") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(3).write.saveAsTable("b")
      val sqlStr = "SELECT count(*) FROM a WHERE p IN (SELECT id FROM b)"
      checkAnswer(sql(sqlStr), Row(13) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      // The IN subquery is rewritten into a semi-join, which leaves no filter above the
      // scan, so the listing is materialized at planning time with no partition filter to apply.
      assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
      assert(listedFilesCount(scan) == 5)
    }
  }

  test("materialize the deferred listing when the subquery is folded into a literal") {
    withTable("a") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      val sqlStr = "SELECT count(*) FROM a WHERE p = (SELECT 1)"
      checkAnswer(sql(sqlStr), Row(4) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      // The subquery is folded into a literal during optimization, so the filter is static
      // and it is eagerly materialized as an InMemoryFileIndex.
      assert(scan.relation.location.isInstanceOf[InMemoryFileIndex])
      assert(listedFilesCount(scan) == 1)
    }
  }

  test("defer listing even when a static partition filter is present with subquery") {
    withTable("a", "b") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(5).write.saveAsTable("b")
      val sqlStr = "SELECT count(*) FROM a WHERE p <= 1 AND p <= (SELECT MIN(id) + 1 FROM b)"
      checkAnswer(sql(sqlStr), Row(9) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      // Both a static filter (p <= 1) and a subquery filter are present: the listing is
      // deferred anyway, and at execution time the materialized subquery is pushed to the
      // metastore together with the static filter, so only the surviving partitions are
      // listed.
      assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
      assert(scan.selectedPartitions.totalNumberOfFiles == 0)
      assert(listedFilesCount(scan) == 2)
    }
  }

  test("planning-time reads do not end the deferral") {
    withTable("a", "b") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(5).write.saveAsTable("b")
      val df = sql("SELECT count(*) FROM a WHERE p <= (SELECT MIN(id) + 1 FROM b)")
      val plan = df.queryExecution.executedPlan match {
        case ae: AdaptiveSparkPlanExec => ae.executedPlan
        case other => other
      }
      val scan = collectFileSourceScans(plan)
        .find(_.tableIdentifier.exists(_.table == "a")).get

      // A planning-time reader sees the honest "nothing is statically selected" answer
      // without deciding the listing: the deferral survives.
      assert(scan.selectedPartitions.totalNumberOfFiles == 0)

      // A scan marked for single-task execution cannot judge its file counts, so it gives up
      // the single-task merge instead of ending the deferral.
      val marked = scan.copy(markedForSingleTaskExecution = true)
      assert(!marked.useSingleTaskExecution)

      // The deferred listing still decides the files at execution.
      checkAnswer(df, Row(9) :: Nil)
      assert(listedFilesCount(scan) == 2)
    }
  }

  test("push materialized dynamic partition filter down to metastore") {
    withTable("a", "b") {
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql("CREATE TABLE a (id string) PARTITIONED BY (p int) STORED AS parquet")
        (0 until 5).foreach { i =>
          sql(s"INSERT INTO a VALUES ($i, $i)")
        }
        spark.range(2).write.saveAsTable("b")
        val sqlStr = "SELECT count(*) FROM a WHERE p <= (SELECT MIN(id) FROM b)"
        val df = sql(sqlStr)

        HiveCatalogMetrics.reset()
        assert(df.collect() === Array(Row(1)))
        // Materialized dynamic filter (p <= 0) is pushed down to metastore,
        // so metastore returns only 1 partition rather than all 5!
        assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount == 1)
      }
    }
  }
}
