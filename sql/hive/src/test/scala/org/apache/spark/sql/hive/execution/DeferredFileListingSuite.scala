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
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite, QueryStageExec}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, InMemoryFileIndex}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

abstract class DeferredFileListingSuiteBase
    extends QueryTest with SQLTestUtils with TestHiveSingleton {

  // Query stages are leaf nodes wrapping the plans they materialize, so a plain `collect`
  // does not reach the scans they contain.
  private def collectFileSourceScans(plan: SparkPlan): Seq[FileSourceScanExec] = {
    val direct = plan.collect { case f: FileSourceScanExec => f }
    val inStages = plan.collect { case s: QueryStageExec => s.plan }
      .flatMap(collectFileSourceScans)
    direct ++ inStages
  }

  // Finds the scan in a fresh execution of `sqlStr`. Since this runs the query again, it must
  // not be combined with a `HiveCatalogMetrics` assertion on the same test: use
  // `findFileSourceScanIn` on an already-executed `DataFrame` for that instead, so the query
  // runs exactly once and the metric reflects that single execution.
  private def findFileSourceScan(sqlStr: String, tableName: String): FileSourceScanExec = {
    val df = sql(sqlStr)
    df.collect()
    findFileSourceScanIn(df, tableName)
  }

  private def findFileSourceScanIn(df: DataFrame, tableName: String): FileSourceScanExec = {
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

  test("an IN subquery rewritten to a semi-join does not trigger deferral") {
    withTable("a", "b") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(3).write.saveAsTable("b")
      val sqlStr = "SELECT count(*) FROM a WHERE p IN (SELECT id FROM b)"
      checkAnswer(sql(sqlStr), Row(13) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      // The IN subquery is rewritten into a semi-join during optimization, which leaves no
      // filter above the scan at all -- the predicate becomes the join condition instead. With
      // no filters to inspect, `PruneFileSourcePartitions`'s pattern match does not even fire, so
      // the location is left completely untouched: still the original `CatalogFileIndex`, not
      // replaced by an eager `InMemoryFileIndex`. There is also no dynamic partition filter
      // attached to this scan (the semi-join is evaluated by the join operator, not pushed down
      // as a scan-level filter), so `shouldDeferListing` does not engage either: the scan simply
      // lists every partition directly off the still-`CatalogFileIndex` location.
      assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
      assert(scan.selectedPartitions.totalNumberOfFiles == 5)
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

  test("a static partition filter alongside a subquery is pruned eagerly, the subquery falls " +
      "back to local filtering") {
    withTable("a", "b") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(5).write.saveAsTable("b")
      val sqlStr = "SELECT count(*) FROM a WHERE p <= 1 AND p <= (SELECT MIN(id) + 1 FROM b)"
      checkAnswer(sql(sqlStr), Row(9) :: Nil)

      val scan = findFileSourceScan(sqlStr, "a")
      // The static filter (p <= 1) is eagerly pruned by `PruneFileSourcePartitions` so that
      // join reordering sees an accurate size, which replaces the location with an
      // InMemoryFileIndex. That means the location is no longer a CatalogFileIndex, so the
      // remaining subquery filter no longer defers: it locally filters the already-listed
      // partitions at execution time instead, same as before the deferred-listing feature.
      assert(scan.relation.location.isInstanceOf[InMemoryFileIndex])
      assert(scan.selectedPartitions.totalNumberOfFiles == 2)
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

  test("the deferral kill switch disables deferred listing") {
    withSQLConf(SQLConf.HIVE_METASTORE_DEFER_FILE_LISTING.key -> "false") {
      withTable("a", "b") {
        spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
        spark.range(5).write.saveAsTable("b")
        val sqlStr = "SELECT count(*) FROM a WHERE p <= (SELECT MIN(id) + 1 FROM b)"
        checkAnswer(sql(sqlStr), Row(9) :: Nil)

        val scan = findFileSourceScan(sqlStr, "a")
        // With the kill switch off, `shouldDeferListing` never engages even though the location
        // is still a `CatalogFileIndex` (same as "defer the file listing for a partition filter
        // with a scalar subquery" above): all 5 partitions are listed eagerly at planning time,
        // and the subquery's result is applied as a local post-filter at execution time instead
        // of being pushed to the metastore -- the correct answer is unaffected either way.
        assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
        assert(scan.selectedPartitions.totalNumberOfFiles == 5)
      }
    }
  }

  test("an empty materialized DPP IN list short-circuits without a metastore call") {
    withSQLConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("a", "b") {
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          sql("CREATE TABLE a (id string) PARTITIONED BY (p int) STORED AS parquet")
          (0 until 4).foreach(i => sql(s"INSERT INTO a VALUES ('$i', $i)"))
          // b is intentionally left empty, so the broadcast join key's materialized values are
          // an empty array. No ANALYZE is run, so the optimizer does not know this ahead of time
          // and still plans a normal dynamic-pruning-eligible broadcast join.
          sql("CREATE TABLE b (id int) STORED AS parquet")
          // A co-present, always-true scalar subquery filter puts a real subquery-based
          // partition filter on the scan so that `shouldDeferListing` engages; the broadcast
          // join supplies the classic DPP `InSubqueryExec` under test here. See
          // `isDeferrableDynamicPruningFilter` for why the DPP filter alone would not do this.
          val sqlStr =
            "SELECT /*+ BROADCAST(b) */ count(*) FROM a JOIN b ON a.p = b.id " +
              "WHERE a.p <= (SELECT MAX(id) + 1000 FROM b)"
          val df = sql(sqlStr)

          HiveCatalogMetrics.reset()
          assert(df.collect() === Array(Row(0)))
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount == 0)
        }
      }
    }
  }

  test("an oversized materialized DPP IN list is narrowed to a range push with an exact local " +
      "post-filter") {
    withSQLConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD.key -> "1") {
      withTable("a", "b") {
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          sql("CREATE TABLE a (id string) PARTITIONED BY (p int) STORED AS parquet")
          (0 until 4).foreach(i => sql(s"INSERT INTO a VALUES ('$i', $i)"))
          sql("CREATE TABLE b (id int) STORED AS parquet")
          sql("INSERT INTO b VALUES (0), (3)")
          // Same co-present-subquery trick as above, to force deferral while the broadcast join
          // supplies the DPP IN-list under test.
          val sqlStr =
            "SELECT /*+ BROADCAST(b) */ count(*) FROM a JOIN b ON a.p = b.id " +
              "WHERE a.p <= (SELECT MAX(id) + 100 FROM b)"
          val df = sql(sqlStr)

          HiveCatalogMetrics.reset()
          assert(df.collect() === Array(Row(2)))
          // The materialized IN list has 2 distinct values (0, 3), exceeding the lowered
          // threshold of 1: the metastore push narrows to the [0, 3] range, which also matches
          // the non-selected partitions p=1 and p=2, so all 4 come back from the metastore...
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount == 4)
          // ...but the exact value set is still applied as a local post-filter on top of that
          // range (see `CatalogFileIndex.materializeInSubquery`), so the final answer above only
          // counts the 2 partitions that actually match -- the range narrowing does not leak
          // extra rows into the result.
        }
      }
    }
  }

  test("classic broadcast-join dynamic partition pruning never defers listing on its own") {
    withSQLConf(
        SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("a", "b") {
        spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
        spark.range(2).write.saveAsTable("b")
        val sqlStr = "SELECT /*+ BROADCAST(b) */ count(*) FROM a JOIN b ON a.p = b.id"
        checkAnswer(sql(sqlStr), Row(9) :: Nil)

        val scan = findFileSourceScan(sqlStr, "a")
        // With constraint propagation disabled, `PruneFileSourcePartitions` has no incidental
        // `IsNotNull` to force eager pruning ahead of `PartitionPruning` attaching this classic
        // broadcast-join DPP filter, so the location can still be the original `CatalogFileIndex`
        // here -- yet the scan must still list every partition eagerly up front and prune
        // locally, exactly like the pre-existing DPP behavior, rather than deferring on the
        // strength of this filter alone (see `isDeferrableDynamicPruningFilter`).
        assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
        assert(scan.selectedPartitions.totalNumberOfFiles == 5)
      }
    }
  }

  // Classic broadcast-join dynamic partition pruning (no subquery involved) is intentionally out
  // of scope for deferred listing: `PruneFileSourcePartitions` cannot see a classic DPP filter at
  // the time it decides whether to prune eagerly -- `PartitionPruning`, the optimizer rule that
  // attaches it, runs in a separate, later batch. Treating "no meaningful static filter yet" as a
  // reason to defer for that case regressed the `PartitionPruning` filtering-ratio heuristic
  // (which depends on `PruneFileSourcePartitions` having refreshed `CatalogStatistics` via an
  // eager listing) and the "static scan metrics" contract (`staticFilesNum`/`staticFilesSize`),
  // which only ever get populated by an eager pre-DPP listing. So most of this suite exercises
  // deferral through a real subquery-based partition filter, and classic broadcast-join DPP keeps
  // its pre-existing eager-listing behavior -- made explicit above by
  // `isDeferrableDynamicPruningFilter` rather than resting on that incidental timing alone -- and
  // is verified more broadly by `DynamicPartitionPruningSuite` /
  // `DynamicPartitionPruningHiveScanSuite`.
}

class DeferredFileListingSuiteAEOff
  extends DeferredFileListingSuiteBase with DisableAdaptiveExecutionSuite

class DeferredFileListingSuiteAEOn
  extends DeferredFileListingSuiteBase with EnableAdaptiveExecutionSuite
