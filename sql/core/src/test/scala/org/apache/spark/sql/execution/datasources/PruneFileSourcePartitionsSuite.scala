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

package org.apache.spark.sql.execution.datasources

import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CreateNamedStruct, DynamicPruningExpression, Expression, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, LocalTableScanExec, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}

class PruneFileSourcePartitionsSuite extends PrunePartitionSuiteBase with SharedSparkSession {

  override def beforeEach(): Unit = {
    super.beforeEach()
    System.gc()
  }

  override def format: String = "parquet"

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PruneFileSourcePartitions", Once, PruneFileSourcePartitions) :: Nil
  }

  test("PruneFileSourcePartitions should not change the output of LogicalRelation") {
    withTable("test") {
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("test")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
      val catalogFileIndex = new CatalogFileIndex(spark, tableMeta, 0)

      val dataSchema = StructType(tableMeta.schema.filterNot { f =>
        tableMeta.partitionColumnNames.contains(f.name)
      })
      val relation = HadoopFsRelation(
        location = catalogFileIndex,
        partitionSchema = tableMeta.partitionSchema,
        dataSchema = dataSchema,
        bucketSpec = None,
        fileFormat = new ParquetFileFormat(),
        options = Map.empty)(sparkSession = spark)

      val logicalRelation = LogicalRelation(relation, tableMeta)
      val query = Project(Seq($"id", $"p"),
        Filter($"p" === 1, logicalRelation)).analyze

      val optimized = Optimize.execute(query)
      assert(optimized.missingInput.isEmpty)
    }
  }

  test("SPARK-20986 Reset table's statistics after PruneFileSourcePartitions rule") {
    withTable("tbl") {
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
      sql(s"ANALYZE TABLE tbl COMPUTE STATISTICS")
      val tableStats = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl")).stats
      assert(tableStats.isDefined && tableStats.get.sizeInBytes > 0, "tableStats is lost")

      val df = sql("SELECT * FROM tbl WHERE p = 1")
      val sizes1 = df.queryExecution.analyzed.collect {
        case relation: LogicalRelation => relation.catalogTable.get.stats.get.sizeInBytes
      }
      assert(sizes1.size === 1, s"Size wrong for:\n ${df.queryExecution}")
      assert(sizes1(0) == tableStats.get.sizeInBytes)

      val relations = df.queryExecution.optimizedPlan.collect {
        case relation: LogicalRelation => relation
      }
      assert(relations.size === 1, s"Size wrong for:\n ${df.queryExecution}")
      val size2 = relations(0).stats.sizeInBytes
      assert(size2 == relations(0).catalogTable.get.stats.get.sizeInBytes)
      assert(size2 < tableStats.get.sizeInBytes)
    }
  }

  test("SPARK-26576 Broadcast hint not applied to partitioned table") {
    withTable("tbl") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
        val df = spark.table("tbl")
        val qe = df.join(broadcast(df), "p").queryExecution
        qe.sparkPlan.collect { case j: BroadcastHashJoinExec => j } should have size 1
      }
    }
  }

  test("SPARK-35985 push filters for empty read schema") {
    // Force datasource v2 for parquet
    withSQLConf((SQLConf.USE_V1_SOURCE_LIST.key, "")) {
      withTempPath { dir =>
        spark.range(10).coalesce(1).selectExpr("id", "id % 3 as p")
            .write.partitionBy("p").parquet(dir.getCanonicalPath)
        withTempView("tmp") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tmp");
          assertPrunedPartitions("SELECT COUNT(*) FROM tmp WHERE p = 0", 1, "(tmp.p = 0)")
          assertPrunedPartitions("SELECT input_file_name() FROM tmp WHERE p = 0", 1, "(tmp.p = 0)")
        }
      }
    }
  }

  test("SPARK-38357: data + partition filters with OR") {
    // Force datasource v2 for parquet
    withSQLConf((SQLConf.USE_V1_SOURCE_LIST.key, "")) {
      withTempPath { dir =>
        spark.range(10).coalesce(1).selectExpr("id", "id % 3 as p")
          .write.partitionBy("p").parquet(dir.getCanonicalPath)
        withTempView("tmp") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tmp");
          assertPrunedPartitions("SELECT * FROM tmp WHERE (p = 0 AND id > 0) OR (p = 1 AND id = 2)",
            2,
            "((tmp.p = 0) || (tmp.p = 1))")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p = 0 AND id > 0",
            1,
            "(tmp.p = 0)")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p = 0",
            1,
            "(tmp.p = 0)")
        }
      }
    }
  }

  test("SPARK-40565: don't push down non-deterministic filters for V2 file sources") {
    // Force datasource v2 for parquet
    withSQLConf((SQLConf.USE_V1_SOURCE_LIST.key, "")) {
      withTempPath { dir =>
        spark.range(10).coalesce(1).selectExpr("id", "id % 3 as p")
          .write.partitionBy("p").parquet(dir.getCanonicalPath)
        withTempView("tmp") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("tmp")
          assertPrunedPartitions("SELECT * FROM tmp WHERE rand() > 0.5", 3, "")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p > rand()", 3, "")
          assertPrunedPartitions("SELECT * FROM tmp WHERE p = 0 AND rand() > 0.5",
            1,
            "(tmp.p = 0)")
        }
      }
    }
  }

  test("defer the file listing for a partition filter with a scalar subquery against the " +
      "in-memory catalog") {
    // The deferred-listing feature is implemented in `CatalogFileIndex`, which is used for any
    // catalog table backed by `ExternalCatalog` -- not only the Hive metastore -- so it must
    // also engage for a plain, non-Hive session catalog table. See
    // `org.apache.spark.sql.hive.execution.DeferredFileListingSuiteBase` for the equivalent
    // Hive-backed coverage, including the metastore-pushdown assertions that only apply there.
    withTable("a", "b") {
      spark.range(21).selectExpr("id", "id % 5 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(5).write.saveAsTable("b")
      val sqlStr = "SELECT count(*) FROM a WHERE p <= (SELECT MIN(id) + 1 FROM b)"
      checkAnswer(sql(sqlStr), Row(9) :: Nil)

      // Use `sparkPlan` rather than `executedPlan`: AQE is on by default in this suite, and
      // `executedPlan` would wrap the plan in an `AdaptiveSparkPlanExec` that `collectFirst`
      // cannot see through.
      val scan = sql(sqlStr).queryExecution.sparkPlan.collectFirst {
        case f: FileSourceScanExec => f
      }.get
      assert(scan.relation.location.isInstanceOf[CatalogFileIndex])
      assert(scan.selectedPartitions.totalNumberOfFiles == 0)
    }
  }

  test("a multi-column CreateNamedStruct dynamic partition filter is applied as an exact " +
      "local post-filter instead of being silently dropped by metastore pushdown") {
    // Whitebox-constructs InSubqueryExec directly (bypassing analysis/optimization), following
    // the pattern in SubquerySuite's SPARK-58481 tests, because reaching this exact shape -- a
    // CreateNamedStruct-rooted dynamic partition filter -- through ordinary SQL is unreliable:
    // classic broadcast-join DPP never combines multiple partition columns into one filter, and
    // an ordinary multi-column `(p1, p2) IN (SELECT ...)` predicate is rewritten into a semi-join
    // rather than left as a scan-level InSubqueryExec filter. See
    // CatalogFileIndex.materializeInSubquery for the code under test.
    withTable("a") {
      spark.range(4).selectExpr("id", "id AS p1", "id AS p2")
        .write.partitionBy("p1", "p2").saveAsTable("a")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "a")
      val catalogFileIndex = new CatalogFileIndex(spark, tableMeta, 0)

      // `id` is BIGINT, so the partitions this writes are partitioned by `p1`/`p2` LongType
      // columns -- the LHS struct and the subquery's materialized result rows must match that.
      val p1 = AttributeReference("p1", LongType)()
      val p2 = AttributeReference("p2", LongType)()
      val lhs = CreateNamedStruct(Seq(Literal("p1"), p1, Literal("p2"), p2))

      val subplanOutput = Seq(
        AttributeReference("p1", LongType)(),
        AttributeReference("p2", LongType)())
      val subplan = SubqueryExec("test_subquery", LocalTableScanExec(subplanOutput, Nil, None))
      val inSubquery = InSubqueryExec(
        lhs, subplan, ExprId(0), isDynamicPruning = true,
        result = Array[InternalRow](InternalRow(1L, 1L), InternalRow(3L, 3L))
          .asInstanceOf[Array[Any]])
      val filter = DynamicPruningExpression(inSubquery)

      // Must not throw -- a naive attempt to push `In(CreateNamedStruct(...), ...)` to the
      // metastore would hit HiveShim's ExtractAttribute extractor, which never matches a
      // CreateNamedStruct and would silently drop the filter instead of failing loudly, so the
      // real assertion is on the returned partitions below, not just the absence of an exception.
      val index = catalogFileIndex.filterPartitions(Seq(filter))
      val selected = index.partitionSpec().partitions
        .map(p => (p.values.getLong(0), p.values.getLong(1))).toSet
      assert(selected == Set((1L, 1L), (3L, 3L)))
    }
  }

  test("a 3-column CreateNamedStruct dynamic partition filter is applied as an exact local " +
      "post-filter") {
    // Same shape as the 2-column test above, but with a third partition column and values chosen
    // so that no column happens to equal the row's position within the struct (p1 == id, but p2
    // and p3 are deliberately not), to guard against a column-ordering mixup that 2 columns alone
    // might not expose. See CatalogFileIndex.materializeInSubquery for the code under test.
    withTable("a") {
      spark.range(4).selectExpr("id", "id AS p1", "id % 2 AS p2", "3 - id AS p3")
        .write.partitionBy("p1", "p2", "p3").saveAsTable("a")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "a")
      val catalogFileIndex = new CatalogFileIndex(spark, tableMeta, 0)

      val p1 = AttributeReference("p1", LongType)()
      val p2 = AttributeReference("p2", LongType)()
      val p3 = AttributeReference("p3", LongType)()
      val lhs = CreateNamedStruct(
        Seq(Literal("p1"), p1, Literal("p2"), p2, Literal("p3"), p3))

      val subplanOutput = Seq(
        AttributeReference("p1", LongType)(),
        AttributeReference("p2", LongType)(),
        AttributeReference("p3", LongType)())
      val subplan = SubqueryExec("test_subquery", LocalTableScanExec(subplanOutput, Nil, None))
      // Rows for id=1 (p1=1, p2=1, p3=2) and id=3 (p1=3, p2=1, p3=0).
      val inSubquery = InSubqueryExec(
        lhs, subplan, ExprId(0), isDynamicPruning = true,
        result = Array[InternalRow](InternalRow(1L, 1L, 2L), InternalRow(3L, 1L, 0L))
          .asInstanceOf[Array[Any]])
      val filter = DynamicPruningExpression(inSubquery)

      val index = catalogFileIndex.filterPartitions(Seq(filter))
      val selected = index.partitionSpec().partitions
        .map(p => (p.values.getLong(0), p.values.getLong(1), p.values.getLong(2))).toSet
      assert(selected == Set((1L, 1L, 2L), (3L, 1L, 0L)))
    }
  }

  test("a bare (non dynamic-pruning) InSubqueryExec partition filter is applied as a local " +
      "post-filter, not materialized and pushed to the metastore") {
    // A bare `InSubqueryExec` not wrapped in `DynamicPruningExpression` is what a static
    // IN-subquery partition filter compiles to when it cannot be rewritten into a semi-join, e.g.
    // `(p IN (SELECT ...)) OR q = 1` -- `PlanSubqueries` sets `isDynamicPruning = false` for this
    // shape (see subquery.scala). `materializeDynamicFilters` deliberately does not match this
    // shape: substituting a materialized literal in its place could silently change the meaning
    // of a surrounding `Not`/`Or` it might be nested inside (unlike the standalone top-level AND
    // conjunct a `DynamicPruningExpression` is always injected as). It is left untouched instead,
    // falls through to `splitPartitionFilters`'s post-filter branch, and is evaluated locally
    // against the metastore's returned partitions -- the same outcome, reached safely.
    // Whitebox-constructs it directly, following the pattern of the CreateNamedStruct test above,
    // since reaching this exact shape through ordinary SQL depends on incidental planner behavior
    // (whether the IN subquery is rewritten into a semi-join) this test should not depend on.
    // See CatalogFileIndex.materializeDynamicFilters for the code under test.
    withTable("a") {
      spark.range(4).selectExpr("id", "id AS p").write.partitionBy("p").saveAsTable("a")
      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "a")
      val catalogFileIndex = new CatalogFileIndex(spark, tableMeta, 0)

      val p = AttributeReference("p", LongType)()
      val subplanOutput = Seq(AttributeReference("p", LongType)())
      val subplan = SubqueryExec("test_subquery", LocalTableScanExec(subplanOutput, Nil, None))
      val inSubquery = InSubqueryExec(
        p, subplan, ExprId(0), isDynamicPruning = false, result = Array[Any](1L, 3L))

      val index = catalogFileIndex.filterPartitions(Seq(inSubquery))
      val selected = index.partitionSpec().partitions.map(_.values.getLong(0)).toSet
      assert(selected == Set(1L, 3L))
    }
  }

  protected def collectPartitionFiltersFn(): PartialFunction[SparkPlan, Seq[Expression]] = {
    case scan: FileSourceScanExec => scan.partitionFilters
  }

  override def getScanExecPartitionSize(plan: SparkPlan): Long = {
    plan.collectFirst {
      case p: FileSourceScanExec => p.selectedPartitions.partitionCount
      case BatchScanExec(_, scan: FileScan, _, _, _, _) =>
        scan.fileIndex.listFiles(scan.partitionFilters, scan.dataFilters).length
    }.get
  }
}
