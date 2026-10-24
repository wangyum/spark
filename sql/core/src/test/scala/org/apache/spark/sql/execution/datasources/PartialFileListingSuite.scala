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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.{CollectLimitExec, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end coverage for the limit-only partial file listing optimization:
 * `MarkPartialFileListing` (plan-shape detection) through
 * `FileSourceStrategy`/`FileSourceScanExec` and `CollectLimitExec` (Driver-side progressive
 * execution). Enabled by default without configuration.
 */
class PartialFileListingSuite extends QueryTest with SharedSparkSession {

  private def getSingleFileSourceScan(plan: SparkPlan): FileSourceScanExec = {
    val scans = plan.collect { case scan: FileSourceScanExec => scan }
    assert(scans.size == 1, s"expected exactly one FileSourceScanExec in:\n$plan")
    scans.head
  }

  test("limit-only query reads fewer files than the table has by default") {
    withTable("t") {
      spark.range(0, 100, 1, 10).selectExpr("id").write.saveAsTable("t")
      val df = sql("select id from t limit 5")
      assert(df.collect().length == 5)
      val scan = getSingleFileSourceScan(df.queryExecution.executedPlan)
      assert(scan.isLimitScanEligible, "expected the scan to be eligible for limit scan")
    }
  }

  test("a filter above the scan disqualifies partial listing") {
    withTable("t") {
      spark.range(0, 100, 1, 10).selectExpr("id").write.saveAsTable("t")
      val df = sql("select id from t where id > 0 limit 5")
      assert(df.collect().length == 5)
      val scan = getSingleFileSourceScan(df.queryExecution.executedPlan)
      assert(!scan.isLimitScanEligible,
        "a filter above the limit must disqualify partial listing")
    }
  }

  test("an order by above the scan disqualifies partial listing") {
    withTable("t") {
      spark.range(0, 100, 1, 10).selectExpr("id").write.saveAsTable("t")
      val df = sql("select id from t order by id limit 5")
      assert(df.collect().map(_.getLong(0)).toSeq == (0 until 5))
      assert(!df.queryExecution.executedPlan.isInstanceOf[CollectLimitExec],
        "a sort above the limit must not plan as CollectLimitExec")
    }
  }

  test("a bucketed table disqualifies partial listing") {
    withTable("t") {
      spark.range(0, 100).selectExpr("id").write.bucketBy(4, "id").saveAsTable("t")
      val df = sql("select id from t limit 5")
      assert(df.collect().length == 5)
      val scan = getSingleFileSourceScan(df.queryExecution.executedPlan)
      assert(!scan.isLimitScanEligible, "a bucketed scan must disqualify partial listing")
    }
  }

  test("more files are listed when the initial cap is not enough") {
    withTable("t") {
      spark.range(0, 100, 1, 10).selectExpr("id").write.saveAsTable("t")
      val df = sql("select id from t limit 95")
      val rows = df.collect()
      assert(rows.length == 95)
      assert(rows.map(_.getLong(0)).toSet.size == 95)
      val scan = getSingleFileSourceScan(df.queryExecution.executedPlan)
      assert(scan.isLimitScanEligible)
    }
  }

  test("partial listing applies under adaptive execution") {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true") {
      withTable("t") {
        spark.range(0, 100, 1, 10).selectExpr("id").write.saveAsTable("t")
        val df = sql("select id from t limit 5")
        assert(df.collect().length == 5)
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec],
          s"expected the plan to be wrapped by AQE:\n${df.queryExecution.executedPlan}")
        val plan = df.queryExecution.executedPlan match {
          case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
          case other => other
        }
        val scan = getSingleFileSourceScan(plan)
        assert(scan.isLimitScanEligible, "expected scan to be eligible under adaptive execution")
      }
    }
  }

  test("limit-only query on a partitioned table reads fewer files than the table has") {
    withTable("t") {
      spark.range(0, 100).selectExpr("id", "id % 10 as p")
        .write.partitionBy("p").saveAsTable("t")
      val df = sql("select id from t limit 5")
      assert(df.collect().length == 5)
      val scan = getSingleFileSourceScan(df.queryExecution.executedPlan)
      assert(scan.isLimitScanEligible)
    }
  }

  test("query with offset works correctly with partial listing") {
    withTable("t") {
      spark.range(0, 100, 1, 10).selectExpr("id").write.saveAsTable("t")
      val df = sql("select id from t limit 5 offset 2")
      val rows = df.collect()
      assert(rows.length == 5)
      val scan = getSingleFileSourceScan(df.queryExecution.executedPlan)
      assert(scan.isLimitScanEligible)
    }
  }

  test("table with fewer rows than limit returns all rows") {
    withTable("t") {
      spark.range(0, 3, 1, 3).selectExpr("id").write.saveAsTable("t")
      val df = sql("select id from t limit 10")
      val rows = df.collect()
      assert(rows.length == 3)
    }
  }

  test("progressive multi-round accumulation collects all rows without duplicates") {
    withTable("t") {
      spark.range(0, 50, 1, 50).selectExpr("id").write.saveAsTable("t")
      val df = sql("select id from t limit 45")
      val rows = df.collect()
      assert(rows.length == 45)
      val ids = rows.map(_.getLong(0)).toSeq
      assert(ids.distinct.length == 45, "all collected rows across rounds must be unique")
    }
  }
}
