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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SortMergeJoinRangeIndexSuite extends QueryTest with SharedSparkSession {

  private def smjConfigs: Seq[(String, String)] = Seq(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> "false",
    SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> "true",
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.SHUFFLE_PARTITIONS.key -> "4")

  private def collectSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] =
    plan.collect { case j: SortMergeJoinExec => j }

  test("sort merge point-in-range inner join") {
    val events = spark.range(0, 50, 1, 4)
      .selectExpr("id as e_id", "id * 2 as event_pt")
    val spans = spark.range(0, 50, 1, 4).selectExpr(
      "id as s_id",
      "id as lo",
      "(id + 5) as hi")
    val cond = events("e_id") === spans("s_id") &&
      events("event_pt") >= spans("lo") && events("event_pt") <= spans("hi")
    val joined = events.hint("merge").join(spans.hint("merge"), cond)
    val baseline = withSQLConf(
      smjConfigs :+ SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> "false": _*) {
      joined.collect()
    }
    withSQLConf(smjConfigs: _*) {
      assert(collectSortMergeJoin(joined.queryExecution.executedPlan).nonEmpty)
      checkAnswer(joined, baseline)
    }
  }

  test("sort merge range index survives spilled buffers") {
    val events = spark.range(0, 200, 1, 1)
      .selectExpr("id as e_id", "cast(id as int) as event_pt")
    val spans = spark.range(0, 200, 1, 1).selectExpr(
      "id as s_id",
      "cast(0 as int) as lo",
      "cast(1000 as int) as hi")
    val joined = events.join(spans, events("e_id") === spans("s_id") &&
      events("event_pt") >= spans("lo") && events("event_pt") <= spans("hi"))
    val spillConfigs = smjConfigs ++ Seq(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1")
    val baseline = withSQLConf(
      spillConfigs :+ SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> "false": _*) {
      joined.collect()
    }
    withSQLConf(spillConfigs: _*) {
      checkAnswer(joined, baseline)
    }
  }
}