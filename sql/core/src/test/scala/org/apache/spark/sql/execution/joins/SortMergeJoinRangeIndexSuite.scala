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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SortMergeJoinRangeIndexSuite extends QueryTest with SharedSparkSession {

  // Exceeds the default SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD (16), so a single equi-key
  // group of this size actually builds and probes a range index instead of falling back to
  // the n <= threshold linear-scan path exercised by the first (small) test below.
  private val largeGroupSize = 20
  // Distinct equi keys so the optimizer keeps a real equi join (constant keys become cartesian).
  private val equiKeyBuckets = 20
  private val rangeSpanWidth = 10

  private def smjConfigs: Seq[(String, String)] = Seq(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> "false",
    SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> "true",
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
    SQLConf.SHUFFLE_PARTITIONS.key -> "4")

  private def collectSortMergeJoin(plan: SparkPlan): Seq[SortMergeJoinExec] =
    plan.collect { case j: SortMergeJoinExec => j }

  /** Checks `joined` against a range-index-disabled baseline, under a real SortMergeJoinExec. */
  private def checkAgainstBaseline(joined: => DataFrame, configs: Seq[(String, String)]): Unit = {
    val baseline = withSQLConf(
      configs :+ SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> "false": _*) {
      joined.collect()
    }
    withSQLConf(configs: _*) {
      val plan = joined.queryExecution.executedPlan
      assert(collectSortMergeJoin(plan).nonEmpty, plan)
      checkAnswer(joined, baseline)
    }
  }

  // Each equi key has `largeGroupSize` non-overlapping [lo, hi] sub-ranges (benchmark shape).
  private def largeGroupSpans(): DataFrame =
    spark.range(equiKeyBuckets.toLong * largeGroupSize).selectExpr(
      s"id % $equiKeyBuckets as s_id",
      s"(id div $equiKeyBuckets) * $rangeSpanWidth as lo",
      s"(id div $equiKeyBuckets) * $rangeSpanWidth + ${rangeSpanWidth - 1} as hi")

  private def largeGroupEvents(): DataFrame =
    spark.range(equiKeyBuckets.toLong * largeGroupSize).selectExpr(
      s"id % $equiKeyBuckets as e_id",
      s"((id div $equiKeyBuckets) % $largeGroupSize) * $rangeSpanWidth + " +
        s"${rangeSpanWidth / 2} as event_pt")

  private def largeGroupMisses(): DataFrame =
    spark.range(equiKeyBuckets.toLong * largeGroupSize).selectExpr(
      s"id % $equiKeyBuckets as e_id",
      s"${largeGroupSize * rangeSpanWidth} + id as event_pt")

  private def pointCond(events: DataFrame, spans: DataFrame) =
    events("e_id") === spans("s_id") &&
      events("event_pt") >= spans("lo") && events("event_pt") <= spans("hi")

  test("sort merge range join keeps whole-stage codegen when range index is enabled") {
    val events = spark.range(0, 5, 1, 1)
      .selectExpr("id as e_id", "id as event_pt")
    val spans = spark.range(0, 5, 1, 1).selectExpr(
      "id as s_id", "id as lo", "(id + 10) as hi")
    val joined = events.hint("merge").join(spans.hint("merge"),
      events("e_id") === spans("s_id") &&
        events("event_pt") >= spans("lo") && events("event_pt") <= spans("hi"))
    withSQLConf(
      smjConfigs :+ SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true": _*) {
      val smj = collectSortMergeJoin(joined.queryExecution.executedPlan)
      assert(smj.nonEmpty)
      assert(smj.forall(_.supportCodegen))
    }
  }

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
    checkAgainstBaseline(joined, smjConfigs)
  }

  test("sort merge join falls back to a linear scan for a spilled buffered group") {
    // A single equi-key group far larger than SORT_MERGE_JOIN_RANGE_INDEX_THRESHOLD (16),
    // forced to actually spill to disk (in-memory/spill thresholds of 1 row): confirms the
    // index is skipped for a spilled group (see RangeGroupProbe.build's isSpillBacked gate)
    // and that the resulting linear-scan fallback still produces correct answers.
    val spans = spark.range(0, 200, 1, 1)
      .selectExpr("id as s_id", "cast(0 as int) as lo", "cast(1000 as int) as hi")
    val events = spark.range(0, 200, 1, 1)
      .selectExpr("id as e_id", "cast(id as int) as event_pt")
    val joined = events.hint("merge").join(spans.hint("merge"),
      events("e_id") === spans("s_id") &&
        events("event_pt") >= spans("lo") && events("event_pt") <= spans("hi"))
    val spillConfigs = smjConfigs ++ Seq(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1")
    checkAgainstBaseline(joined, spillConfigs)
  }

  test("sort merge point-in-range inner join engages the index for a large group") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(events.hint("merge").join(spans.hint("merge"), cond), smjConfigs)
  }

  test("sort merge point-in-range left outer join engages the index for a large group") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(events.hint("merge").join(spans.hint("merge"), cond, "left"), smjConfigs)
  }

  test("sort merge point-in-range right outer join engages the index for a large group") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(spans.hint("merge").join(events.hint("merge"), cond, "right"), smjConfigs)
  }

  test("sort merge point-in-range left semi join engages the index for a large group") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(events.hint("merge").join(spans.hint("merge"), cond, "leftsemi"), smjConfigs)
  }

  test("sort merge point-in-range left anti join engages the index for a large group") {
    val events = largeGroupMisses()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(events.hint("merge").join(spans.hint("merge"), cond, "leftanti"), smjConfigs)
  }

  test("sort merge interval overlap inner join engages the index for a large group") {
    val n = equiKeyBuckets.toLong * largeGroupSize
    val spans = spark.range(n).selectExpr(
      s"id % $equiKeyBuckets as s_id",
      s"(id div $equiKeyBuckets) * $rangeSpanWidth as blo",
      s"(id div $equiKeyBuckets) * $rangeSpanWidth + 15 as bhi")
    val events = spark.range(n).selectExpr(
      s"id % $equiKeyBuckets as e_id",
      s"(id div $equiKeyBuckets) * $rangeSpanWidth as alo",
      s"(id div $equiKeyBuckets) * $rangeSpanWidth + 15 as ahi")
    val cond = events("e_id") === spans("s_id") &&
      events("alo") <= spans("bhi") && spans("blo") <= events("ahi")
    checkAgainstBaseline(events.hint("merge").join(spans.hint("merge"), cond), smjConfigs)
  }

  test("sort merge partial < inner join engages the index for a large group") {
    val n = equiKeyBuckets.toLong * largeGroupSize
    val spans = spark.range(n).selectExpr(
      s"id % $equiKeyBuckets as s_id",
      s"id div $equiKeyBuckets as s_b")
    val events = spark.range(n).selectExpr(
      s"id % $equiKeyBuckets as e_id",
      s"(id div $equiKeyBuckets) - 1 as e_a")
    val cond = events("e_id") === spans("s_id") && events("e_a") < spans("s_b")
    checkAgainstBaseline(events.hint("merge").join(spans.hint("merge"), cond), smjConfigs)
  }

  test("sort merge partial > inner join engages the index for a large group") {
    val n = equiKeyBuckets.toLong * largeGroupSize
    val spans = spark.range(n).selectExpr(
      s"id % $equiKeyBuckets as s_id",
      s"id div $equiKeyBuckets as s_b")
    val events = spark.range(n).selectExpr(
      s"id % $equiKeyBuckets as e_id",
      s"(id div $equiKeyBuckets) + 1 as e_a")
    val cond = events("e_id") === spans("s_id") && events("e_a") > spans("s_b")
    checkAgainstBaseline(events.hint("merge").join(spans.hint("merge"), cond), smjConfigs)
  }
}
