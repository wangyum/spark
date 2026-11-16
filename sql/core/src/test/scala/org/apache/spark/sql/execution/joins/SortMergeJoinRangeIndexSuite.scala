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
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
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

  /**
   * Checks `joined` against a range-index-disabled baseline across codegen on and off.
   *
   * @param expectIndexed when true, assert that at least one equi-key group was actually
   *                      range-indexed (`numRangeIndexedGroups` metric > 0), so a regression
   *                      that silently disables indexing (while still producing correct
   *                      results via the linear-scan fallback) is caught.
   * @param expectFallback when true, assert that at least one group fell back to a linear scan
   *                       (`numRangeIndexFallbackGroups` metric > 0) and that no group was
   *                       range-indexed.
   */
  private def checkAgainstBaseline(
      joined: => DataFrame,
      configs: Seq[(String, String)],
      expectIndexed: Boolean = false,
      expectFallback: Boolean = false): Unit = {
    for (codegen <- Seq(true, false)) {
      val runConfigs = configs :+ (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString)
      val baseline = withSQLConf(
        runConfigs :+ SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> "false": _*) {
        joined.collect()
      }
      withSQLConf(runConfigs: _*) {
        // Evaluate the by-name `joined` exactly once: `checkAnswer` must execute the very same
        // plan instance inspected below, or the metrics read afterward belong to a different,
        // never-executed physical plan and stay at zero.
        val df = joined
        val plan = df.queryExecution.executedPlan
        val smj = collectSortMergeJoin(plan)
        assert(smj.nonEmpty, plan)
        assert(smj.forall(_.supportCodegen))
        checkAnswer(df, baseline)
        if (expectIndexed) {
          assert(smj.map(_.metrics("numRangeIndexedGroups").value).sum > 0,
            s"expected the range index to engage (numRangeIndexedGroups was 0):\n$plan")
        }
        if (expectFallback) {
          assert(smj.map(_.metrics("numRangeIndexFallbackGroups").value).sum > 0,
            s"expected a fallback to linear scan (numRangeIndexFallbackGroups was 0):\n$plan")
          assert(smj.map(_.metrics("numRangeIndexedGroups").value).sum == 0,
            s"expected no group to be range-indexed when exercising the fallback path:\n$plan")
        }
      }
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
    // Two equi-key groups of 25 rows each (above the default threshold of 16), forced to spill
    // by the tight buffer thresholds below, so this exercises a group that is large enough to
    // be index-eligible by row count but rejected specifically because it spilled -- unlike a
    // group of 1:1 unique keys, which would never have been a candidate for indexing at all and
    // so would trivially "fall back" without the spill rejection path ever firing. `hi` varies
    // per spans row (rather than being a single global literal) so the optimizer can't constant
    // -fold the range predicate into a filter-only condition that drops off the join entirely.
    val spans = spark.range(0, 50, 1, 1)
      .selectExpr("id % 2 as s_id", "cast(0 as int) as lo", "cast(1000 + id as int) as hi")
    val events = spark.range(0, 50, 1, 1)
      .selectExpr("id % 2 as e_id", "cast(id as int) as event_pt")
    val spillConfigs = smjConfigs ++ Seq(
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "1",
      SQLConf.SORT_MERGE_JOIN_EXEC_BUFFER_SPILL_THRESHOLD.key -> "1")
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"),
        events("e_id") === spans("s_id") &&
          events("event_pt") >= spans("lo") && events("event_pt") <= spans("hi")),
      spillConfigs,
      expectFallback = true)
  }

  test("sort merge point-in-range inner join engages index (buffered spans)") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"), cond), smjConfigs, expectIndexed = true)
  }

  test("sort merge point-in-range inner join engages index (buffered events)") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(
      spans.hint("merge").join(events.hint("merge"), cond), smjConfigs, expectIndexed = true)
  }

  test("sort merge point-in-range left outer join engages the index for a large group") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"), cond, "left"),
      smjConfigs,
      expectIndexed = true)
  }

  test("sort merge point-in-range right outer join engages the index for a large group") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(
      spans.hint("merge").join(events.hint("merge"), cond, "right"),
      smjConfigs,
      expectIndexed = true)
  }

  test("sort merge point-in-range left semi join engages the index for a large group") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    // Pass the join expression inline rather than through an intermediate val: `joined` is a
    // by-name parameter of checkAgainstBaseline, and a pre-built Dataset would memoize its
    // executedPlan (and the rangeProbeSpec lazily resolved from the FIRST conf under which the
    // plan was built) on first access, reusing the baseline (index-disabled) plan instance even
    // after the conf is flipped back to enabled.
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"), cond, "leftsemi"),
      smjConfigs,
      expectIndexed = true)
  }

  test("sort merge point-in-range left anti join engages the index for a large group") {
    val events = largeGroupMisses()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"), cond, "leftanti"),
      smjConfigs,
      expectIndexed = true)
  }

  test("sort merge existence join engages the index for a large group") {
    import testImplicits._
    // Built from independent Scala tuples (not SQL arithmetic sharing a common base column)
    // so the equi key and the range bounds can never be traced back to a shared attribute by
    // projection pushdown. A query where both derive from the same underlying column (e.g.
    // `e_id = id % n` and `event_pt` computed from the same `id`) correctly trips
    // RangeGroupProbe.createSpec's overlap guard once the correlated-subquery rewrite delays
    // the projection past the join, which is the scenario this shape avoids.
    val eventsData = for (k <- 0 until equiKeyBuckets; i <- 0 until largeGroupSize) yield
      (k.toLong, i.toLong * rangeSpanWidth + rangeSpanWidth / 2)
    val spansData = for (k <- 0 until equiKeyBuckets; i <- 0 until largeGroupSize) yield
      (k.toLong, i.toLong * rangeSpanWidth, i.toLong * rangeSpanWidth + rangeSpanWidth - 1)
    val events = eventsData.toDF("e_id", "event_pt")
    val spans = spansData.toDF("s_id", "lo", "hi")
    events.createOrReplaceTempView("rangeIndexExistenceEvents")
    spans.createOrReplaceTempView("rangeIndexExistenceSpans")
    // The OR keeps the EXISTS result as a boolean rather than collapsing it into a plain
    // filter, which is what makes the optimizer's subquery rewrite emit an ExistenceJoin
    // (as opposed to a LeftSemi) here.
    val existsQuery =
      """
        |SELECT * FROM rangeIndexExistenceEvents e WHERE
        |  EXISTS (SELECT 1 FROM rangeIndexExistenceSpans s WHERE e.e_id = s.s_id
        |          AND e.event_pt >= s.lo AND e.event_pt <= s.hi)
        |  OR e.e_id < 0
      """.stripMargin
    withSQLConf(smjConfigs: _*) {
      val plan = spark.sql(existsQuery).queryExecution.executedPlan
      assert(collectSortMergeJoin(plan).exists(_.joinType.isInstanceOf[ExistenceJoin]),
        s"expected the plan to use an ExistenceJoin sort-merge join:\n$plan")
    }
    checkAgainstBaseline(spark.sql(existsQuery), smjConfigs, expectIndexed = true)
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
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"), cond), smjConfigs, expectIndexed = true)
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
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"), cond), smjConfigs, expectIndexed = true)
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
    checkAgainstBaseline(
      events.hint("merge").join(spans.hint("merge"), cond), smjConfigs, expectIndexed = true)
  }

  test("sort merge join nodeName reports rangeIndex=true and stacks after skew=true") {
    val events = largeGroupEvents()
    val spans = largeGroupSpans()
    val cond = pointCond(events, spans)
    withSQLConf(smjConfigs: _*) {
      val joined = events.hint("merge").join(spans.hint("merge"), cond)
      val smj = collectSortMergeJoin(joined.queryExecution.executedPlan).head
      assert(smj.nodeName.contains("(rangeIndex=true)"),
        s"expected nodeName to report rangeIndex=true when the index is engaged: ${smj.nodeName}")

      val skewed = smj.copy(isSkewJoin = true)
      assert(skewed.nodeName.contains("(skew=true)") &&
        skewed.nodeName.contains("(rangeIndex=true)"))
      assert(skewed.nodeName.indexOf("(skew=true)") < skewed.nodeName.indexOf("(rangeIndex=true)"),
        s"expected (skew=true) to precede (rangeIndex=true): ${skewed.nodeName}")
    }
    withSQLConf(smjConfigs :+ SQLConf.SORT_MERGE_JOIN_RANGE_INDEX_ENABLED.key -> "false": _*) {
      val joined = events.hint("merge").join(spans.hint("merge"), cond)
      val smj = collectSortMergeJoin(joined.queryExecution.executedPlan).head
      assert(!smj.nodeName.contains("(rangeIndex=true)"),
        s"expected no rangeIndex suffix when the feature is disabled: ${smj.nodeName}")
    }
  }
}
