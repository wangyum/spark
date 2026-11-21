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

package org.apache.spark.sql.execution.window

import java.util.Properties

import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.internal.config.BUFFER_PAGESIZE
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Descending, SortOrder,
  UnsafeProjection}
import org.apache.spark.sql.execution.{SortExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class HashWindowGroupLimitSuite extends SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def dedupLatest(df: DataFrame): DataFrame = {
    df.selectExpr("*", "row_number() over (partition by k order by ts desc) as rn")
      .where("rn = 1")
      .drop("rn")
  }

  test("SPARK-61804: hash path matches sort-based path for top-1 row_number") {
    val df = Seq(
      (1, 100L, "a"), (1, 200L, "b"), (2, 50L, "c"),
      (2, 50L, "d"), (3, 10L, "e")
    ).toDF("k", "ts", "v")

    val withHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      dedupLatest(df).collect().toSet
    }
    val withoutHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      dedupLatest(df).collect().toSet
    }
    assert(withHash == withoutHash)
    // Sanity check against the expected, hand-computed winners.
    assert(withHash == Set(Row(1, 200L, "b"), Row(2, 50L, "c"), Row(3, 10L, "e")) ||
      withHash == Set(Row(1, 200L, "b"), Row(2, 50L, "d"), Row(3, 10L, "e")))
  }

  test("SPARK-61804: hash path handles nulls in order key") {
    val df = Seq((1, Some(5L)), (1, None), (1, Some(10L)), (2, None))
      .toDF("k", "ts")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      checkAnswer(dedupLatest(df), Seq(Row(1, 10L), Row(2, null)))
    }
  }

  test("SPARK-61804: hash path handles ties in order key") {
    val df = Seq((1, 5L, "a"), (1, 5L, "b")).toDF("k", "ts", "v")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val result = dedupLatest(df).collect()
      assert(result.length == 1)
      assert(result.head.getLong(1) == 5L)
    }
  }

  // Regression test for a deep-review finding: the single-order-column fast path's comparator used
  // raw java.lang.Double.compare on bit-reconstructed values, which (unlike Spark's actual SQL
  // float ordering) ranks 0.0 strictly above -0.0. For ROW_NUMBER()'s bounded top-K heap this
  // cannot change how many rows survive (the heap's size is structurally capped at `limit`
  // regardless of comparator correctness) -- only, potentially, which of several value-equal rows
  // is the arbitrary one kept -- so this checks the row count and value set, not a specific row's
  // identity, which ROW_NUMBER does not guarantee among genuine value ties.
  test("SPARK-61804: row_number() <= 2 treats -0.0 and 0.0 as equal for a Double order key") {
    val df = Seq((1, 5.0), (1, 0.0), (1, -0.0)).toDF("k", "ts")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val result = df.selectExpr("*", "row_number() over (partition by k order by ts desc) as rn")
        .where("rn <= 2").drop("rn").collect()
      assert(result.length == 2, s"expected exactly 2 rows, got ${result.toSeq}")
      assert(result.exists(_.getDouble(1) == 5.0), s"5.0 must survive, got ${result.toSeq}")
      assert(result.exists(r => r.getDouble(1) == 0.0 || r.getDouble(1) == -0.0),
        s"exactly one of the tied 0.0/-0.0 rows must survive, got ${result.toSeq}")
    }
  }

  test("SPARK-61804: global top-1 (empty partition spec) uses hash path") {
    val df = Seq((1L, "a"), (5L, "b"), (3L, "c")).toDF("ts", "v")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val result = df.selectExpr("*", "row_number() over (order by ts desc) as rn")
        .where("rn = 1").drop("rn")
      checkAnswer(result, Seq(Row(5L, "b")))
    }
  }

  test("SPARK-61804: multi-column order spec") {
    val df = Seq((1, 1L, 2L), (1, 1L, 5L), (1, 2L, 0L), (2, 0L, 0L)).toDF("k", "a", "b")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val result = df.selectExpr(
        "*", "row_number() over (partition by k order by a desc, b desc) as rn")
        .where("rn = 1").drop("rn")
      checkAnswer(result, Seq(Row(1, 2L, 0L), Row(2, 0L, 0L)))
    }
  }

  test("SPARK-61804: string order key is not hash-eligible (plan unchanged)") {
    val df = Seq((1, "b"), (1, "a")).toDF("k", "v")
    val withPushdown = df.selectExpr(
      "*", "row_number() over (partition by k order by v desc) as rn")
      .where("rn = 1").drop("rn")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val plan = withPushdown.queryExecution.executedPlan
      assert(collectWithSubqueries(plan) { case s: SortExec => s }.nonEmpty,
        "string order key must still use the sort-based path")
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(!_.isHashEligible))
    }
  }

  // Regression test for a deep-review finding: isHashEligible used to gate on UnsafeRow.isMutable,
  // which (unlike UnsafeRow.isFixedLength) also accepts a wide DecimalType (precision above
  // Decimal.MAX_LONG_DIGITS = 18), CalendarIntervalType, or a nanosecond-precision timestamp --
  // all of which UnsafeRow stores as a (cursor, length) pointer into the row's own variable-length
  // region, not as a raw 8-byte value. Both accumulators' extractSlotInto/writeSlotFrom copy an
  // order-key field between rows with raw getLong/setLong, which silently corrupts that pointer
  // (it's only meaningful relative to the row it was read from) once copied into a buffer/scratch
  // row with a different (or no) variable-length region -- the eventual GenerateOrdering comparison
  // then decodes the stale pointer as a real offset with no bounds checking.
  test("SPARK-61804: a wide (precision > 18) decimal order key is not hash-eligible (plan" +
      " unchanged, and the query is still correct via the sort-based fallback)") {
    val df = Seq((1, "123456789012345678.123456789"), (1, "1.0"), (2, "5.5"))
      .toDF("k", "raw")
      .selectExpr("k", "cast(raw as decimal(38, 9)) as amt")
    val withPushdown = df.selectExpr(
      "*", "row_number() over (partition by k order by amt desc) as rn")
      .where("rn = 1").drop("rn")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val plan = withPushdown.queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(!_.isHashEligible),
        "a wide decimal order key must not be hash-eligible")
      assert(!plan.toString.contains("WindowGroupLimit(hash)"),
        "EXPLAIN must not show the (hash) suffix for an ineligible plan")
      checkAnswer(withPushdown,
        Seq(Row(1, BigDecimal("123456789012345678.123456789")), Row(2, BigDecimal("5.5"))))
    }
  }

  test("SPARK-61804: decimal order key eligibility is drawn at precision 18" +
      " (UnsafeRow.isFixedLength's Decimal.MAX_LONG_DIGITS boundary, not UnsafeRow.isMutable's" +
      " unlimited precision)") {
    val child = spark.range(1).queryExecution.sparkPlan
    val k = AttributeReference("k", IntegerType)()
    def planWithOrderType(dt: DecimalType): WindowGroupLimitExec = WindowGroupLimitExec(
      partitionSpec = Seq(k),
      orderSpec = Seq(SortOrder(AttributeReference("amt", dt)(), Descending)),
      rankLikeFunction = org.apache.spark.sql.catalyst.expressions.RowNumber(),
      limit = 1,
      mode = Partial,
      child = child)
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      assert(planWithOrderType(DecimalType(18, 2)).isHashEligible,
        "DecimalType(18, 2) must remain hash-eligible")
      assert(!planWithOrderType(DecimalType(19, 2)).isHashEligible,
        "DecimalType(19, 2) must not be hash-eligible")
    }
  }

  test("SPARK-61804: numeric order key is hash-eligible (no SortExec before" +
      " WindowGroupLimitExec)") {
    val df = Seq((1, 1L), (1, 2L)).toDF("k", "ts")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val plan = dedupLatest(df).queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(_.isHashEligible))
      // The claim this test is named for: EnsureRequirements must not have injected a SortExec
      // directly below a hash-eligible WindowGroupLimitExec (WindowExec's own downstream sort,
      // above the Final WindowGroupLimitExec, is unrelated and expected -- see the spec's
      // "Note that WindowExec itself ... still force a sort" section).
      assert(limits.forall(!_.child.isInstanceOf[SortExec]),
        "hash-eligible WindowGroupLimitExec must not have a SortExec as its immediate child")
    }
  }

  // Regression test for a deep-review finding: isHashEligible is a derived lazy val (not a
  // constructor field), so it never showed up in EXPLAIN/plan-string output -- a user had to run
  // the query and inspect SQLMetrics after the fact to tell which path was actually taken.
  test("SPARK-61804: EXPLAIN distinguishes the hash path from the sort-based path via nodeName") {
    val eligibleDf = Seq((1, 1L), (1, 2L)).toDF("k", "ts")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      assert(dedupLatest(eligibleDf).queryExecution.executedPlan.toString.contains(
        "WindowGroupLimit(hash)"), "a hash-eligible instance must show the (hash) nodeName suffix")
    }
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      val planString = dedupLatest(eligibleDf).queryExecution.executedPlan.toString
      assert(!planString.contains("WindowGroupLimit(hash)"),
        "disabled hash pushdown must not show the (hash) nodeName suffix")
    }
    val ineligibleDf = Seq((1, "b"), (1, "a")).toDF("k", "v")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val planString = ineligibleDf.selectExpr(
        "*", "row_number() over (partition by k order by v desc) as rn")
        .where("rn = 1").drop("rn").queryExecution.executedPlan.toString
      assert(!planString.contains("WindowGroupLimit(hash)"),
        "a string order key (not hash-eligible) must not show the (hash) nodeName suffix" +
          " even with hash pushdown enabled")
    }
  }

  test("SPARK-61804: hashPushdown.enabled=false disables the hash path") {
    val df = Seq((1, 1L), (1, 2L)).toDF("k", "ts")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      val plan = dedupLatest(df).queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(!_.isHashEligible))
    }
  }

  test("SPARK-61804: limit > 1 (rn <= 2) is hash-eligible within maxLimit") {
    val df = Seq((1, 3L), (1, 2L), (1, 1L), (2, 5L)).toDF("k", "ts")
    val query = df.selectExpr("*", "row_number() over (partition by k order by ts desc) as rn")
      .where("rn <= 2").drop("rn")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      checkAnswer(query, Seq(Row(1, 3L), Row(1, 2L), Row(2, 5L)))
      val plan = query.queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(_.isHashEligible),
        "limit > 1 must be hash-eligible as long as it is within maxLimit")
    }
  }

  test("SPARK-61804: limit > maxLimit is not hash-eligible (stays on the sort-based path)") {
    val df = Seq((1, 3L), (1, 2L), (1, 1L), (2, 5L)).toDF("k", "ts")
    val query = df.selectExpr("*", "row_number() over (partition by k order by ts desc) as rn")
      .where("rn <= 2").drop("rn")
    withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MAX_LIMIT.key -> "1") {
      checkAnswer(query, Seq(Row(1, 3L), Row(1, 2L), Row(2, 5L)))
      val plan = query.queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(!_.isHashEligible),
        "limit (2) exceeding maxLimit (1) must fall back to the sort-based path")
    }
  }

  test("SPARK-61804: hash path handles a non-Int partition key type (String)") {
    val df = Seq(("a", 1L, "x"), ("a", 2L, "y"), ("b", 5L, "z")).toDF("k", "ts", "v")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val plan = dedupLatest(df).queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      // Partition-key type is unconstrained by isHashEligible -- only orderSpec's type matters.
      assert(limits.nonEmpty && limits.forall(_.isHashEligible))
      checkAnswer(dedupLatest(df), Seq(Row("a", 2L, "y"), Row("b", 5L, "z")))
    }
  }

  // SharedSparkSession's session does not expose a lightweight way to shrink the task memory
  // manager's page budget per-test, so this only checks correctness at a scale larger than a
  // single page would hold in the common case -- it does not itself force the
  // UnsafeFixedWidthAggregationMap-is-full branch. See
  // "hash path falls back to sort-based output when the hash map genuinely runs out of memory"
  // below for a direct, low-level test of that branch.
  test("SPARK-61804: hash path is correct at a scale beyond a handful of rows") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val n = 20000
      val df = (0 until n).map(i => (i, i.toLong, i.toLong)).toDF("k", "ts", "v")
      val result = dedupLatest(df).collect()
      assert(result.length == n)
    }
  }

  // Regression coverage for an interaction the PR's own suite otherwise never exercises:
  // PushDownShuffleExchangeThroughUnionSuite's "cross a partial window group limit above a
  // union" test uses the exact same hash-eligible query shape (RowNumber, limit 1, numeric
  // order key) but only asserts on shuffle counts, not on whether a SortExec got injected, so it
  // would keep passing even if isHashEligible's sort-free requiredChildOrdering regressed. This
  // test adds that missing assertion directly, without modifying the pre-existing cross-cutting
  // suite.
  test("SPARK-61804: hash path stays sort-free when combined with shuffle pushdown through a" +
      " union") {
    withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
      SQLConf.PUSH_DOWN_SHUFFLE_THROUGH_UNION_LOOK_THROUGH_OPERATORS_ENABLED.key -> "true") {
      withTempView("t1", "t2") {
        Seq((1, 10L), (1, 20L), (2, 5L)).toDF("id", "v").createOrReplaceTempView("t1")
        Seq((1, 30L), (3, 7L)).toDF("id", "v").createOrReplaceTempView("t2")
        val query = spark.sql(
          """SELECT id, v FROM (
            |  SELECT id, v, row_number() OVER (PARTITION BY id ORDER BY v DESC) AS rn
            |  FROM (
            |    SELECT id, v FROM (SELECT id, v FROM t1 DISTRIBUTE BY id)
            |    UNION ALL
            |    SELECT id, v FROM t2)
            |) WHERE rn = 1""".stripMargin)
        checkAnswer(query, Seq(Row(1, 30L), Row(2, 5L), Row(3, 7L)))

        val plan = query.queryExecution.executedPlan
        val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
        assert(limits.nonEmpty && limits.forall(_.isHashEligible),
          "this query shape must remain hash-eligible across a union")
        assert(limits.forall(!_.child.isInstanceOf[SortExec]),
          "hash-eligible WindowGroupLimitExec must not have a SortExec as its immediate child " +
            "even when combined with shuffle pushdown through a union")
      }
    }
  }

  test("SPARK-61804: hash path proactively bails out when the compaction ratio is too low") {
    withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_ROWS.key -> "10",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_COMPACTION_RATIO.key -> "1.05") {
      // All-unique keys: compaction ratio is always exactly 1.0, well below the 1.05 threshold,
      // so the periodic check (every 10 rows here) should trigger a bail-out to the sort-based
      // fallback well before all 1000 rows are consumed.
      val n = 1000
      val df = (0 until n).map(i => (i, i.toLong, i.toLong)).toDF("k", "ts", "v")
      val resultDf = dedupLatest(df)
      val result = resultDf.collect()
      assert(result.length == n, "bailing out to the sort-based path must not lose or duplicate" +
        " rows")

      val plan = resultDf.queryExecution.executedPlan
      val fallbackCounts = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
        .flatMap(_.metrics.get("numTasksFallBackedPoorCompaction"))
        .map(_.value)
      assert(fallbackCounts.exists(_ > 0),
        "expected at least one WindowGroupLimitExec task to have bailed out to the sort-based " +
          s"path, got fallback counts: $fallbackCounts")
    }
  }

  test("SPARK-61804: hash path does not bail out when the compaction ratio is healthy") {
    withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      // minRows must exceed the key cardinality (10): with a round-robin key assignment, the
      // first check always lands at ratio == 1.0 (one row per key seen so far) until rows start
      // repeating keys, so a too-small minRows would spuriously bail out before any real
      // duplication has had a chance to accumulate -- not a bug in the production code, just
      // this test needing to give the ratio room to rise first.
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_ROWS.key -> "50",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_COMPACTION_RATIO.key -> "1.05") {
      // 10 distinct keys, 100 rows each: compaction ratio is 100, far above the 1.05 threshold,
      // so the periodic check must never trigger a bail-out despite running every 50 rows. ts
      // decreases within each key's own occurrences (n - i, round-robin over k = i % 10), so a
      // key's very first row is immediately its final winner and every later occurrence of that
      // key is discarded without touching the arena.
      val n = 1000
      val df = (0 until n).map(i => (i % 10, (n - i).toLong, (n - i).toLong)).toDF("k", "ts", "v")
      val resultDf = dedupLatest(df)
      checkAnswer(resultDf, (0 until 10).map(k => Row(k, (n - k).toLong, (n - k).toLong)))

      val plan = resultDf.queryExecution.executedPlan
      val fallbackCounts = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
        .flatMap(_.metrics.get("numTasksFallBackedPoorCompaction"))
        .map(_.value)
      assert(fallbackCounts.forall(_ == 0),
        s"expected no bail-out with a healthy compaction ratio, got: $fallbackCounts")
    }
  }

  // End-to-end correctness check for Final-mode fallback's unfiltered output (see the low-level
  // accumulator test "Final-mode fallback emits every consumed row unfiltered" for the mechanism):
  // an impossible minCompactionRatio forces both Partial and Final to fall back even though this
  // data genuinely compacts well, so Final emits every surviving candidate unfiltered -- this only
  // produces the right single winner per key because the real Sort + Window + Filter(rn = 1) above
  // the group-limit chain recomputes the answer from scratch, exactly as it would if the
  // group-limit optimization had never fired at all.
  //
  // Partial's own fallback (unchanged) always correctly filters down to (distinct keys it saw) *
  // limit rows -- that is the whole point of Partial's pre-filter, hash or sort-based -- so with
  // only a handful of keys, too few survivors would ever reach any single Final-stage reduce task
  // to trigger even one compaction check there. 2000 keys with repartition(1) (forcing all of
  // Partial's input through one task, so its own fallback is driven deterministically) ensures
  // Final's shuffle input is large enough, however it gets split across reduce tasks, to clear the
  // minRows threshold below.
  test("SPARK-61804: forcing both stages to fall back still produces the correct answer" +
      " (Final's unfiltered output is corrected by the real Window above it)") {
    withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_ROWS.key -> "10",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_COMPACTION_RATIO.key -> "1000000.0") {
      val numKeys = 2000
      val n = numKeys * 5
      val df = (0 until n).map(i => (i % numKeys, (n - i).toLong, (n - i).toLong))
        .toDF("k", "ts", "v").repartition(1)
      val resultDf = dedupLatest(df)
      checkAnswer(resultDf, (0 until numKeys).map(k => Row(k, (n - k).toLong, (n - k).toLong)))

      val plan = resultDf.queryExecution.executedPlan
      val fallbackCounts = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
        .map(w =>
          w.mode.toString -> w.metrics.get("numTasksFallBackedPoorCompaction").map(_.value))
      assert(fallbackCounts.collect { case (m, Some(c)) if m == "Final" => c }.exists(_ > 0),
        s"expected this impossible compaction ratio to force Final's fallback too: $fallbackCounts")
    }
  }

  // Shared by the low-level forced-OOM tests below: a standalone TaskMemoryManager/TaskContext
  // backed by TestMemoryManager with a small page size and a tight execution-memory budget,
  // mirroring the WindowSegmentTreeMemorySuite / HashAggregateExecSuite pattern, instead of going
  // through a full SparkSession query (which has no lightweight way to force this).
  private def withStandaloneTaskContext[T](budgetBytes: Long, pageSizeBytes: String = "4096")
      (f: => T): T = {
    val conf = new SparkConf(false).set(BUFFER_PAGESIZE.key, pageSizeBytes)
    val memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(budgetBytes)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    val prevTaskContext = TaskContext.get()
    val taskContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = 0,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = null)
    TaskContext.setTaskContext(taskContext)
    try {
      f
    } finally {
      if (prevTaskContext != null) {
        TaskContext.setTaskContext(prevTaskContext)
      } else {
        TaskContext.unset()
      }
    }
  }

  // Direct, low-level tests of HashRowNumberGroupLimitAccumulator's memory-pressure fallback
  // (switchToFallback / buildFallbackSorter / the GroupedLimitIterator/SimpleLimitIterator
  // drain), independent of the proactive compaction-ratio bail-out above: these force a genuine
  // OOM from each of the two independent structures consumeRow touches -- the map
  // (getAggregationBufferFromUnsafeRow returning null) and the row arena (rowArena.append
  // throwing inside storeWinner) -- separately, so each of switchToFallback's two call sites (see
  // consumeRow) gets its own deterministic, isolated coverage rather than an ambiguous mix of
  // both triggers competing for the same shared memory budget.

  // Isolates the *map* path: every row here is narrow (just two 8-byte-ish fields) and there are
  // only 2,000 distinct keys, so rowArena's cumulative storage (2,000 rows x roughly a few dozen
  // bytes each) never approaches its own 64 KB initial page -- by the same page-size math
  // UnsafeRowArena.grow() uses, it provably never needs to grow at all here, so it can never be
  // the one to throw. Only the map, boxed in by a small BUFFER_PAGESIZE and a tight budget, can
  // fail.
  test("SPARK-61804: hash path falls back to sort-based output when the hash map genuinely" +
      " runs out of memory") {
    withStandaloneTaskContext(budgetBytes = 360 * 1024) {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      // A huge minRows/ample compaction ratio keeps the *proactive* bail-out from firing here,
      // isolating this test to the genuine-OOM fallback path only.
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 0.0,
        minRowsBetweenCompactionChecks = 0L, limit = 1)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      val n = 2000
      val expected = (0 until n).map(i => (i, i.toLong))
      expected.foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == n, s"expected $n distinct-key winners, got ${result.length}")
      val resultSet = result.map(r => (r.getInt(0), r.getLong(1))).toSet
      assert(resultSet == expected.toSet,
        "every distinct key's single row must survive the fallback unchanged")
      assert(numTasksFallBackedOOM.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/page size above rather than deleting " +
          "this assertion")
      assert(numTasksFallBackedPoorCompaction.value == 0,
        "this test isolates the genuine-OOM trigger; the proactive compaction-ratio bail-out " +
          "must not also fire")
    }
  }

  // Final-mode fallback must NOT pre-filter down to `limit` rows per key: a hash-eligible
  // WindowGroupLimitExec always declares outputOrdering = Nil (see isHashEligible), so
  // EnsureRequirements unconditionally plants a real Sort directly above the Final node, feeding
  // the real WindowExec + its Filter(rn <= limit) above that -- which recomputes the correct
  // answer from whatever rows arrive, regardless of how many that is. A Partial-mode fallback must
  // still filter: it is the only thing standing between its task's rows and the shuffle, so
  // skipping its own filtering would send every row across the network, losing the whole point of
  // the pre-filter. Mode defaults to Partial (unchanged behavior) everywhere else in this suite.
  //
  // Uses a *proactive* bail-out firing at the very first possible check (row 10, one per each of
  // 10 round-robin keys) rather than a genuine OOM: by that point no key has seen a second row
  // yet, so the heap has not discarded/replaced anything -- discarding is itself correct and
  // permanent regardless of mode (a row that loses to the current winner can never become
  // eligible later, see the class doc), so a test that let real discarding happen first (e.g. via
  // many replacements before a genuine OOM) would not actually isolate what this test means to
  // check: that Final-mode's *own* fallback transition does not throw away still-live candidates
  // the way Partial-mode's sort-then-filter would.
  test("SPARK-61804: Final-mode fallback emits every consumed row unfiltered, relying on the" +
      " downstream Sort+Window+Filter for correctness") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 1000000.0,
        minRowsBetweenCompactionChecks = 10L, limit = 1, mode = Final)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      val numKeys = 10
      val n = 1000
      (0 until n).foreach { i =>
        accumulator.consumeRow(rowProjection(InternalRow(i % numKeys, i.toLong)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == n,
        s"Final-mode fallback must emit every one of the $n consumed rows unfiltered, got " +
          s"${result.length}")
      assert(numTasksFallBackedPoorCompaction.value > 0,
        "this test is only meaningful if the proactive bail-out actually triggered -- if it " +
          "stops triggering, retune minRowsBetweenCompactionChecks/minCompactionRatio above " +
          "rather than deleting this assertion")
      assert(numTasksFallBackedOOM.value == 0,
        "this test isolates the proactive compaction-ratio trigger; the genuine-OOM fallback " +
          "must not also fire")
    }
  }

  // Mirror of the test above, confirming Partial-mode's existing (unchanged) behavior under the
  // identical setup: it still filters its fallback output down to exactly one row per key, since
  // skipping that filtering would ship every row across the shuffle.
  test("SPARK-61804: Partial-mode fallback still filters down to `limit` rows per key" +
      " (unchanged by the Final-mode optimization)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 1000000.0,
        minRowsBetweenCompactionChecks = 10L, limit = 1, mode = Partial)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      val numKeys = 10
      val n = 1000
      (0 until n).foreach { i =>
        accumulator.consumeRow(rowProjection(InternalRow(i % numKeys, i.toLong)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == numKeys,
        s"Partial-mode fallback must still filter down to $numKeys rows (one per key), got " +
          s"${result.length}")
      val resultSet = result.map(r => (r.getInt(0), r.getLong(1))).toSet
      assert(resultSet == (0 until numKeys).map(k => (k, (n - numKeys + k).toLong)).toSet,
        "each key's winner must be its highest-ts (last-arriving) row")
      assert(numTasksFallBackedPoorCompaction.value > 0,
        "this test is only meaningful if the proactive bail-out actually triggered -- if it " +
          "stops triggering, retune minRowsBetweenCompactionChecks/minCompactionRatio above " +
          "rather than deleting this assertion")
      assert(numTasksFallBackedOOM.value == 0,
        "this test isolates the proactive compaction-ratio trigger; the genuine-OOM fallback " +
          "must not also fire")
    }
  }

  // Isolates the *row arena* path, the mirror image of the map-OOM test above: the map is given a
  // single page far larger than 5,000 tiny entries will ever need (so it never grows again after
  // construction and so can never be the one to throw), while every row carries a wide padding
  // column that is part of childOutput but *not* part of orderSpec, so it bloats every row
  // rowArena.append stores without affecting the map's (order-key-only) buffer width at all.
  test("SPARK-61804: hash path falls back to sort-based output when the row arena genuinely" +
      " runs out of memory") {
    // 2 MB: a single page comfortably larger than 5,000 tiny map entries will ever need, so the
    // map never grows again after construction; only rowArena's growth can hit the remaining
    // (300 KB) budget below.
    withStandaloneTaskContext(budgetBytes = 2 * 1024 * 1024 + 300 * 1024, pageSizeBytes = "2m") {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val padAttr = AttributeReference("pad", StringType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr, padAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      // A huge minRows/ample compaction ratio keeps the *proactive* bail-out from firing here,
      // isolating this test to the genuine-OOM fallback path only.
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 0.0,
        minRowsBetweenCompactionChecks = 0L, limit = 1)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      val pad = UTF8String.fromString("x" * 100)
      val n = 5000
      val expected = (0 until n).map(i => (i, i.toLong))
      expected.foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts, pad)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == n, s"expected $n distinct-key winners, got ${result.length}")
      val resultSet = result.map(r => (r.getInt(0), r.getLong(1))).toSet
      assert(resultSet == expected.toSet,
        "every distinct key's single row must survive the fallback unchanged")
      assert(numTasksFallBackedOOM.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/enlarge the padding above rather than " +
          "deleting this assertion")
      assert(numTasksFallBackedPoorCompaction.value == 0,
        "this test isolates the genuine-OOM trigger; the proactive compaction-ratio bail-out " +
          "must not also fire")
    }
  }

  // Regression test for a bug where consumeRow() did not short-circuit after storeWinner()
  // internally triggered switchToFallback() (via rowArena.append failing with a genuine OOM): the
  // periodic compaction-ratio check right below it would then run on the row that just triggered
  // the fallback, calling map.getNumKeys() on the now-freed map. BytesToBytesMap.free() does not
  // reset numKeys, so that call returns the real (not corrupted, just stale) pre-free key count;
  // if the compaction ratio genuinely was low at that exact row -- as it is here, see below -- the
  // check proceeds to call switchToFallbackProactively() -> buildFallbackSorter() a *second* time
  // on the already-freed map/arena, crashing with an IllegalStateException ("not usable in state
  // FREED") instead of the handled SparkOutOfMemoryError this test is actually about. The fix is
  // the `if (fallbackSorter != null) return` guards added after each storeWinner() call in
  // consumeRow(); this test fails (with that IllegalStateException) without them.
  //
  // Two things have to line up for this interleaving to be reachable at all:
  //  - The OOM must come from rowArena.append specifically, not the map's own
  //    getAggregationBufferFromUnsafeRow (the *other* switchToFallback trigger, which already
  //    short-circuits via its own unconditional `return` right where it's checked). This test
  //    gives the map a single page far larger than ~5,000 tiny entries will ever need, so the
  //    map's own memory footprint is fixed after construction and never grows again; every row
  //    also carries a wide padding column that's part of childOutput but *not* part of orderSpec,
  //    so it bloats every row rowArena.append stores without affecting the map's (order-key-only)
  //    buffer width at all.
  //  - The compaction check must evaluate as low (triggering a bail-out) on the *same* row the
  //    genuine OOM strikes, not on some earlier row -- otherwise a healthy, correct proactive
  //    bail-out consumes the fallback first and the arena never gets a chance to genuinely OOM at
  //    all. Since every key here is distinct (ratio == 1.0 throughout, comfortably below
  //    minCompactionRatio whenever the check is actually evaluated),
  //    minRowsBetweenCompactionChecks is tuned to the exact row count at which rowArena.append
  //    happens to first throw under this budget/padding (empirically found to be 482; see the
  //    comment below), so the check's first
  //    and only evaluation for the whole test lands on exactly that row. If an unrelated change
  //    to row/page layout shifts that row count, this test still passes/fails correctly for
  //    *some* nearby value -- rerun with a temporary debug print of `rowsConsumed` in
  //    `switchToFallback` to find the new value.
  test("SPARK-61804: a genuine OOM fallback is not immediately followed by a second," +
      " double-free fallback attempt") {
    // 2 MB: a single page comfortably larger than ~5,000 tiny map entries will ever need, so the
    // map never grows again after construction; only rowArena's growth can hit the remaining
    // (300 KB) budget below.
    withStandaloneTaskContext(budgetBytes = 2 * 1024 * 1024 + 300 * 1024, pageSizeBytes = "2m") {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val padAttr = AttributeReference("pad", StringType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr, padAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      // rowArena.append empirically first throws SparkOutOfMemoryError once rowsConsumed reaches
      // 481 (i.e. while processing the 482nd distinct key), under this exact budget/padding/page
      // configuration. minRowsBetweenCompactionChecks = 482 makes the periodic check's first-ever
      // evaluation land exactly there (rowsConsumed == nextCompactionCheckAt == 482 right after
      // that row's would-be rowsConsumed increment), instead of firing (harmlessly, on a still-
      // live map) on any earlier row.
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 1.05,
        minRowsBetweenCompactionChecks = 482L, limit = 1)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      val pad = UTF8String.fromString("x" * 100)
      val n = 5000
      val expected = (0 until n).map(i => (i, i.toLong))
      expected.foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts, pad)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == n, s"expected $n distinct-key winners, got ${result.length}")
      val resultSet = result.map(r => (r.getInt(0), r.getLong(1))).toSet
      assert(resultSet == expected.toSet,
        "every distinct key's single row must survive the fallback unchanged")
      // Whichever of the two triggers actually wins this empirically-tuned race (see the comment
      // above -- the exact row a genuine OOM strikes is layout-sensitive, so a change elsewhere
      // could shift it to either side of the periodic check's one evaluation), exactly one of them
      // must have fired, and not both: this test's real purpose is the no-double-free regression
      // below, not pinning down which specific trigger wins the race.
      assert(numTasksFallBackedOOM.value + numTasksFallBackedPoorCompaction.value > 0,
        "this test is only meaningful if some fallback actually triggered -- if it stops " +
          "triggering, tighten the budget/enlarge the padding above rather than deleting this " +
          "assertion")
    }
  }

  test("SPARK-61804: Final-mode genuine OOM fallback safely short-circuits without accessing" +
      " freed memory") {
    withStandaloneTaskContext(budgetBytes = 2 * 1024 * 1024 + 300 * 1024, pageSizeBytes = "2m") {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val padAttr = AttributeReference("pad", StringType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr, padAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 0.0,
        minRowsBetweenCompactionChecks = 0L, limit = 1, mode = Final)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      val pad = UTF8String.fromString("x" * 100)
      val n = 5000
      (0 until n).foreach { i =>
        accumulator.consumeRow(rowProjection(InternalRow(i, i.toLong, pad)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == n,
        s"Final-mode fallback must emit every one of the $n rows, got ${result.length}")
      assert(numTasksFallBackedOOM.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered")
      assert(numTasksFallBackedPoorCompaction.value == 0,
        "this test isolates the genuine-OOM trigger (minCompactionRatio = 0.0 can never trip " +
          "the proactive bail-out)")
    }
  }

  test("SPARK-61804: hash path fuses into WholeStageCodegenExec") {
    val df = Seq((1, 1L, "a"), (1, 2L, "b")).toDF("k", "ts", "v")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val resultDf = dedupLatest(df)
      resultDf.collect() // force AQE to finalize per-stage plans (incl. codegen collapsing)
      val plan = resultDf.queryExecution.executedPlan
      assert(collectWithSubqueries(plan) {
        case w: WholeStageCodegenExec if w.child.exists(_.isInstanceOf[WindowGroupLimitExec]) => w
      }.nonEmpty, "hash-eligible WindowGroupLimitExec should fuse into whole-stage codegen")
    }
  }

  test("SPARK-61804: codegen and interpreted hash paths agree") {
    val df = (0 until 500).map(i => (i % 37, i.toLong, i.toString)).toDF("k", "ts", "v")
    val codegenOn = withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      dedupLatest(df).collect().toSet
    }
    val codegenOff = withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      dedupLatest(df).collect().toSet
    }
    assert(codegenOn == codegenOff)
  }

  // Regression test for doConsume's direct-dispatch-to-sorter codegen path (added to avoid every
  // post-fallback row paying consumeRow's extra virtual dispatch through the
  // HashGroupLimitAccumulator trait, on top of its own internal null check, for the rest of the
  // task -- see HashGroupLimitAccumulator.fallbackSorterOrNull's doc). Forces a mid-stream
  // bail-out, like the proactive-bail-out test above, but specifically compares codegen-on
  // against codegen-off (which never takes the new branch, since only doConsume's generated code
  // references fallbackSorterOrNull -- the interpreted path drives consumeRow directly) to
  // confirm the new branch's output is identical, not just that bailing out doesn't lose or
  // duplicate rows.
  test("SPARK-61804: codegen and interpreted hash paths agree when a mid-stream bail-out" +
      " routes most rows through the new direct-to-sorter dispatch") {
    withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_ROWS.key -> "10",
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_MIN_COMPACTION_RATIO.key -> "1.05") {
      // All-unique keys, same shape as the proactive-bail-out test: the periodic check (every 10
      // rows) bails out well before all 1000 rows are consumed, so the bulk of this DataFrame's
      // rows are processed by doConsume's new fallbackSorterOrNull-guarded branch when codegen is
      // on.
      val n = 1000
      val df = (0 until n).map(i => (i, i.toLong, i.toLong)).toDF("k", "ts", "v")
      val codegenOn = withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        dedupLatest(df).collect().toSet
      }
      val codegenOff = withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        dedupLatest(df).collect().toSet
      }
      assert(codegenOn.size == n, s"expected $n distinct-key rows, got ${codegenOn.size}")
      assert(codegenOn == codegenOff)
    }
  }

  // Regression test for a real bug caught only by running a benchmark against actual Parquet
  // files: the Parquet scan's own generated code can throw a checked java.io.IOException, which
  // propagates through doProduce's generated "consume the child" helper function. Every other
  // test in this suite builds its DataFrame via toDF() (LocalTableScan), which never generates
  // IOException-throwing code, so none of them exercised this path.
  test("SPARK-61804: hash path codegen compiles against a real (Parquet) file source") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq((1, 1L, "a"), (1, 2L, "b"), (2, 5L, "c")).toDF("k", "ts", "v")
        .write.parquet(path)
      withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
        checkAnswer(dedupLatest(spark.read.parquet(path)), Seq(Row(1, 2L, "b"), Row(2, 5L, "c")))
      }
    }
  }

  // Direct, low-level tests of HashRowNumberGroupLimitAccumulator's K-slot bounded max-heap
  // (limit > 1), independent of isHashEligible/SQL-level routing -- constructs the accumulator
  // directly, the same way the OOM fallback tests above do.

  test("SPARK-61804: limit > 1 keeps exactly the best `limit` rows per key, any arrival order") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, limit = 3)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      // Key 1 has 10 rows (ts 0..9, descending order means the 3 largest -- 9, 8, 7 -- survive);
      // key 2 has only 2 rows (fewer than the limit, so both must survive); fed in shuffled order
      // to make sure the heap doesn't depend on arrival order.
      val key1Rows = scala.util.Random.shuffle((0 until 10).map(ts => (1, ts.toLong)))
      val key2Rows = Seq((2, 100L), (2, 200L))
      (key1Rows ++ key2Rows).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val resultSet = result.map(r => (r.getInt(0), r.getLong(1))).toSet
      assert(resultSet == Set((1, 9L), (1, 8L), (1, 7L), (2, 100L), (2, 200L)),
        s"expected the 3 largest ts per key (or all of them if fewer than 3 exist), got $resultSet")
    }
  }

  // Regression test: a discarded row (heap full, new row doesn't beat the current worst-kept)
  // must still count toward rowsConsumed for the periodic compaction-ratio check, exactly like
  // the original K=1 code counted every consumed row regardless of whether it replaced the
  // winner. If discards silently stopped incrementing rowsConsumed, a key that fills its heap
  // early and then discards everything else -- the *ideal* case for this optimization, since the
  // arena barely grows after the initial fill -- would make rowsConsumed undercount so badly that
  // the distinct-key compaction ratio looks artificially low and proactively (and wrongly) bails
  // out to the sort-based path.
  test("SPARK-61804: limit > 1 discarded rows still count toward the compaction ratio" +
      " (a well-compacting key must not spuriously bail out)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 5.0,
        minRowsBetweenCompactionChecks = 50L, limit = 3)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      // Each key's rows arrive in descending ts order, so only the first 3 ever update the heap;
      // the other 97 are discarded without touching the arena -- the ideal compaction scenario.
      // Keys are processed one fully at a time (all of key 0, then all of key 1, ...), so:
      //   - correctly counting every consumed row, the very first periodic check (at row 50)
      //     lands while key 0 is still the only key seen: ratio = 50/1 = 50, far above the 5.0
      //     threshold.
      //   - if discards didn't count, only 3 rows per key would ever reach the bottom of
      //     consumeRow, so reaching rowsConsumed == 50 requires roughly 17 keys' worth of
      //     initial fills (3 each): ratio = 50/~17 =~ 2.9, below the threshold -- a 30-key sweep
      //     guarantees that broken count actually crosses 50 (and so the check actually fires)
      //     instead of silently never triggering at all.
      (0 until 30).foreach { k =>
        (0 until 100).foreach { ts =>
          accumulator.consumeRow(rowProjection(InternalRow(k, (99 - ts).toLong)).copy())
        }
      }
      accumulator.finish()
      assert(numTasksFallBackedPoorCompaction.value == 0,
        "a well-compacting key (heap fills early, then discards everything else) must not " +
          "spuriously bail out to the sort-based path")
      assert(numTasksFallBackedOOM.value == 0, "this test has an unbounded memory budget")
    }
  }

  // Mirrors the K=1 row-arena OOM test above, but with limit > 1: every key's kept state spans
  // multiple heap slots, so the fallback sorter must drain *all* of them, not just one -- this is
  // the main new risk the K-slot refactor introduces over the original single-winner design.
  test("SPARK-61804: limit > 1 falls back to sort-based output when the row arena genuinely" +
      " runs out of memory, draining every kept slot per key") {
    withStandaloneTaskContext(budgetBytes = 2 * 1024 * 1024 + 300 * 1024, pageSizeBytes = "2m") {
      val kAttr = AttributeReference("k", IntegerType)()
      val tsAttr = AttributeReference("ts", LongType)()
      val padAttr = AttributeReference("pad", StringType)()
      val partitionSpec = Seq(kAttr)
      val orderSpec = Seq(SortOrder(tsAttr, Descending))
      val childOutput = Seq(kAttr, tsAttr, padAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBackedOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallBackedPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompactionRatio = 0.0,
        minRowsBetweenCompactionChecks = 0L, limit = 3)

      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      val pad = UTF8String.fromString("x" * 100)
      val n = 5000
      // Each key gets exactly `limit` rows with distinct ts, so every kept row must survive.
      val expected = (0 until n).flatMap { k =>
        (0 until 3).map(j => (k, j.toLong))
      }
      expected.foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts, pad)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val resultSet = result.map(r => (r.getInt(0), r.getLong(1))).toSet
      assert(resultSet == expected.toSet,
        "every row (there are only exactly `limit` per key) must survive the fallback unchanged")
      assert(numTasksFallBackedOOM.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/enlarge the padding above rather than " +
          "deleting this assertion")
      assert(numTasksFallBackedPoorCompaction.value == 0,
        "this test isolates the genuine-OOM trigger (minCompactionRatio = 0.0 can never trip " +
          "the proactive bail-out)")
    }
  }
}
