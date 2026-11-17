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
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
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

  test("SPARK-61804: hashPushdown.enabled=false disables the hash path") {
    val df = Seq((1, 1L), (1, 2L)).toDF("k", "ts")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      val plan = dedupLatest(df).queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(!_.isHashEligible))
    }
  }

  test("SPARK-61804: limit > 1 (rn <= 2) is not hash-eligible (stays on the sort-based path)") {
    val df = Seq((1, 3L), (1, 2L), (1, 1L), (2, 5L)).toDF("k", "ts")
    val query = df.selectExpr("*", "row_number() over (partition by k order by ts desc) as rn")
      .where("rn <= 2").drop("rn")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      checkAnswer(query, Seq(Row(1, 3L), Row(1, 2L), Row(2, 5L)))
      val plan = query.queryExecution.executedPlan
      val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
      assert(limits.nonEmpty && limits.forall(!_.isHashEligible),
        "limit > 1 must never be treated as hash-eligible")
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
        .flatMap(_.metrics.get("numTasksFallBacked"))
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
      // so the periodic check must never trigger a bail-out despite running every 50 rows.
      val n = 1000
      val df = (0 until n).map(i => (i % 10, i.toLong, i.toLong)).toDF("k", "ts", "v")
      val resultDf = dedupLatest(df)
      checkAnswer(resultDf, (0 until 10).map(k => Row(k, (990 + k).toLong, (990 + k).toLong)))

      val plan = resultDf.queryExecution.executedPlan
      val fallbackCounts = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
        .flatMap(_.metrics.get("numTasksFallBacked"))
        .map(_.value)
      assert(fallbackCounts.forall(_ == 0),
        s"expected no bail-out with a healthy compaction ratio, got: $fallbackCounts")
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
      val numTasksFallBacked = SQLMetrics.createMetric(spark.sparkContext, "fallback")
      // A huge minRows/ample compaction ratio keeps the *proactive* bail-out from firing here,
      // isolating this test to the genuine-OOM fallback path only.
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBacked, minCompactionRatio = 0.0, minRowsBetweenCompactionChecks = 0L)

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
      assert(numTasksFallBacked.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/page size above rather than deleting " +
          "this assertion")
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
      val numTasksFallBacked = SQLMetrics.createMetric(spark.sparkContext, "fallback")
      // A huge minRows/ample compaction ratio keeps the *proactive* bail-out from firing here,
      // isolating this test to the genuine-OOM fallback path only.
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBacked, minCompactionRatio = 0.0, minRowsBetweenCompactionChecks = 0L)

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
      assert(numTasksFallBacked.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/enlarge the padding above rather than " +
          "deleting this assertion")
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
      val numTasksFallBacked = SQLMetrics.createMetric(spark.sparkContext, "fallback")
      // rowArena.append empirically first throws SparkOutOfMemoryError once rowsConsumed reaches
      // 481 (i.e. while processing the 482nd distinct key), under this exact budget/padding/page
      // configuration. minRowsBetweenCompactionChecks = 482 makes the periodic check's first-ever
      // evaluation land exactly there (rowsConsumed == nextCompactionCheckAt == 482 right after
      // that row's would-be rowsConsumed increment), instead of firing (harmlessly, on a still-
      // live map) on any earlier row.
      val accumulator = new HashRowNumberGroupLimitAccumulator(
        partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBacked, minCompactionRatio = 1.05, minRowsBetweenCompactionChecks = 482L)

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
      assert(numTasksFallBacked.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/enlarge the padding above rather than " +
          "deleting this assertion")
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
}
