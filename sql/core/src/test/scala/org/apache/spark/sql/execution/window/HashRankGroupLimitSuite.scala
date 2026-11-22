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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference,
  DenseRank, Descending, Expression, Rank, SortOrder, UnsafeProjection}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{Decimal, DecimalType, DoubleType, FloatType, IntegerType,
  LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests [[HashRankGroupLimitAccumulator]] both directly (low-level, independent of
 * isHashEligible/SQL-level routing, the same way [[HashWindowGroupLimitSuite]]'s low-level tests
 * do) and through full SQL queries exercising [[WindowGroupLimitExec]]'s hash-eligible routing
 * for `RANK()`/`DENSE_RANK()`.
 */
class HashRankGroupLimitSuite extends SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

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

  private val kAttr = AttributeReference("k", IntegerType)()
  private val tsAttr = AttributeReference("ts", LongType)()
  private val partitionSpec = Seq(kAttr)
  private val orderSpec = Seq(SortOrder(tsAttr, Descending))
  private val childOutput = Seq(kAttr, tsAttr)

  private def newAccumulator(
      rankLikeFunction: Expression,
      limit: Int,
      minCompaction: Double = 0.0,
      minRowsBetweenCompactionChecks: Long = 0L,
      // Defaulted to the suite-level single-column fields so every existing call site is
      // unaffected; a multi-column test overrides both to exercise the generic (numOrderCols > 1)
      // comparison/shift branch instead of the single-column fast path.
      customOrderSpec: Seq[SortOrder] = orderSpec,
      customChildOutput: Seq[Attribute] = childOutput): HashRankGroupLimitAccumulator = {
    val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
    val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
    val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
    val numTasksFallbackOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
    val numTasksFallbackPoorCompaction =
      SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
    new HashRankGroupLimitAccumulator(
      partitionSpec, customOrderSpec, rankLikeFunction, customChildOutput, numOutputRows,
      peakMemory, spillSize, numTasksFallbackOOM, numTasksFallbackPoorCompaction, minCompaction,
      minRowsBetweenCompactionChecks, enableRadixSort = true, limit = limit)
  }

  // Regression test for a deep-review finding: finishAccumulator previously only updated the
  // SQL-plan-local peakMemory SQLMetric, never TaskContext.taskMetrics().peakExecutionMemory --
  // the field the Stage/Executors UI, REST API, and any SparkListener.onTaskEnd consumer actually
  // read, unlike every sibling memory-consuming operator (SortExec, HashAggregateExec,
  // BroadcastHashJoinExec). See HashWindowGroupLimitSuite's identically-purposed test.
  // Constructed directly (not via newAccumulator()) so the test has its own handle on peakMemory
  // to compare against -- newAccumulator() builds that SQLMetric as a local val and never returns
  // it, which would leave this test unable to tell "the real peak" from "some other positive
  // value" and only able to assert > 0, unlike HashWindowGroupLimitSuite's identically-purposed
  // test (which does have that handle and asserts == alongside > 0).
  test("SPARK-61804 (rank hash pushdown): finishAccumulator reports peak memory to TaskMetrics," +
      " not just the SQL plan's own SQLMetric") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallbackOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallbackPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRankGroupLimitAccumulator(
        partitionSpec, orderSpec, new Rank(), childOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallbackOOM, numTasksFallbackPoorCompaction, minCompaction = 0.0,
        minRowsBetweenCompactionChecks = 0L, enableRadixSort = true, limit = 2)

      assert(TaskContext.get().taskMetrics().peakExecutionMemory == 0,
        "test setup assumption: nothing else has reported peak execution memory yet")
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      (0 until 100).foreach { i =>
        accumulator.consumeRow(rowProjection(InternalRow(i, i.toLong)).copy())
      }
      accumulator.finish().map(_.copy()).toSeq

      assert(TaskContext.get().taskMetrics().peakExecutionMemory == peakMemory.value,
        "finishAccumulator must feed its SQLMetric's peak value into TaskMetrics, mirroring " +
          "SortExec/HashAggregateExec/BroadcastHashJoinExec")
      assert(TaskContext.get().taskMetrics().peakExecutionMemory > 0,
        "the map/arena must have reported a nonzero peak for 100 distinct-key rows")
    }
  }

  test("SPARK-61804 (rank hash pushdown): DENSE_RANK() <= 2 keeps the first 2 distinct values" +
      " (all ties), regardless of tie-group size") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val accumulator = newAccumulator(new DenseRank(), limit = 2)
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      Seq((1, 10L), (1, 10L), (1, 10L), (1, 5L), (1, 1L)).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getLong(1)).sorted
      // dense_rank(10)=1, dense_rank(5)=2, dense_rank(1)=3 -- keeps every row at ts=10 or ts=5.
      assert(tsValues == Seq(5L, 10L, 10L, 10L),
        s"DENSE_RANK() <= 2 must keep all ties at the 2 smallest distinct values, got $tsValues")
    }
  }

  test("SPARK-61804 (rank hash pushdown): RANK() <= 2 excludes a group whose true rank" +
      " (accounting for preceding ties) exceeds the limit, even though it's the 2nd" +
      " distinct value") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val accumulator = newAccumulator(new Rank(), limit = 2)
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      Seq((1, 10L), (1, 10L), (1, 10L), (1, 5L), (1, 1L)).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getLong(1)).sorted
      // rank(10)=1 (3 rows); rank(5)=1+3=4 > 2, so the ts=5 group -- and ts=1 -- are excluded,
      // even though ts=5 is only the *2nd distinct value*: this is exactly the RANK vs DENSE_RANK
      // divergence the design review caught.
      assert(tsValues == Seq(10L, 10L, 10L),
        s"RANK() <= 2 must exclude ts=5 here since 3 rows of ts=10 precede it, got $tsValues")
    }
  }

  // Regression test for a deep-review finding: the admission test for a brand-new distinct value
  // only runs once, using whatever weight its better-ranked rivals have accumulated *so far*. If a
  // tie for an already-admitted, better slot arrives *after* a worse value has already been locked
  // into a slot, that worse slot's true rank can grow past the limit without ever being
  // re-validated or evicted -- unlike the suite's other RANK() test above, which (by feeding every
  // tie for the better group before the worse value ever arrives) never exercises this ordering-
  // sensitive gap.
  test("SPARK-61804 (rank hash pushdown): RANK() re-validates an already-admitted slot when a" +
      " later-arriving tie for a better slot pushes its true rank past the limit") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val accumulator = newAccumulator(new Rank(), limit = 2)
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      // ts=10 admitted into slot0; ts=5 admitted into slot1 (only 1 row of ts=10 counted so far,
      // so RANK(5) looks like 2 <= 2 at that instant); the second ts=10 arrives last, tying
      // slot0 -- its true RANK is now 1, but ts=5's true RANK is now 1+2=3 > 2, so ts=5 must be
      // evicted even though no *new* distinct value ever arrives to trigger re-evaluation.
      Seq((1, 10L), (1, 5L), (1, 10L)).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getLong(1)).sorted
      assert(tsValues == Seq(10L, 10L),
        s"RANK() <= 2 must exclude ts=5 once the second ts=10 tie raises its true rank to 3," +
          s" got $tsValues")
    }
  }

  // A second, independent way the same re-validation gap can be triggered: inserting a brand-new
  // (not tied) better value shifts every slot after it, which can push an already-admitted,
  // non-overflow slot's true rank past the limit without the array ever reaching capacity -- a
  // case the class doc's original "inserting a new slot can invalidate at most the single
  // trailing slot" claim did not account for.
  test("SPARK-61804 (rank hash pushdown): RANK() re-validates trailing slots when a brand-new" +
      " better value shifts their preceding weight past the limit, even without reaching" +
      " capacity") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val accumulator = newAccumulator(new Rank(), limit = 3)
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      // 5,5 admits ts=5 (weight 2); 3 admits ts=3 (preceding weight 2 < 3) -- slots: [(5,2),(3,1)],
      // slotCount=2, well under the limit-3 capacity. ts=7 then arrives, better than everything,
      // and is inserted at position 0, shifting both existing slots right by one: ts=5's new
      // preceding weight is 1 (just ts=7), still valid, but ts=3's new preceding weight becomes
      // 1 (ts=7) + 2 (ts=5) = 3, which is no longer < limit (3) -- even though slotCount (3 after
      // the insert) never exceeded the limit-3 capacity, so the old "only a capacity overflow can
      // invalidate a slot" reasoning would have missed this.
      // A final ts=4 then arrives: its admission check (sumWeights against the shifted slot1,
      // which now holds ts=5's data copied over from the old slot0 by insertSlotAt's copySlot)
      // must see the correctly-carried-forward weight of 2, not a stale/zero value -- making this
      // also a regression test for copySlot's rowCount-copying correctness, independent of the
      // revalidateTrailingSlots fix this test was originally written for.
      Seq((1, 5L), (1, 5L), (1, 3L), (1, 7L), (1, 4L)).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getLong(1)).sorted
      assert(tsValues == Seq(5L, 5L, 7L),
        s"RANK() <= 3 must exclude ts=3 (rank 4) and ts=4 (rank 4) once ts=7's insertion and" +
          s" copySlot's carried-forward weight are both accounted for, got $tsValues")
    }
  }

  test("SPARK-61804 (rank hash pushdown): fewer distinct values than the limit keeps everything" +
      " (both RANK and DENSE_RANK)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      Seq(new Rank(), new DenseRank()).foreach { fn =>
        val accumulator = newAccumulator(fn, limit = 10)
        val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
        Seq((1, 10L), (1, 10L), (1, 5L), (1, 1L)).foreach { case (k, ts) =>
          accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
        }
        val result = accumulator.finish().map(_.copy()).toSeq
        assert(result.map(_.getLong(1)).sorted == Seq(1L, 5L, 10L, 10L),
          s"with only 3 distinct values and limit=10, every row must survive for $fn")
      }
    }
  }

  // Mirrors HashWindowGroupLimitSuite's "hash path handles nulls in order key" test, for the new
  // Rank/DenseRank accumulator: extractSlotInto/writeSlotValue have explicit isNullAt/setNullAt
  // branches that were otherwise completely untested in this suite. The suite's orderSpec is
  // Descending, whose default null ordering is NULLS LAST, so a null order key is always the
  // worst-ranked row regardless of function.
  test("SPARK-61804 (rank hash pushdown): DENSE_RANK() <= 2 handles a null order key (sorts" +
      " last under this suite's Descending orderSpec, so it is excluded)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val accumulator = newAccumulator(new DenseRank(), limit = 2)
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      Seq((1, null), (1, 10L), (1, 10L), (1, 5L)).foreach {
        case (k: Int, ts) =>
          accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getLong(1)).sorted
      // dense_rank(10)=1, dense_rank(5)=2 (both <= 2); the null row sorts last (dense_rank=3)
      // and is excluded.
      assert(tsValues == Seq(5L, 10L, 10L),
        s"DENSE_RANK() <= 2 must exclude the null order key here, got $tsValues")
    }
  }

  test("SPARK-61804 (rank hash pushdown): RANK() <= 2 handles a null order key (sorts last" +
      " under this suite's Descending orderSpec, so it is excluded along with ts=5)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val accumulator = newAccumulator(new Rank(), limit = 2)
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      Seq((1, null), (1, 10L), (1, 10L), (1, 5L)).foreach {
        case (k: Int, ts) =>
          accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getLong(1)).sorted
      // rank(10)=1 (2 rows); rank(5)=1+2=3 > 2 and the null row's rank=4 > 2, so both are
      // excluded, leaving only the two ts=10 rows.
      assert(tsValues == Seq(10L, 10L),
        s"RANK() <= 2 must exclude both ts=5 and the null order key here, got $tsValues")
    }
  }

  test("SPARK-61804 (rank hash pushdown): a late-arriving better value evicts the current" +
      " worst-tracked value, discarding its rows from the output") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val accumulator = newAccumulator(new DenseRank(), limit = 2)
      val rowProjection = UnsafeProjection.create(childOutput.map(_.dataType).toArray)
      // Arrival order: 5, 3 (heap fills with {5, 3}), then 10 arrives (better than both) --
      // 3 must be evicted since only the top-2 distinct values survive: {10, 5}.
      Seq((1, 5L), (1, 3L), (1, 10L)).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.map(_.getLong(1)).sorted == Seq(5L, 10L),
        s"ts=3 should have been evicted once ts=10 arrived, got ${result.map(_.getLong(1))}")
    }
  }

  test("SPARK-61804 (rank hash pushdown): falls back to sort-based output when the row arena" +
      " genuinely runs out of memory, and the fallback still applies RANK() semantics") {
    withStandaloneTaskContext(budgetBytes = 2 * 1024 * 1024 + 300 * 1024, pageSizeBytes = "2m") {
      val padAttr = AttributeReference("pad", StringType)()
      val wideChildOutput = Seq(kAttr, tsAttr, padAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallbackOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallbackPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRankGroupLimitAccumulator(
        partitionSpec, orderSpec, new Rank(), wideChildOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallbackOOM, numTasksFallbackPoorCompaction, minCompaction = 0.0,
        minRowsBetweenCompactionChecks = 0L, enableRadixSort = true, limit = 1)

      val rowProjection = UnsafeProjection.create(wideChildOutput.map(_.dataType).toArray)
      val pad = UTF8String.fromString("x" * 100)
      val n = 5000
      // Each key has a single distinct ts (no ties), so RANK() <= 1 keeps exactly one row per key
      // regardless of fallback -- mirrors the K=1 ROW_NUMBER OOM test's shape.
      val expected = (0 until n).map(k => (k, k.toLong))
      expected.foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts, pad)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val resultSet = result.map(r => (r.getInt(0), r.getLong(1))).toSet
      assert(resultSet == expected.toSet,
        "every key's single row must survive the fallback unchanged")
      assert(numTasksFallbackOOM.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/enlarge the padding above rather than " +
          "deleting this assertion")
      assert(numTasksFallbackPoorCompaction.value == 0,
        "this test isolates the genuine-OOM trigger (minCompaction = 0.0 can never trip " +
          "the proactive bail-out)")
    }
  }

  test("SPARK-61804 (rank hash pushdown): Final-mode genuine OOM fallback safely short-circuits" +
      " without accessing freed memory") {
    withStandaloneTaskContext(budgetBytes = 2 * 1024 * 1024 + 300 * 1024, pageSizeBytes = "2m") {
      val padAttr = AttributeReference("pad", StringType)()
      val wideChildOutput = Seq(kAttr, tsAttr, padAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallbackOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
      val numTasksFallbackPoorCompaction =
        SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
      val accumulator = new HashRankGroupLimitAccumulator(
        partitionSpec, orderSpec, new Rank(), wideChildOutput, numOutputRows, peakMemory,
        spillSize, numTasksFallbackOOM, numTasksFallbackPoorCompaction,
        minCompaction = 0.0, minRowsBetweenCompactionChecks = 0L, enableRadixSort = true,
        limit = 1, mode = Final)

      val rowProjection = UnsafeProjection.create(wideChildOutput.map(_.dataType).toArray)
      val pad = UTF8String.fromString("x" * 100)
      val n = 5000
      (0 until n).foreach { k =>
        accumulator.consumeRow(rowProjection(InternalRow(k, k.toLong, pad)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == n,
        s"Final-mode fallback must emit every one of the $n rows, got ${result.length}")
      assert(numTasksFallbackOOM.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered")
      assert(numTasksFallbackPoorCompaction.value == 0,
        "this test isolates the genuine-OOM trigger (minCompaction = 0.0 can never trip " +
          "the proactive bail-out)")
    }
  }

  // Regression test for a deep-review finding: every other test in this suite uses the
  // suite-level single-column `orderSpec` (`ts desc`), so `numOrderCols == 1` always held and
  // every slot comparison/shift went through the single-column fast path
  // (compareSlotSingle/insertSlotAtSingle). The generic multi-column branch (extractSlotInto +
  // boundOrdering.compare for comparisons, copySlot's per-field loop -- including its
  // isDenseRank row-count-copy gating -- for slot shifts) that RANK()/DENSE_RANK() ORDER BY 2+
  // columns actually uses was never exercised by any test. Driven directly against the
  // accumulator (not via SQL) with a hand-traced arrival order, mirroring this suite's other
  // low-level tests, so the slot shift this is meant to cover is guaranteed to happen rather than
  // left to shuffle-dependent chance.
  test("SPARK-61804 (rank hash pushdown): multi-column order spec exercises the generic" +
      " comparison/shift path, not just the single-column fast path") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val aAttr = AttributeReference("a", LongType)()
      val bAttr = AttributeReference("b", LongType)()
      val multiOrderSpec = Seq(SortOrder(aAttr, Descending), SortOrder(bAttr, Ascending))
      val multiChildOutput = Seq(kAttr, aAttr, bAttr)
      val accumulator = newAccumulator(
        new Rank(), limit = 2, customOrderSpec = multiOrderSpec,
        customChildOutput = multiChildOutput)
      val rowProjection = UnsafeProjection.create(multiChildOutput.map(_.dataType).toArray)
      // Sorted by (a desc, b asc): (2,0), (2,9), (1,2), (1,5) -- a=2's two rows share the same
      // first column but are genuinely distinct order-key values, only distinguishable via b; a
      // comparator that dropped the second column would treat them as equal (one slot instead of
      // two), silently changing which rows RANK()/DENSE_RANK() admit.
      Seq((1, 1L, 5L), (1, 1L, 2L), (1, 2L, 0L), (1, 2L, 9L)).foreach { case (k, a, b) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, a, b)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val pairs = result.map(r => (r.getLong(1), r.getLong(2))).sorted
      // rank(2,0)=1, rank(2,9)=2, rank(1,2)=3 (excluded), rank(1,5)=4 (excluded): RANK() <= 2
      // keeps only the a=2 group, and only because b correctly ordered (2,0) ahead of (2,9).
      assert(pairs == Seq((2L, 0L), (2L, 9L)),
        s"RANK() <= 2 over (a desc, b asc) must keep exactly the a=2 group, got $pairs")
    }
  }

  // SQL-level tests exercising the full isHashEligible routing (optimizer -> WindowGroupLimitExec
  // -> HashRankGroupLimitEvaluatorFactory), not just the accumulator directly.

  private def topKByRank(df: DataFrame, k: Int) =
    df.selectExpr("*", s"rank() over (partition by key order by ts desc) as rk")
      .where(s"rk <= $k").drop("rk")

  private def topKByDenseRank(df: DataFrame, k: Int) =
    df.selectExpr("*", s"dense_rank() over (partition by key order by ts desc) as rk")
      .where(s"rk <= $k").drop("rk")

  test("SPARK-61804 (rank hash pushdown): RANK() <= 2 hash path matches sort-based path," +
      " including a tie group wider than the limit") {
    val df = Seq((1, 10L), (1, 10L), (1, 10L), (1, 5L), (1, 1L), (2, 7L))
      .toDF("key", "ts")
    val withHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      topKByRank(df, 2).collect().toSet
    }
    val withoutHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      topKByRank(df, 2).collect().toSet
    }
    assert(withHash == withoutHash)
    // rank(10)=1 (3 rows); rank(5)=4 > 2 so it (and ts=1) are excluded; key 2's single row always
    // has rank 1.
    assert(withHash == Set(Row(1, 10L), Row(1, 10L), Row(1, 10L), Row(2, 7L)))
  }

  test("SPARK-61804 (rank hash pushdown): DENSE_RANK() <= 2 hash path matches sort-based path," +
      " keeping every tied row across the 2 smallest distinct values") {
    val df = Seq((1, 10L), (1, 10L), (1, 10L), (1, 5L), (1, 1L), (2, 7L))
      .toDF("key", "ts")
    val withHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      topKByDenseRank(df, 2).collect().toSet
    }
    val withoutHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      topKByDenseRank(df, 2).collect().toSet
    }
    assert(withHash == withoutHash)
    // dense_rank(10)=1, dense_rank(5)=2 (both <= 2); dense_rank(1)=3 excluded.
    assert(withHash == Set(Row(1, 10L), Row(1, 10L), Row(1, 10L), Row(1, 5L), Row(2, 7L)))
  }

  // Regression test for a deep-review finding: every SQL-level test in this suite orders by a
  // single column (`ts desc`), unlike HashWindowGroupLimitSuite's own "multi-column order spec"
  // test for ROW_NUMBER(). Compares the full hash-eligible routing (optimizer -> isHashEligible ->
  // HashRankGroupLimitEvaluatorFactory) against the real sort-based baseline for both RANK() and
  // DENSE_RANK() over a 2-column order spec with a genuine ordering dependency on the second
  // column: key 1's two `a=2` rows are only correctly ordered relative to each other via `b`.
  test("SPARK-61804 (rank hash pushdown): RANK()/DENSE_RANK() hash path matches sort-based path" +
      " for a multi-column order spec") {
    val df = Seq((1, 1L, 5L), (1, 1L, 2L), (1, 2L, 0L), (1, 2L, 9L), (2, 0L, 0L))
      .toDF("key", "a", "b")
    def topK(func: String, k: Int) =
      df.selectExpr("*", s"$func() over (partition by key order by a desc, b asc) as rk")
        .where(s"rk <= $k").drop("rk")

    Seq("rank", "dense_rank").foreach { func =>
      val withHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
        topK(func, 2).collect().toSet
      }
      val withoutHash =
        withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
          topK(func, 2).collect().toSet
        }
      assert(withHash == withoutHash, s"$func() mismatch between hash and sort-based paths " +
        s"for a multi-column order spec: hash=$withHash, sort=$withoutHash")
      // Both functions keep exactly key 1's a=2 group (rank/dense_rank 1 and 2) plus key 2's
      // single row -- correct only if b was actually used to order the two a=2 rows.
      assert(withHash == Set(Row(1, 2L, 0L), Row(1, 2L, 9L), Row(2, 0L, 0L)),
        s"$func() <= 2 over (a desc, b asc) must keep exactly the a=2 group, got $withHash")
    }
  }

  // Regression tests for a deep-review finding: the single-order-column fast path used to compare
  // Double/Float order keys with raw java.lang.Double/Float.compare on their bit-reconstructed
  // values instead of Spark's actual SQL float ordering (SQLOrderingUtil.compareDoubles/
  // compareFloats, which treats -0.0 == 0.0). DENSE_RANK()'s slot weight counts *distinct* slots,
  // so splitting one logical -0.0/0.0 tied group into two slots inflated the distinct-group count
  // ahead of any later value, silently dropping rows that must survive. RANK()'s weight is a pure
  // row-count sum, which happens to be invariant to this split, so it is covered here purely as a
  // sanity guard, not because it was ever broken.
  // Driven directly against the accumulator (not via SQL) so the arrival order -- 5.0, then 0.0,
  // then -0.0, into a single accumulator instance -- is exact and not at the mercy of shuffle/
  // partition-dependent reordering, which a SQL-level test over a 3-row DataFrame cannot guarantee
  // (confirmed: an earlier SQL-level version of this test passed even against the unfixed
  // comparator, because this tiny input happened not to get redistributed in an order that
  // triggers the bug). With this exact order: 5.0 is admitted into slot0; 0.0 is admitted into
  // slot1 (preceding weight 1 < limit 2); -0.0 must then be recognized as a tie for slot1 (since
  // -0.0 == 0.0 under SQL semantics) -- under the pre-fix raw Double.compare, -0.0 instead sorted
  // as a brand-new 3rd distinct value with preceding weight 2, which is not < limit (2), so it was
  // discarded outright before ever being appended as a candidate.
  test("SPARK-61804 (rank hash pushdown): DENSE_RANK() <= 2 treats -0.0 and 0.0 as equal for a" +
      " Double order key (regression test for a signed-zero comparator bug)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val tsDoubleAttr = AttributeReference("ts", DoubleType)()
      val doubleOrderSpec = Seq(SortOrder(tsDoubleAttr, Descending))
      val doubleChildOutput = Seq(kAttr, tsDoubleAttr)
      val accumulator = {
        val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
        val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
        val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
        val numTasksFallbackOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
        val numTasksFallbackPoorCompaction =
          SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
        new HashRankGroupLimitAccumulator(
          partitionSpec, doubleOrderSpec, new DenseRank(), doubleChildOutput, numOutputRows,
          peakMemory, spillSize, numTasksFallbackOOM, numTasksFallbackPoorCompaction,
          minCompaction = 0.0, minRowsBetweenCompactionChecks = 0L, enableRadixSort = true,
          limit = 2)
      }
      val rowProjection = UnsafeProjection.create(doubleChildOutput.map(_.dataType).toArray)
      Seq((1, 5.0), (1, 0.0), (1, -0.0)).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getDouble(1)).sorted
      assert(tsValues.length == 3,
        s"all 3 rows (5.0, and the tied 0.0/-0.0 pair at dense_rank 2) must survive, got $tsValues")
    }
  }

  test("SPARK-61804 (rank hash pushdown): DENSE_RANK() <= 2 treats -0.0f and 0.0f as equal for a" +
      " Float order key (regression test for a signed-zero comparator bug)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val tsFloatAttr = AttributeReference("ts", FloatType)()
      val floatOrderSpec = Seq(SortOrder(tsFloatAttr, Descending))
      val floatChildOutput = Seq(kAttr, tsFloatAttr)
      val accumulator = {
        val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
        val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
        val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
        val numTasksFallbackOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
        val numTasksFallbackPoorCompaction =
          SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
        new HashRankGroupLimitAccumulator(
          partitionSpec, floatOrderSpec, new DenseRank(), floatChildOutput, numOutputRows,
          peakMemory, spillSize, numTasksFallbackOOM, numTasksFallbackPoorCompaction,
          minCompaction = 0.0, minRowsBetweenCompactionChecks = 0L, enableRadixSort = true,
          limit = 2)
      }
      val rowProjection = UnsafeProjection.create(floatChildOutput.map(_.dataType).toArray)
      Seq((1, 5.0f), (1, 0.0f), (1, -0.0f)).foreach { case (k, ts) =>
        accumulator.consumeRow(rowProjection(InternalRow(k, ts)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      val tsValues = result.map(_.getFloat(1)).sorted
      assert(tsValues.length == 3,
        s"all 3 rows (5.0f, and the tied 0.0f/-0.0f pair at dense_rank 2) must survive, got" +
          s" $tsValues")
    }
  }

  test("SPARK-61804 (rank hash pushdown): RANK() <= 2 already handles -0.0/0.0 correctly (row-" +
      " count weight is invariant to the DENSE_RANK-only slot-splitting bug)") {
    val df = Seq((1, 5.0), (1, 0.0), (1, -0.0)).toDF("key", "ts")
    val withHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      topKByRank(df, 2).collect().toSet
    }
    val withoutHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      topKByRank(df, 2).collect().toSet
    }
    assert(withHash == withoutHash)
    assert(withHash == Set(Row(1, 5.0), Row(1, 0.0), Row(1, -0.0)))
  }

  test("SPARK-61804 (rank hash pushdown): DENSE_RANK() <= 2 handles NaN order keys (sorts" +
      " greatest, unaffected by the -0.0/0.0 fix since doubleToLongBits already canonicalizes" +
      " NaN's bit pattern)") {
    val df = Seq((1, Double.NaN), (1, Double.NaN), (1, 5.0)).toDF("key", "ts")
    val withHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      topKByDenseRank(df, 2).collect().toSet
    }
    val withoutHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      topKByDenseRank(df, 2).collect().toSet
    }
    assert(withHash == withoutHash)
    // Descending order: NaN sorts greatest, so dense_rank(NaN)=1, dense_rank(5.0)=2 -- all 3 rows.
    assert(withHash == Set(Row(1, Double.NaN), Row(1, Double.NaN), Row(1, 5.0)))
  }

  // Regression test for a deep-review finding: DecimalType(<= 18) was added to the single-column
  // fast path's type-eligibility table (singleColTypeTag case 8, codegen's generated
  // toUnscaledLong() call), but nothing exercised it end-to-end. Driven directly against the
  // accumulator (not via SQL) with a precise arrival order, for the same reason as the analogous
  // ROW_NUMBER() regression test in HashWindowGroupLimitSuite: an earlier SQL-level version of
  // this test passed even against a deliberately broken case-8 (always reports "equal"), because
  // -- with every row quick-matching the first-ever slot regardless of its real value -- the
  // specific data happened to produce the same *content* either way. RANK()'s dataset -- 5.50,
  // then 3.00 (a genuine second distinct value), then a second 5.50 (a genuine tie with the first
  // slot, not the most-recently-touched one) -- forces both a real non-equality decision (5.50 vs
  // 3.00 must NOT quick-match) and a real equality decision (the second 5.50 must correctly match
  // slot 0, not fall through to binarySearchSlot as a bogus new value) through compareSlotSingle's
  // case-8 branch specifically. DENSE_RANK() uses its own, duplicate-free dataset below (a tie
  // doesn't exercise its eviction path the same way -- see the comment at that assertion) so that
  // reusing RANK()'s tied dataset doesn't silently turn into a non-discriminating sanity check.
  test("SPARK-61804 (rank hash pushdown): RANK()/DENSE_RANK() <= 2 over a narrow (precision <=" +
      " 18) decimal order key (regression test for a decimal comparator bug)") {
    withStandaloneTaskContext(budgetBytes = Long.MaxValue) {
      val amtAttr = AttributeReference("amt", DecimalType(18, 2))()
      val decimalOrderSpec = Seq(SortOrder(amtAttr, Descending))
      val decimalChildOutput = Seq(kAttr, amtAttr)
      def decimal(unscaled: Long): Decimal = Decimal(unscaled, 18, 2)

      def newDecimalAccumulator(rankLikeFunction: Expression): HashRankGroupLimitAccumulator = {
        val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
        val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
        val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
        val numTasksFallbackOOM = SQLMetrics.createMetric(spark.sparkContext, "fallbackOOM")
        val numTasksFallbackPoorCompaction =
          SQLMetrics.createMetric(spark.sparkContext, "fallbackPoorCompaction")
        new HashRankGroupLimitAccumulator(
          partitionSpec, decimalOrderSpec, rankLikeFunction, decimalChildOutput, numOutputRows,
          peakMemory, spillSize, numTasksFallbackOOM, numTasksFallbackPoorCompaction,
          limit = 2)
      }
      val rowProjection = UnsafeProjection.create(decimalChildOutput.map(_.dataType).toArray)
      def feedRows(accumulator: HashRankGroupLimitAccumulator, amounts: Seq[Decimal]): Unit = {
        amounts.foreach { amt =>
          accumulator.consumeRow(rowProjection(InternalRow(1, amt)).copy())
        }
      }

      // RANK(): the two genuine 5.50 ties accumulate weight 2 at slot 0, which pushes 3.00's
      // preceding weight to 2 -- no longer < limit (2) -- evicting it (revalidateTrailingSlots).
      val rankAccumulator = newDecimalAccumulator(new Rank())
      feedRows(rankAccumulator, Seq(decimal(550), decimal(300), decimal(550)))
      val rankResult = rankAccumulator.finish().map(_.copy()).toSeq
        .map(_.getDecimal(1, 18, 2)).sorted
      assert(rankResult == Seq(decimal(550), decimal(550)),
        s"RANK() <= 2 must keep only the tied 5.50 pair (3.00's rank is 3), got $rankResult")

      // DENSE_RANK(): a *different* (duplicate-free) sequence from RANK()'s, since a repeated tie
      // doesn't actually exercise DENSE_RANK()'s own eviction path -- its flat per-slot weight
      // (isDenseRank skips the rowCount/revalidateTrailingSlots update entirely, see consumeRow)
      // means a tie never changes how many *distinct* values are admitted, so reusing RANK()'s
      // 3-row tied dataset here would pass identically whether case 8 is correct or a broken
      // "always tie" comparator (every row would spuriously match slot 0, slotCount would never
      // grow past 1, hasEvictions would never flip, and nextResultRow's `!hasEvictions` branch
      // would unconditionally emit all 3 candidates regardless of the comparator -- the exact
      // same {3.00, 5.50, 5.50} result as the correct implementation). Three genuinely distinct
      // values instead -- 5.50, 3.00, 1.00 -- forces a real admission-boundary decision: a correct
      // comparator keeps the two best (5.50, 3.00) and discards 1.00 (dense rank 3 > limit 2);
      // a broken "always tie" comparator instead merges all three into slot 0 (hasEvictions never
      // flips) and emits all three, including the wrongly-kept 1.00 -- a result that disagrees
      // with the correct one and so actually fails the assertion below.
      val denseRankAccumulator = newDecimalAccumulator(new DenseRank())
      feedRows(denseRankAccumulator, Seq(decimal(550), decimal(300), decimal(100)))
      val denseRankResult = denseRankAccumulator.finish().map(_.copy()).toSeq
        .map(_.getDecimal(1, 18, 2)).sorted
      assert(denseRankResult == Seq(decimal(300), decimal(550)),
        s"DENSE_RANK() <= 2 must keep 5.50 (dense rank 1) and 3.00 (dense rank 2), excluding" +
          s" 1.00 (dense rank 3), got $denseRankResult")
    }
  }

  test("SPARK-61804 (rank hash pushdown): RANK()/DENSE_RANK() plans are hash-eligible") {
    val df = Seq((1, 10L), (1, 5L), (2, 7L)).toDF("key", "ts")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      Seq(topKByRank(df, 2), topKByDenseRank(df, 2)).foreach { query =>
        val plan = query.queryExecution.executedPlan
        val limits = collectWithSubqueries(plan) { case w: WindowGroupLimitExec => w }
        assert(limits.nonEmpty && limits.forall(_.isHashEligible),
          "RANK()/DENSE_RANK() with a numeric order key must be hash-eligible")
        assert(limits.forall(!_.child.isInstanceOf[SortExec]),
          "hash-eligible WindowGroupLimitExec must not have a SortExec as its immediate child")
      }
    }
  }

  test("SPARK-61804 (rank hash pushdown): codegen and interpreted hash paths agree for RANK()" +
      " -- against a real sort-based baseline, not just each other") {
    val df = (0 until 500).map(i => (i % 37, (i % 11).toLong)).toDF("key", "ts")
    // Asserting codegenOn == codegenOff alone is not a sufficient regression guard: both paths
    // drive the identical HashRankGroupLimitAccumulator logic, so a bug in that shared logic
    // (e.g. the re-validation gap fixed alongside this test) would make both sides agree on the
    // same wrong answer and this test would pass despite the defect. Comparing against the
    // sort-based baseline -- which goes through the real, independently-implemented
    // RankLimitIterator -- actually exercises correctness, not just codegen/interpreted parity.
    val baseline = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
      topKByRank(df, 3).collect().toSet
    }
    val codegenOn = withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      topKByRank(df, 3).collect().toSet
    }
    val codegenOff = withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      topKByRank(df, 3).collect().toSet
    }
    assert(codegenOn == baseline, "hash path (codegen) must match the sort-based baseline")
    assert(codegenOff == baseline, "hash path (interpreted) must match the sort-based baseline")
  }

  test("SPARK-61804 (rank hash pushdown): RANK()/DENSE_RANK() hash path matches sort-based path" +
      " under heavy slot eviction churn (drain-time cutoff check)") {
    // Exercises nextResultRow's O(1) cutoff-based admission check (see its doc): many more
    // distinct values than `limit` arrive per key, in random order, forcing repeated tail
    // eviction interleaved with brand-new, never-admitted values -- ties land on still-admitted
    // values, already-evicted values, and values that were never admitted at all, the combination
    // most likely to expose a bug in comparing only against the final cutoff instead of
    // re-deriving each candidate's own slot.
    val limit = 20
    val numKeys = 10
    val valuesPerKey = 200
    val rand = new scala.util.Random(42)
    val df = (0 until numKeys).flatMap { k =>
      rand.shuffle((0 until valuesPerKey).toList).flatMap(v => Seq.fill(2)((k, v.toLong)))
    }.toDF("key", "ts")

    Seq(topKByRank(df, limit) -> "RANK", topKByDenseRank(df, limit) -> "DENSE_RANK").foreach {
      case (query, label) =>
        val withHash = withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
          query.collect().toSet
        }
        val withoutHash =
          withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
            query.collect().toSet
          }
        assert(withHash == withoutHash, s"$label hash path must match the sort-based baseline")
    }
  }
}
