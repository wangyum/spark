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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DenseRank, Descending,
  Expression, Rank, SortOrder, UnsafeProjection}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
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
      minCompactionRatio: Double = 0.0,
      minRowsBetweenCompactionChecks: Long = 0L): HashRankGroupLimitAccumulator = {
    val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
    val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
    val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
    val numTasksFallBacked = SQLMetrics.createMetric(spark.sparkContext, "fallback")
    new HashRankGroupLimitAccumulator(
      partitionSpec, orderSpec, rankLikeFunction, childOutput, numOutputRows, peakMemory,
      spillSize, numTasksFallBacked, minCompactionRatio, minRowsBetweenCompactionChecks,
      enableRadixSort = true, limit)
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
      val numTasksFallBacked = SQLMetrics.createMetric(spark.sparkContext, "fallback")
      val accumulator = new HashRankGroupLimitAccumulator(
        partitionSpec, orderSpec, new Rank(), wideChildOutput, numOutputRows, peakMemory, spillSize,
        numTasksFallBacked, minCompactionRatio = 0.0, minRowsBetweenCompactionChecks = 0L,
        enableRadixSort = true, limit = 1)

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
      assert(numTasksFallBacked.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered -- " +
          "if it stops triggering, tighten the budget/enlarge the padding above rather than " +
          "deleting this assertion")
    }
  }

  test("SPARK-61804: Final-mode genuine OOM fallback safely short-circuits without accessing" +
      " freed memory") {
    withStandaloneTaskContext(budgetBytes = 2 * 1024 * 1024 + 300 * 1024, pageSizeBytes = "2m") {
      val padAttr = AttributeReference("pad", StringType)()
      val wideChildOutput = Seq(kAttr, tsAttr, padAttr)
      val numOutputRows = SQLMetrics.createMetric(spark.sparkContext, "rows")
      val peakMemory = SQLMetrics.createSizeMetric(spark.sparkContext, "peak")
      val spillSize = SQLMetrics.createSizeMetric(spark.sparkContext, "spill")
      val numTasksFallBacked = SQLMetrics.createMetric(spark.sparkContext, "fallback")
      val accumulator = new HashRankGroupLimitAccumulator(
        partitionSpec, orderSpec, new Rank(), wideChildOutput, numOutputRows, peakMemory,
        spillSize, numTasksFallBacked, minCompactionRatio = 0.0,
        minRowsBetweenCompactionChecks = 0L, enableRadixSort = true, limit = 1, mode = Final)

      val rowProjection = UnsafeProjection.create(wideChildOutput.map(_.dataType).toArray)
      val pad = UTF8String.fromString("x" * 100)
      val n = 5000
      (0 until n).foreach { k =>
        accumulator.consumeRow(rowProjection(InternalRow(k, k.toLong, pad)).copy())
      }
      val result = accumulator.finish().map(_.copy()).toSeq
      assert(result.length == n,
        s"Final-mode fallback must emit every one of the $n rows, got ${result.length}")
      assert(numTasksFallBacked.value > 0,
        "this test is only meaningful if the genuine-OOM fallback actually triggered")
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
    // Exercises kvIteratorToScala's O(1) cutoff-based admission check (see its doc): many more
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
