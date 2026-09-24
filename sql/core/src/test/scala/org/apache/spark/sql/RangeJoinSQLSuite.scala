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

package org.apache.spark.sql

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.IntervalOverlapJoin
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.execution.{
  InputAdapter, ReusedSubqueryExec, ScalarSubquery, SparkPlan, SubqueryExec,
  WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.{
  BroadcastNestedLoopJoinExec, BroadcastRangeJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end SQL tests for the range join physical operator, which speeds up range-predicate
 * joins (e.g. `a.low <= b.high AND b.low <= a.high`, or an IP-lookup-style
 * `ip BETWEEN low AND high`) that would otherwise fall back to [[BroadcastNestedLoopJoinExec]].
 * The operator itself is covered at a lower level by `RangeJoinSuite`; this suite focuses on
 * planner gating (`spark.sql.join.broadcastRangeJoin.enabled`) and end-to-end correctness.
 */
class RangeJoinSQLSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {

  private def findRangeJoin(plan: SparkPlan): Seq[BroadcastRangeJoinExec] = {
    collect(plan) { case j: BroadcastRangeJoinExec => j }
  }

  /** The range join is compiled into a whole-stage, not hidden under an input adapter. */
  private def rangeJoinIsCodegen(plan: SparkPlan): Boolean = {
    exists(plan) {
      case w: WholeStageCodegenExec =>
        def contains(p: SparkPlan): Boolean = p match {
          case _: InputAdapter => false
          case _: BroadcastRangeJoinExec => true
          case other => other.children.exists(contains)
        }
        contains(w.child)
      case _ => false
    }
  }

  private def findBroadcastNestedLoopJoin(plan: SparkPlan): Seq[BroadcastNestedLoopJoinExec] = {
    collect(plan) { case j: BroadcastNestedLoopJoinExec => j }
  }

  private def findCartesianProduct(plan: SparkPlan): Seq[CartesianProductExec] = {
    collect(plan) { case c: CartesianProductExec => c }
  }

  private def setupIpLookupViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW signin_ip(id, ip_add_int) AS VALUES
        |  ('1', 1467630666L),
        |  ('2', 1186929717L),
        |  ('3', 3501544352L),
        |  ('31', 3501544320L),
        |  ('32', 3501544383L),
        |  ('33', 3501544330L),
        |  ('34', 3501544390L),
        |  ('4', 1626243295L),
        |  ('41', 1626243072L),
        |  ('42', 1626243327L),
        |  ('5', 1206355995L),
        |  ('5', 1206355995L),
        |  ('6', CAST(NULL AS BIGINT)),
        |  ('7', 0L)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW ip_lookup(
        |    BEGIN_IP2LONG, END_IP2LONG, TWO_CHAR_CNTRY_CD, zip_id) AS VALUES
        |  (1467630592L, 1467630847L, 'de', 2380520L),
        |  (1186929664L, 1186930175L, 'us', 6723936L),
        |  (1186929664L, 1186930175L, 'us', 6723936L),
        |  (3501544320L, 3501544383L, 'ca', 12804631L),
        |  (3501544330L, 3501544390L, 'ca', 12804631001L),
        |  (1626243072L, 1626243327L, 'us', 8849753L),
        |  (1206355968L, 1206356223L, 'us', 10138801L),
        |  (CAST(NULL AS BIGINT), 1206356223L, 'us', 10138801L),
        |  (1206355968L, CAST(NULL AS BIGINT), 'us', 10138801L),
        |  (CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), 'us', 10138801L)
        |""".stripMargin)
  }

  private def setupIntervalViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW intervals_a(lo, hi) AS VALUES
        |  (-1, 0), (0, 1), (0, 2), (1, 5)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW intervals_b(lo, hi) AS VALUES
        |  (-2, -1), (-4, -2), (1, 3), (5, 7)
        |""".stripMargin)
  }

  test("range join is disabled by default and falls back to broadcast nested loop join") {
    setupIpLookupViews()
    val pointInRangeQuery =
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    val df = sql(pointInRangeQuery)
    assert(findRangeJoin(df.queryExecution.executedPlan).isEmpty)
    assert(findBroadcastNestedLoopJoin(df.queryExecution.executedPlan).size == 1)
  }

  test("point-in-range join produces the same result as broadcast nested loop join") {
    setupIpLookupViews()
    val pointInRangeQuery =
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    val expected = sql(pointInRangeQuery).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(pointInRangeQuery)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("BETWEEN is planned as a point-in-range join") {
    setupIpLookupViews()
    val betweenQuery =
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int BETWEEN l.BEGIN_IP2LONG AND l.END_IP2LONG"
    val expected = sql(betweenQuery).collect()

    withSQLConf(
      SQLConf.RANGE_JOIN_ENABLED.key -> "true",
      SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> "false") {
      val df = sql(betweenQuery)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("broadcast hint on a supported side plans a range join") {
    setupIpLookupViews()
    val hinted =
      "SELECT /*+ BROADCAST(l) */ s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    val expected = sql(
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG").collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(hinted)
      val joins = findRangeJoin(df.queryExecution.executedPlan)
      assert(joins.size == 1)
      assert(joins.head.buildSide == BuildRight)
      checkAnswer(df, expected)
    }
  }

  test("broadcast hint on an unsupported side stays a nested loop join") {
    setupIntervalViews()
    val hinted =
      "SELECT /*+ BROADCAST(a) */ a.lo, b.hi FROM intervals_a a LEFT JOIN intervals_b b " +
        "ON a.lo < b.hi"
    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(hinted)
      val joins = findBroadcastNestedLoopJoin(df.queryExecution.executedPlan)
      assert(findRangeJoin(df.queryExecution.executedPlan).isEmpty)
      assert(joins.size == 1)
      assert(joins.head.buildSide == BuildLeft)
    }
  }

  test("shuffle replicate hint stays a cartesian product") {
    setupIpLookupViews()
    val hinted =
      "SELECT /*+ SHUFFLE_REPLICATE_NL(s) */ s.id, l.zip_id " +
        "FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val plan = sql(hinted).queryExecution.executedPlan
      assert(findRangeJoin(plan).isEmpty)
      assert(findCartesianProduct(plan).size == 1)
    }
  }

  test("partial range join is planned when the range-join flag is enabled") {
    setupIntervalViews()
    val partialQuery = "SELECT a.lo, b.hi FROM intervals_a a JOIN intervals_b b ON a.lo < b.hi"
    val expected = sql(partialQuery).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val enabled = sql(partialQuery)
      assert(findRangeJoin(enabled.queryExecution.executedPlan).size == 1)
      checkAnswer(enabled, expected)
    }
  }

  test("partial range join with an extra predicate is planned when the flag is enabled") {
    setupIntervalViews()
    // A single partial-range predicate plus a residual condition. This exercises the
    // `findRangeJoin` partial-range + residual path (the point-in-range + residual path
    // is covered by the complex test below).
    val partialWithExtraQuery =
      "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a JOIN intervals_b b " +
        "ON a.lo < b.hi AND a.hi <> b.lo"
    val expected = sql(partialWithExtraQuery).collect()
    assert(expected.nonEmpty)

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val enabled = sql(partialWithExtraQuery)
      assert(findRangeJoin(enabled.queryExecution.executedPlan).size == 1)
      checkAnswer(enabled, expected)
    }
  }

  test("range join with an extra predicate is planned when the range-join flag is enabled") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW complex_a(id, lo, hi) AS VALUES
        |  (1, 0, 10), (2, 0, 10)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW complex_b(id, point) AS VALUES
        |  (1, 5), (2, 5)
        |""".stripMargin)
    // The range predicate alone matches all 4 (a, b) combinations, since both a's ranges cover
    // both b's points. The extra predicate compares an id column from each side with a
    // non-equality operator, so it is neither pushed down to either child nor extracted as an
    // equi-join key -- it survives as a genuine join rest condition that filters the 4 candidate
    // pairs down to the 2 with mismatched ids.
    val complexQuery =
      "SELECT a.id, b.id FROM complex_a a JOIN complex_b b " +
        "ON b.point >= a.lo AND b.point <= a.hi AND a.id <> b.id"
    val expected = sql(complexQuery).collect()
    assert(expected.length == 2)

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val enabled = sql(complexQuery)
      assert(findRangeJoin(enabled.queryExecution.executedPlan).size == 1)
      checkAnswer(enabled, expected)
    }
  }

  test("partial range join with a greater-than predicate is planned when the flag is enabled") {
    setupIntervalViews()
    val partialQuery = "SELECT a.lo, b.hi FROM intervals_a a JOIN intervals_b b ON a.lo > b.hi"
    val expected = sql(partialQuery).collect()
    assert(expected.nonEmpty)

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val enabled = sql(partialQuery)
      assert(findRangeJoin(enabled.queryExecution.executedPlan).size == 1)
      checkAnswer(enabled, expected)
    }
  }

  test("range join handles heavily-overlapping build intervals") {
    // With the row-reference cap removed, a build side with heavily-overlapping
    // intervals no longer aborts; the join runs to completion and produces the full
    // cross-product of matching (range, point) pairs.
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW overlap_a(lo, hi) AS VALUES
        |  (0, 100), (0, 100)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW overlap_b(point) AS VALUES
        |  (50), (50), (50), (50), (50), (50), (50), (50), (50), (50)
        |""".stripMargin)
    val query =
      "SELECT a.lo, a.hi, b.point FROM overlap_a a JOIN overlap_b b " +
        "ON b.point >= a.lo AND b.point <= a.hi"
    val expected = sql(query).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("boundary-exact-match: point equal to low bound matches with inclusive low") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW probe_a(lo, hi) AS VALUES
        |  (0, 100), (50, 200)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW probe_b(point) AS VALUES
        |  (50), (50), (50), (50), (50), (50), (50), (50), (50), (50)
        |""".stripMargin)
    val query =
      "SELECT a.lo, a.hi, b.point FROM probe_a a JOIN probe_b b " +
        "ON b.point >= a.lo AND b.point < a.hi"
    val expected = sql(query).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      val joins = findRangeJoin(df.queryExecution.executedPlan)
      assert(joins.nonEmpty)
      // b.point >= a.lo (inclusive low) AND b.point < a.hi (exclusive high).
      // point=50 matches [0, 100) (50 < 100) and [50, 200) (50 >= 50, 50 < 200).
      checkAnswer(df, expected)
    }
  }

  test("range join treats null range boundaries as non-matching") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW null_points(point) AS VALUES
        |  (CAST(1 AS BIGINT)), (CAST(NULL AS BIGINT))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW null_ranges(lo, hi) AS VALUES
        |  (CAST(0 AS BIGINT), CAST(2 AS BIGINT)),
        |  (CAST(NULL AS BIGINT), CAST(2 AS BIGINT)),
        |  (CAST(0 AS BIGINT), CAST(NULL AS BIGINT))
        |""".stripMargin)
    val nullQuery =
      "SELECT p.point, r.lo, r.hi FROM null_points p JOIN null_ranges r " +
        "ON p.point >= r.lo AND p.point <= r.hi"

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(nullQuery)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, Seq(Row(1L, 0L, 2L)))
    }
  }

  test("range join produces correct results under adaptive query execution") {
    setupIntervalViews()
    sql("CREATE OR REPLACE TEMP VIEW reuse_points(point) AS VALUES (-3), (1), (3), (6)")
    val query =
      "SELECT p.point, i1.lo, i1.hi, i2.lo, i2.hi FROM reuse_points p " +
        "JOIN intervals_a i1 ON p.point >= i1.lo AND p.point < i1.hi " +
        "JOIN intervals_a i2 ON p.point >= i2.lo AND p.point < i2.hi"

    val expected = sql(query).collect()

    withSQLConf(
        SQLConf.RANGE_JOIN_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val df = sql(query)
      checkAnswer(df, expected)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 2)
    }
  }

  test("range join handles an empty build side") {
    sql("CREATE OR REPLACE TEMP VIEW empty_ranges(lo, hi) AS " +
      "VALUES (CAST(NULL AS BIGINT), CAST(NULL AS BIGINT))")
    sql("CREATE OR REPLACE TEMP VIEW nonempty_points(point) AS VALUES (1), (2), (3)")
    val query =
      "SELECT p.point, r.lo, r.hi FROM nonempty_points p JOIN empty_ranges r " +
        "ON p.point >= r.lo AND p.point <= r.hi"

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, Seq.empty[Row])
    }
  }

  test("two-sided interval overlap is planned as an interval index probe") {
    // a.lo < b.hi AND b.lo < a.hi has (low, high) on each side. The build side is an
    // interval index and the stream side probes it with overlapping(low, high).
    setupIntervalViews()
    val overlapQuery =
      "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a JOIN intervals_b b " +
        "ON a.lo < b.hi AND b.lo < a.hi"
    val expected = sql(overlapQuery).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(overlapQuery)
      val planned = findRangeJoin(df.queryExecution.executedPlan)
      assert(planned.size == 1)
      assert(planned.head.rangeJoin == IntervalOverlapJoin)
      assert(planned.head.leftKeys.length == 2)
      assert(planned.head.rightKeys.length == 2)
      checkAnswer(df, expected)
    }
  }

  test("explain prints the original range keys") {
    setupIpLookupViews()
    val query =
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val planned = findRangeJoin(sql(query).queryExecution.executedPlan)
      assert(planned.size == 1)
      val explained = planned.head.verboseStringWithOperatorId()
      assert(explained.contains("ip_add_int"))
      assert(explained.contains("BEGIN_IP2LONG"))
      assert(!explained.contains("none#"))
    }
  }

  test("scalar subquery in the range condition is planned") {
    setupIpLookupViews()
    // The extra conjunct names both sides, so it stays on the join instead of
    // being pushed to one child. The subquery has to be planned from that condition.
    val query =
      "SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG " +
        "AND s.ip_add_int + l.zip_id > (SELECT min(ip_add_int) FROM signin_ip)"
    val expected = sql(query).collect()
    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      val planned = findRangeJoin(df.queryExecution.executedPlan)
      assert(planned.size == 1)
      val cond = planned.head.condition.get
      val subquery = cond.collectFirst {
        case s: ScalarSubquery => s.plan
      }
      assert(subquery.exists(plan =>
        plan.isInstanceOf[SubqueryExec] || plan.isInstanceOf[ReusedSubqueryExec]),
        cond)
      checkAnswer(df, expected)
    }
  }

  test("day-time interval inequality stays a nested loop join") {
    val query =
      "SELECT a.x, b.y FROM VALUES (INTERVAL '1' HOUR) a(x) " +
        "JOIN VALUES (INTERVAL '2' HOUR) b(y) ON a.x < b.y"
    val expected = sql(query).collect()
    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).isEmpty)
      assert(findBroadcastNestedLoopJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("range join boundary equality: point exactly matches a build key") {
    sql("CREATE OR REPLACE TEMP VIEW boundary_points(point) AS VALUES (0), (1), (2), (3)")
    sql("CREATE OR REPLACE TEMP VIEW boundary_ranges(lo, hi) AS VALUES (0, 1), (1, 2), (2, 3)")
    // Inclusive on both ends: a point equal to lo or hi should match.
    val inclusiveQuery =
      "SELECT p.point, r.lo, r.hi FROM boundary_points p JOIN boundary_ranges r " +
        "ON p.point >= r.lo AND p.point <= r.hi"
    val inclusiveExpected = sql(inclusiveQuery).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(inclusiveQuery)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, inclusiveExpected)
    }

    // Exclusive on the high end: a point equal to hi should NOT match.
    val exclusiveHighQuery =
      "SELECT p.point, r.lo, r.hi FROM boundary_points p JOIN boundary_ranges r " +
        "ON p.point >= r.lo AND p.point < r.hi"
    val exclusiveHighExpected = sql(exclusiveHighQuery).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(exclusiveHighQuery)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, exclusiveHighExpected)
    }

    // Exclusive on the low end: a point equal to lo should NOT match.
    val exclusiveLowQuery =
      "SELECT p.point, r.lo, r.hi FROM boundary_points p JOIN boundary_ranges r " +
        "ON p.point > r.lo AND p.point <= r.hi"
    val exclusiveLowExpected = sql(exclusiveLowQuery).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(exclusiveLowQuery)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, exclusiveLowExpected)
    }

    // Same mixed bounds, high conjunct written first. Inclusivity must follow the
    // predicates themselves, not AND order.
    val highThenLowQuery =
      "SELECT p.point, r.lo, r.hi FROM boundary_points p JOIN boundary_ranges r " +
        "ON p.point <= r.hi AND p.point > r.lo"
    val lowThenExclusiveHighQuery =
      "SELECT p.point, r.lo, r.hi FROM boundary_points p JOIN boundary_ranges r " +
        "ON p.point < r.hi AND p.point >= r.lo"
    val highThenLowExpected = sql(highThenLowQuery).collect()
    val lowThenExclusiveHighExpected = sql(lowThenExclusiveHighQuery).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val highThenLow = sql(highThenLowQuery)
      assert(findRangeJoin(highThenLow.queryExecution.executedPlan).size == 1)
      checkAnswer(highThenLow, highThenLowExpected)
      val lowThenExclusiveHigh = sql(lowThenExclusiveHighQuery)
      assert(findRangeJoin(lowThenExclusiveHigh.queryExecution.executedPlan).size == 1)
      checkAnswer(lowThenExclusiveHigh, lowThenExclusiveHighExpected)
    }
  }

  test("range join on decimal keys") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dec_ranges(lo, hi) AS VALUES
        |  (CAST(0.00 AS DECIMAL(10, 2)), CAST(2.50 AS DECIMAL(10, 2))),
        |  (CAST(3.00 AS DECIMAL(10, 2)), CAST(5.00 AS DECIMAL(10, 2)))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dec_points(p) AS VALUES
        |  (CAST(1.25 AS DECIMAL(10, 2))),
        |  (CAST(3.00 AS DECIMAL(10, 2))),
        |  (CAST(9.00 AS DECIMAL(10, 2)))
        |""".stripMargin)
    val query =
      "SELECT p.p, r.lo, r.hi FROM dec_points p JOIN dec_ranges r " +
        "ON p.p >= r.lo AND p.p <= r.hi"
    val expected = sql(query).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("range join on date keys") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW date_ranges(lo, hi) AS VALUES
        |  (DATE '2020-01-01', DATE '2020-01-31'),
        |  (DATE '2020-02-01', DATE '2020-02-29')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW date_points(d) AS VALUES
        |  (DATE '2020-01-15'),
        |  (DATE '2020-02-01'),
        |  (DATE '2020-03-01')
        |""".stripMargin)
    val query =
      "SELECT p.d, r.lo, r.hi FROM date_points p JOIN date_ranges r " +
        "ON p.d >= r.lo AND p.d <= r.hi"
    val expected = sql(query).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("range join reuses a broadcast range exchange under AQE") {
    sql("CREATE OR REPLACE TEMP VIEW overlap_points(point) AS VALUES (0), (1), (2), (3)")
    sql("CREATE OR REPLACE TEMP VIEW overlap_ranges(lo, hi) AS VALUES (0, 2), (1, 3)")
    val query =
      "SELECT p.point, r1.lo, r1.hi, r2.lo, r2.hi FROM overlap_points p " +
        "JOIN overlap_ranges r1 ON p.point >= r1.lo AND p.point <= r1.hi " +
        "JOIN overlap_ranges r2 ON p.point >= r2.lo AND p.point <= r2.hi"
    val expected = sql(query).collect()

    withSQLConf(
        SQLConf.RANGE_JOIN_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val df = sql(query)
      checkAnswer(df, expected)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 2)
    }
  }

  test("range join on case-insensitive strings matches the nested loop result") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_ranges(lo, hi) AS VALUES
        |  (CAST('a' AS STRING COLLATE UNICODE_CI), CAST('m' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('n' AS STRING COLLATE UNICODE_CI), CAST('z' AS STRING COLLATE UNICODE_CI))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_points(s) AS VALUES
        |  (CAST('B' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('n' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('Z' AS STRING COLLATE UNICODE_CI))
        |""".stripMargin)
    val query =
      "SELECT p.s, r.lo, r.hi FROM str_points p JOIN str_ranges r " +
        "ON p.s >= r.lo AND p.s <= r.hi"
    val expected = sql(query).collect()

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      checkAnswer(df, expected)
    }
  }

  test("left outer, right outer, semi, and anti match the nested loop result") {
    setupIpLookupViews()
    val cond =
      "s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    Seq(
      s"SELECT s.id, l.zip_id FROM signin_ip s LEFT JOIN ip_lookup l ON $cond",
      s"SELECT s.id, l.zip_id FROM signin_ip s RIGHT JOIN ip_lookup l ON $cond",
      s"SELECT s.id FROM signin_ip s LEFT SEMI JOIN ip_lookup l ON $cond",
      s"SELECT s.id FROM signin_ip s LEFT ANTI JOIN ip_lookup l ON $cond"
    ).foreach { query =>
      val expected = sql(query).collect()
      withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
        val df = sql(query)
        assert(findRangeJoin(df.queryExecution.executedPlan).size == 1, query)
        checkAnswer(df, expected)
      }
    }
  }

  test("partial range left outer matches the nested loop result") {
    setupIntervalViews()
    // (1, 5) is below every b.lo, so the left row is kept with nulls.
    val query =
      "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a LEFT JOIN intervals_b b " +
        "ON a.hi < b.lo"
    val expected = sql(query).collect()
    assert(expected.exists(_.anyNull))

    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      val planned = findRangeJoin(df.queryExecution.executedPlan)
      assert(planned.size == 1)
      assert(planned.head.joinType == LeftOuter)
      assert(planned.head.buildSide == BuildRight)
      assert(planned.head.verboseStringWithOperatorId().contains("LessPartialRangeJoin"))
      checkAnswer(df, expected)
    }
  }

  test("whole-stage codegen covers inner, outer, semi, and anti range joins") {
    setupIpLookupViews()
    setupIntervalViews()
    // Decimal and collated string take the object-boxing probe path.
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dec_ranges(lo, hi) AS VALUES
        |  (CAST(0.00 AS DECIMAL(10, 2)), CAST(2.50 AS DECIMAL(10, 2))),
        |  (CAST(3.00 AS DECIMAL(10, 2)), CAST(5.00 AS DECIMAL(10, 2)))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW dec_points(p) AS VALUES
        |  (CAST(1.25 AS DECIMAL(10, 2))),
        |  (CAST(3.00 AS DECIMAL(10, 2))),
        |  (CAST(9.00 AS DECIMAL(10, 2)))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_ranges(lo, hi) AS VALUES
        |  (CAST('a' AS STRING COLLATE UNICODE_CI), CAST('m' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('n' AS STRING COLLATE UNICODE_CI), CAST('z' AS STRING COLLATE UNICODE_CI))
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW str_points(s) AS VALUES
        |  (CAST('B' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('n' AS STRING COLLATE UNICODE_CI)),
        |  (CAST('Z' AS STRING COLLATE UNICODE_CI))
        |""".stripMargin)
    val cond = "s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    val queries = Seq(
      s"SELECT s.id, l.zip_id FROM signin_ip s JOIN ip_lookup l ON $cond",
      s"SELECT s.id, l.zip_id FROM signin_ip s LEFT JOIN ip_lookup l ON $cond",
      s"SELECT s.id, l.zip_id FROM signin_ip s RIGHT JOIN ip_lookup l ON $cond",
      s"SELECT s.id FROM signin_ip s LEFT SEMI JOIN ip_lookup l ON $cond",
      s"SELECT s.id FROM signin_ip s LEFT ANTI JOIN ip_lookup l ON $cond",
      "SELECT a.lo, a.hi, b.lo, b.hi FROM intervals_a a LEFT JOIN intervals_b b " +
        "ON a.hi < b.lo",
      "SELECT p.p, r.lo, r.hi FROM dec_points p JOIN dec_ranges r " +
        "ON p.p >= r.lo AND p.p <= r.hi",
      "SELECT p.s, r.lo, r.hi FROM str_points p JOIN str_ranges r " +
        "ON p.s >= r.lo AND p.s <= r.hi")
    queries.foreach { query =>
      val expected = sql(query).collect()
      withSQLConf(
        SQLConf.RANGE_JOIN_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FALLBACK.key -> "false") {
        val df = sql(query)
        checkAnswer(df, expected)
        assert(rangeJoinIsCodegen(df.queryExecution.executedPlan), query)
      }
    }
  }

  test("full outer is not planned as a broadcast range join") {
    setupIpLookupViews()
    val query =
      "SELECT s.id, l.zip_id FROM signin_ip s FULL OUTER JOIN ip_lookup l " +
        "ON s.ip_add_int >= l.BEGIN_IP2LONG AND s.ip_add_int <= l.END_IP2LONG"
    withSQLConf(SQLConf.RANGE_JOIN_ENABLED.key -> "true") {
      val df = sql(query)
      assert(findRangeJoin(df.queryExecution.executedPlan).isEmpty)
    }
  }

  test("an interval index that reaches maxBroadcastTableSize fails the query") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW tiny_ranges(lo, hi) AS VALUES (0, 10), (1, 11)
        |""".stripMargin)
    withSQLConf(
        SQLConf.RANGE_JOIN_ENABLED.key -> "true",
        SQLConf.MAX_BROADCAST_TABLE_SIZE.key -> "1",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(
        "SELECT p.v, r.lo, r.hi FROM VALUES (5) p(v) JOIN tiny_ranges r " +
          "ON p.v >= r.lo AND p.v <= r.hi")
      assert(findRangeJoin(df.queryExecution.executedPlan).size == 1)
      val thrown = intercept[Exception] {
        df.collect()
      }
      val ex = Iterator.iterate(thrown: Throwable)(_.getCause)
        .takeWhile(_ != null)
        .collectFirst { case s: SparkException => s }
        .getOrElse(throw thrown)
      assert(ex.getCondition == "_LEGACY_ERROR_TEMP_2249")
    }
  }
}
