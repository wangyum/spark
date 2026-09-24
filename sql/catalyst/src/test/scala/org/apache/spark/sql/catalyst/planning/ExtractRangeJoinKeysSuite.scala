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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LocalRelation}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/**
 * Plan-level coverage for [[ExtractRangeJoinKeys]]: which condition shapes are
 * recognized, which `RangeJoin` tag is produced, and what residual remains.
 */
class ExtractRangeJoinKeysSuite extends PlanTest {

  private val points = LocalRelation($"p".int)
  private val ranges = LocalRelation($"lo".int, $"hi".int)
  private val intervalsA = LocalRelation($"lo".int, $"hi".int)
  private val intervalsB = LocalRelation($"lo".int, $"hi".int)

  private val p = points.output.head
  private val rLo = ranges.output(0)
  private val rHi = ranges.output(1)
  private val aLo = intervalsA.output(0)
  private val aHi = intervalsA.output(1)
  private val bLo = intervalsB.output(0)
  private val bHi = intervalsB.output(1)

  private def join(left: LocalRelation, right: LocalRelation, cond: Expression): Join =
    Join(left, right, Inner, Some(cond), JoinHint.NONE)

  private def assertPointInRange(cond: Expression): Unit = {
    val plan = join(points, ranges, cond)
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === PointInRangeJoin)
        assert(leftKeys === Seq(p, p))
        assert(rightKeys === Seq(rLo, rHi))
      case other => fail(s"expected point-in-range, got $other")
    }
  }

  test("point-in-range: inclusive bounds, no residual") {
    assertPointInRange(And(GreaterThanOrEqual(p, rLo), LessThanOrEqual(p, rHi)))
  }

  test("point-in-range: BETWEEN lowered to a With of the point") {
    // Default spark.sql.alwaysInlineCommonExpr wraps BETWEEN as With(point).
    // The ref has no attributes; keys must be the point and the bounds.
    val cond = With(p) { case Seq(ref) =>
      And(GreaterThanOrEqual(ref, rLo), LessThanOrEqual(ref, rHi))
    }
    assertPointInRange(cond)
  }

  test("non-deterministic inequality is not a range join") {
    // The index would store one rand() and the condition would draw another.
    val cond = LessThan(Cast(aLo, DoubleType), Rand(Literal(0L)))
    assert(ExtractRangeJoinKeys.unapply(join(intervalsA, intervalsB, cond)).isEmpty)
  }

  test("non-deterministic bound is not used as a range key") {
    val randBound = Cast(Rand(Literal(0L)), IntegerType)
    val plan = join(points, ranges,
      And(GreaterThanOrEqual(p, randBound), LessThanOrEqual(p, rHi)))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === LessPartialRangeJoin)
        assert(leftKeys === Seq(p))
        assert(rightKeys === Seq(rHi))
      case other => fail(s"expected the deterministic bound only, got $other")
    }
  }

  test("non-deterministic residual stays a partial range") {
    // The rand() conjunct is first. It is not a key; a.lo < b.hi still is.
    val plan = join(intervalsA, intervalsB, And(
      GreaterThan(Rand(Literal(0L)), Literal(0.0)),
      LessThan(aLo, bHi)))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === LessPartialRangeJoin)
        assert(leftKeys === Seq(aLo))
        assert(rightKeys === Seq(bHi))
      case other => fail(s"expected partial range with a non-deterministic residual, got $other")
    }
  }

  test("non-deterministic With is not a range join") {
    val cond = With(Rand(Literal(0L))) { case Seq(ref) =>
      And(
        GreaterThanOrEqual(ref, Cast(rLo, DoubleType)),
        LessThanOrEqual(ref, Cast(rHi, DoubleType)))
    }
    assert(ExtractRangeJoinKeys.unapply(join(points, ranges, cond)).isEmpty)
  }

  test("point-in-range: exclusive bounds") {
    assertPointInRange(And(GreaterThan(p, rLo), LessThan(p, rHi)))
  }

  test("point-in-range: mixed bounds keep keys when the high conjunct is first") {
    // `p <= hi AND p > lo` is the conjunct order that used to swap inclusivity.
    // Keys must still be the point and the (lo, hi) range; inclusivity stays on
    // the original condition, which execution evaluates as-is.
    assertPointInRange(And(LessThanOrEqual(p, rHi), GreaterThan(p, rLo)))
  }

  test("point-in-range: mixed bounds keep keys when the low conjunct is first") {
    assertPointInRange(And(GreaterThanOrEqual(p, rLo), LessThan(p, rHi)))
  }

  test("point-in-range with a residual conjunct") {
    val extra = Not(EqualTo(p, rLo))
    val plan = join(points, ranges,
      And(And(GreaterThanOrEqual(p, rLo), LessThanOrEqual(p, rHi)), extra))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === PointInRangeJoin)
        assert(leftKeys === Seq(p, p))
        assert(rightKeys === Seq(rLo, rHi))
      case other => fail(s"expected point-in-range with residual, got $other")
    }
  }

  test("partial range: single inequality") {
    val plan = join(intervalsA, intervalsB, LessThan(aLo, bHi))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === LessPartialRangeJoin)
        assert(leftKeys === Seq(aLo))
        assert(rightKeys === Seq(bHi))
      case other => fail(s"expected partial range, got $other")
    }
  }

  test("partial range with a residual conjunct") {
    val extra = Not(EqualTo(aHi, bLo))
    val plan = join(intervalsA, intervalsB, And(LessThan(aLo, bHi), extra))
    plan match {
      case ExtractRangeJoinKeys(_, _, _, _, _, rangeJoin) =>
        assert(rangeJoin === LessPartialRangeJoin)
      case other => fail(s"expected partial range with residual, got $other")
    }
  }

  test("two-sided interval overlap") {
    val plan = join(intervalsA, intervalsB, And(LessThan(aLo, bHi), LessThan(bLo, aHi)))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === IntervalOverlapJoin)
        assert(leftKeys === Seq(aLo, aHi))
        assert(rightKeys === Seq(bLo, bHi))
      case other => fail(s"expected interval overlap, got $other")
    }
  }

  test("interval overlap written as greater-than") {
    val plan = join(intervalsA, intervalsB,
      And(GreaterThan(aHi, bLo), GreaterThan(bHi, aLo)))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === IntervalOverlapJoin)
        assert(leftKeys === Seq(aLo, aHi))
        assert(rightKeys === Seq(bLo, bHi))
      case other => fail(s"expected interval overlap, got $other")
    }
  }

  test("interval overlap keeps a residual conjunct") {
    val plan = join(intervalsA, intervalsB,
      And(And(LessThan(aLo, bHi), LessThan(bLo, aHi)), Not(EqualTo(aLo, bLo))))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === IntervalOverlapJoin)
        assert(leftKeys === Seq(aLo, aHi))
        assert(rightKeys === Seq(bLo, bHi))
      case other => fail(s"expected interval overlap with residual, got $other")
    }
  }

  test("array inequality is not a range join") {
    val left = LocalRelation($"a".array(IntegerType))
    val right = LocalRelation($"b".array(IntegerType))
    val plan = Join(left, right, Inner,
      Some(LessThan(left.output.head, right.output.head)), JoinHint.NONE)
    assert(ExtractRangeJoinKeys.unapply(plan).isEmpty)
  }

  test("same-side bound plus a cross-side inequality is a partial range") {
    // a.lo <= a.hi shares a.hi with a.hi <= b.lo, but the two bounds are not on
    // the other side, so this is not point-in-range. The cross-side inequality is.
    val plan = join(intervalsA, intervalsB,
      And(LessThanOrEqual(aLo, aHi), LessThanOrEqual(aHi, bLo)))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === LessPartialRangeJoin)
        assert(leftKeys === Seq(aHi))
        assert(rightKeys === Seq(bLo))
      case other => fail(s"expected partial range, got $other")
    }
  }

  test("greater-than partial range") {
    val plan = join(intervalsA, intervalsB, GreaterThan(aLo, bHi))
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === GreaterPartialRangeJoin)
        // Same columns as `a.lo < b.hi`. Direction is the tag, not a second key.
        assert(leftKeys === Seq(aLo))
        assert(rightKeys === Seq(bHi))
      case other => fail(s"expected greater partial range, got $other")
    }
  }

  test("equi-join only is not a range join") {
    val plan = join(points, ranges, EqualTo(p, rLo))
    assert(ExtractRangeJoinKeys.unapply(plan).isEmpty)
  }

  test("type-mismatched bounds are not a range join") {
    val longPoints = LocalRelation($"p".long)
    val plan = Join(longPoints, ranges, Inner,
      Some(And(GreaterThanOrEqual(longPoints.output.head, rLo),
        LessThanOrEqual(longPoints.output.head, rHi))),
      JoinHint.NONE)
    assert(ExtractRangeJoinKeys.unapply(plan).isEmpty)
  }
}
