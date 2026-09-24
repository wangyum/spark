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
import org.apache.spark.sql.types.{ArrayType, DayTimeIntervalType, DoubleType, IntegerType, PythonUserDefinedType}

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

  private def assertKeys(
      left: LocalRelation,
      right: LocalRelation,
      cond: Expression,
      expected: RangeJoin,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Unit = {
    join(left, right, cond) match {
      case ExtractRangeJoinKeys(_, _, actualLeft, actualRight, _, rangeJoin) =>
        assert(rangeJoin === expected)
        assert(actualLeft === leftKeys)
        assert(actualRight === rightKeys)
      case other => fail(s"expected $expected, got $other")
    }
  }

  private def assertUnrecognized(plan: Join): Unit = {
    assert(ExtractRangeJoinKeys.unapply(plan).isEmpty)
  }

  test("point-in-range keys are the point and the two bounds") {
    // Inclusivity and conjunct order stay on the condition. BETWEEN is a With
    // of the point when common expressions are not inlined; the ref has no
    // attributes, so the keys are still the point and the bounds.
    val inclusive = And(GreaterThanOrEqual(p, rLo), LessThanOrEqual(p, rHi))
    val exclusive = And(GreaterThan(p, rLo), LessThan(p, rHi))
    val highFirst = And(LessThanOrEqual(p, rHi), GreaterThan(p, rLo))
    val lowFirst = And(GreaterThanOrEqual(p, rLo), LessThan(p, rHi))
    val between = With(p) { case Seq(ref) =>
      And(GreaterThanOrEqual(ref, rLo), LessThanOrEqual(ref, rHi))
    }
    Seq(inclusive, exclusive, highFirst, lowFirst, between).foreach { cond =>
      assertKeys(points, ranges, cond, PointInRangeJoin, Seq(p, p), Seq(rLo, rHi))
    }
    assertKeys(
      points, ranges, And(inclusive, Not(EqualTo(p, rLo))),
      PointInRangeJoin, Seq(p, p), Seq(rLo, rHi))
  }

  test("non-deterministic expressions are not range keys") {
    // The index would store one rand() and the condition would draw another.
    assertUnrecognized(join(intervalsA, intervalsB,
      LessThan(Cast(aLo, DoubleType), Rand(Literal(0L)))))

    // Only the deterministic bound becomes a key.
    val randBound = Cast(Rand(Literal(0L)), IntegerType)
    assertKeys(
      points, ranges, And(GreaterThanOrEqual(p, randBound), LessThanOrEqual(p, rHi)),
      LessPartialRangeJoin, Seq(p), Seq(rHi))

    // A leading rand() conjunct stays residual; a.lo < b.hi is still the key.
    assertKeys(
      intervalsA, intervalsB,
      And(GreaterThan(Rand(Literal(0L)), Literal(0.0)), LessThan(aLo, bHi)),
      LessPartialRangeJoin, Seq(aLo), Seq(bHi))

    val nonDetWith = With(Rand(Literal(0L))) { case Seq(ref) =>
      And(
        GreaterThanOrEqual(ref, Cast(rLo, DoubleType)),
        LessThanOrEqual(ref, Cast(rHi, DoubleType)))
    }
    assertUnrecognized(join(points, ranges, nonDetWith))
  }

  test("interval overlap keys are (low, high) on each side") {
    val overlap = And(LessThan(aLo, bHi), LessThan(bLo, aHi))
    val writtenAsGreater = And(GreaterThan(aHi, bLo), GreaterThan(bHi, aLo))
    val withResidual = And(overlap, Not(EqualTo(aLo, bLo)))
    Seq(overlap, writtenAsGreater, withResidual).foreach { cond =>
      assertKeys(
        intervalsA, intervalsB, cond, IntervalOverlapJoin, Seq(aLo, aHi), Seq(bLo, bHi))
    }
  }

  test("one cross-side inequality is a partial range") {
    assertKeys(
      intervalsA, intervalsB, LessThan(aLo, bHi),
      LessPartialRangeJoin, Seq(aLo), Seq(bHi))
    // Same columns. Direction is the tag, not a second key.
    assertKeys(
      intervalsA, intervalsB, GreaterThan(aLo, bHi),
      GreaterPartialRangeJoin, Seq(aLo), Seq(bHi))
    assertKeys(
      intervalsA, intervalsB, And(LessThan(aLo, bHi), Not(EqualTo(aHi, bLo))),
      LessPartialRangeJoin, Seq(aLo), Seq(bHi))
    // a.lo <= a.hi shares a.hi with a.hi <= b.lo, but both bounds of the first
    // conjunct are on the left, so this is not point-in-range.
    assertKeys(
      intervalsA, intervalsB, And(LessThanOrEqual(aLo, aHi), LessThanOrEqual(aHi, bLo)),
      LessPartialRangeJoin, Seq(aHi), Seq(bLo))
  }

  test("a UDT is a range key only when its sql type is indexed") {
    val intUdt = new PythonUserDefinedType(IntegerType, "py", "ser")
    val nested = new PythonUserDefinedType(intUdt, "outer", "outer")
    val arrayUdt = new PythonUserDefinedType(ArrayType(IntegerType), "py", "ser")
    assert(RangePredicate.supportedType(nested))
    assert(!RangePredicate.supportedType(arrayUdt))

    val left = LocalRelation(AttributeReference("a", nested)())
    val right = LocalRelation(AttributeReference("b", nested)())
    val plan = Join(left, right, Inner,
      Some(LessThan(left.output.head, right.output.head)), JoinHint.NONE)
    plan match {
      case ExtractRangeJoinKeys(_, _, leftKeys, rightKeys, _, rangeJoin) =>
        assert(rangeJoin === LessPartialRangeJoin)
        assert(leftKeys === Seq(left.output.head))
        assert(rightKeys === Seq(right.output.head))
      case other => fail(s"expected partial range on a UDT key, got $other")
    }
  }

  test("equality, mismatched types, and non-indexed types are not range joins") {
    assertUnrecognized(join(points, ranges, EqualTo(p, rLo)))

    val longPoints = LocalRelation($"p".long)
    assertUnrecognized(Join(longPoints, ranges, Inner,
      Some(And(
        GreaterThanOrEqual(longPoints.output.head, rLo),
        LessThanOrEqual(longPoints.output.head, rHi))),
      JoinHint.NONE))

    val arraysL = LocalRelation($"a".array(IntegerType))
    val arraysR = LocalRelation($"b".array(IntegerType))
    assertUnrecognized(Join(arraysL, arraysR, Inner,
      Some(LessThan(arraysL.output.head, arraysR.output.head)), JoinHint.NONE))

    val interval = DayTimeIntervalType()
    val left = LocalRelation(AttributeReference("x", interval)())
    val right = LocalRelation(AttributeReference("y", interval)())
    assert(!RangePredicate.supportedType(interval))
    assertUnrecognized(Join(left, right, Inner,
      Some(LessThan(left.output.head, right.output.head)), JoinHint.NONE))
  }
}
