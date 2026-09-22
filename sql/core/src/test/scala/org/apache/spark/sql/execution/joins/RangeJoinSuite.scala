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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.{PointInRangeJoin, RangeEquality}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

class RangeJoinSuite extends QueryTest with SharedSparkSession {
  private lazy val intervals1: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(-1, 0),
      Row(0, 1),
      Row(0, 2),
      Row(1, 5)
    )), new StructType().add("low1", IntegerType).add("high1", IntegerType))

  private lazy val points: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(-3),
      Row(1),
      Row(3),
      Row(6)
    )), new StructType().add("point", IntegerType))

  test("interval-point range join") {
    /* low1 <= point && point < high1 */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      val rangeInfo = RangeInfo(BuildRight,
        None,
        Seq.empty[Attribute],
        right.output.head :: right.output.head :: Nil,
        right.output,
        left.output,
        left.output,
        RangeEquality(lowInclusive = true, highInclusive = false),
        PointInRangeJoin,
        true)
      BroadcastRangeJoinExec(
        left,
        right,
        BuildRight,
        Inner,
        rangeInfo)
    },
      Seq(
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    /* low1 <= point && point < high1 */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      val rangeInfo = RangeInfo(BuildRight,
        None,
        Seq.empty[Attribute],
        right.output.head :: right.output.head :: Nil,
        right.output,
        left.output,
        left.output,
        RangeEquality(lowInclusive = false, highInclusive = false), PointInRangeJoin,
        true)
      BroadcastRangeJoinExec(
        left,
        right,
        BuildRight,
        Inner,
        rangeInfo)
    },
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    /* low <= point && point <= high1 */
    checkAnswer2(points, intervals1, (left: SparkPlan, right: SparkPlan) => {
      val rangeInfo = RangeInfo(BuildRight,
        None,
        Seq.empty[Attribute],
        right.output,
        right.output,
        left.output.head :: left.output.head :: Nil,
        left.output,
        RangeEquality(lowInclusive = true, highInclusive = true), PointInRangeJoin,
        false)
      BroadcastRangeJoinExec(
        left,
        right,
        BuildRight,
        Inner,
        rangeInfo)
    },
      Seq(
        (1, 0, 1),
        (1, 0, 2),
        (1, 1, 5),
        (3, 1, 5)
      ).map(Row.fromTuple))

    /* low1 < point && point < high1, build side is the left (interval) relation */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      val rangeInfo = RangeInfo(BuildLeft,
        None,
        Seq.empty[Attribute],
        left.output,
        left.output,
        right.output.head :: right.output.head :: Nil,
        right.output,
        RangeEquality(lowInclusive = false, highInclusive = false), PointInRangeJoin,
        false)
      BroadcastRangeJoinExec(
        left,
        right,
        BuildLeft,
        Inner,
        rangeInfo)
    },
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    /* low1 <= point && point < high1, build side is the left (interval) relation. Exercises
     * asymmetric equality (low-inclusive, high-exclusive) through BuildLeft, which reverses the
     * equality flags before building the range index -- BuildRight cases above only cover the
     * unreversed path. */
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) => {
      val rangeInfo = RangeInfo(BuildLeft,
        None,
        Seq.empty[Attribute],
        left.output,
        left.output,
        right.output.head :: right.output.head :: Nil,
        right.output,
        RangeEquality(lowInclusive = true, highInclusive = false), PointInRangeJoin,
        false)
      BroadcastRangeJoinExec(
        left,
        right,
        BuildLeft,
        Inner,
        rangeInfo)
    },
      Seq(
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))
  }
}
