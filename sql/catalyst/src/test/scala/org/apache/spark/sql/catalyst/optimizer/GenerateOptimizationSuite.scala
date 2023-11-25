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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Explode, MapKeys, MapValues}
import org.apache.spark.sql.catalyst.optimizer.NestedColumnAliasingSuite.collectGeneratedAliases
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{StringType, StructType}

class GenerateOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("GenerateOptimization", FixedPoint(100),
      ColumnPruning,
      CollapseProject,
      GenerateOptimization) :: Nil
  }

  private val item = StructType.fromDDL("item_id int, item_data string, item_price int")
  private val relation = LocalRelation(
    $"key".string,
    $"items".array(item),
    Symbol("kvs").map(StringType, StringType))

  test("Prune unnecessary field on Explode from count-only aggregate") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("explode"))
      .select($"explode")
      .groupBy()(count(1))
      .analyze

    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = relation
      .select(
        $"items".getField("item_id").as(aliases(0)))
      .generate(Explode($"${aliases(0)}"),
        unrequiredChildIndex = Seq(0),
        outputNames = Seq("explode"))
      .select()
      .groupBy()(count(1))
      .analyze
    comparePlans(optimized, expected)
  }

  test("Do not prune field from Explode if the struct is needed") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("explode"))
      .select($"explode")
      .groupBy()(count(1), collectList($"explode"))
      .analyze

    val optimized = Optimize.execute(query)

    val expected = relation
      .select($"items")
      .generate(Explode($"items"), unrequiredChildIndex = Seq(0), outputNames = Seq("explode"))
      .select($"explode")
      .groupBy()(count(1), collectList($"explode"))
      .analyze

    comparePlans(optimized, expected)
  }

  test("Prune values if only referenced by keys") {
    val query = relation
      .generate(Explode($"kvs"), outer = true, outputNames = Seq("map_key", "map_value"))
      .select($"map_key")
      .analyze
    val expected = relation
      .select(MapKeys($"kvs").as("keys"))
      .generate(Explode($"keys"), Seq(0), outer = true, outputNames = Seq("map_key"))
      .select($"map_key")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("Prune keys if only referenced by values") {
    val query = relation
      .generate(Explode($"kvs"), outputNames = Seq("map_key", "map_value"))
      .select($"map_value")
      .analyze
    val expected = relation
      .select(MapValues($"kvs").as("values"))
      .generate(Explode($"values"), Seq(0), outputNames = Seq("map_value"))
      .select($"map_value")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("Do not prune if referenced by keys and values") {
    val query = relation
      .generate(Explode($"kvs"), outer = true, outputNames = Seq("map_key", "map_value"))
      .select($"map_key", $"map_value")
      .analyze

    val expected = relation
      .select($"kvs")
      .generate(Explode($"kvs"), Seq(0), outer = true, outputNames = Seq("map_key", "map_value"))
      .select($"map_key", $"map_value")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }
}
