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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, BinaryComparison, Cast, Descending, Expression, GreaterThan, InSubquery, Literal, Not, PredicateHelper, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LeafNode, Limit, LogicalPlan, Project, Sort, Window}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DoubleType, StringType}

case class ExplainOptimizeCommand(logicalPlan: LogicalPlan)
  extends RunnableCommand with PredicateHelper with JoinSelectionHelper  {

  private def trackLineageDown(
      exp: Expression,
      plan: LogicalPlan): Option[(Expression, LogicalPlan)] = plan match {
    case Project(projectList, child) =>
      val aliases = AttributeMap(projectList.collect {
        case a @ Alias(child, _) => (a.toAttribute, child)
      })
      trackLineageDown(replaceAlias(exp, aliases), child)
    // we can unwrap only if there are row projections, and no aggregation operation
    case Aggregate(_, aggregateExpressions, child) =>
      val aliasMap = AttributeMap(aggregateExpressions.collect {
        case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
          (a.toAttribute, a.child)
      })
      trackLineageDown(replaceAlias(exp, aliasMap), child)
    case f @ Filter(_, l: LeafNode)
      if exp.references.subsetOf(f.outputSet) && exp.references.subsetOf(l.outputSet) =>
      Some((exp, f))
    case l: LeafNode if exp.references.subsetOf(l.outputSet) =>
      Some((exp, l))
    case other =>
      other.children.flatMap {
        child => if (exp.references.subsetOf(child.outputSet)) {
          trackLineageDown(exp, child)
        } else {
          None
        }
      }.headOption
  }

  private def getTableName(logicalPlan: LogicalPlan): Option[String] = logicalPlan match {
    case l: LogicalRelation => l.catalogTable.map(_.qualifiedName)
    case other => other.children.flatMap(child => getTableName(child)).headOption
  }

  private def trackLineageDown(exp: Expression, plan: SparkPlan): Option[Expression] = plan match {
    case f: FileSourceScanExec =>
      f.outputPartitioning match {
        case HashPartitioning(expressions, _) =>
          expressions.find(_.references.equals(exp.references))
        case _ => None
      }
    case other => other.children.flatMap(child => trackLineageDown(exp, child)).headOption
  }

  private def getBucketReadTips(plan: SparkPlan): Seq[String] = plan match {
    case ShuffleExchangeExec(h: HashPartitioning, child, _) =>
      h.expressions.flatMap { e =>
        trackLineageDown(e, child) match {
          case Some(o) if o.dataType != e.dataType =>
            Seq(s"${" " * 4}The data type do not match for bucket read, " +
              s"consider cast ${o.sql} to ${o.dataType.catalogString}.")
          case _ => Nil
        }
      }
    case other => other.children.flatMap { child =>
      getBucketReadTips(child)
    }
  }

  private def getDistinctCount(
      exps: Seq[Expression],
      plan: LogicalPlan,
      sparkSession: SparkSession): Seq[String] = {
    exps.flatMap(exp => trackLineageDown(exp, plan)).groupBy(_._2).flatMap {
      case (plan, exps) =>
        val distinctAggExprs =
          Alias(Count(Literal(1)).toAggregateExpression(isDistinct = false), "count")()

        val alias = exps.map(_._1).map(k => Alias(k, k.toString)())
        val aggregate = Aggregate(alias, alias ++ Seq(distinctAggExprs), plan)

        val filter = Filter(GreaterThan(distinctAggExprs.toAttribute, Literal(1)), aggregate)
        val sortOrder = SortOrder(distinctAggExprs.toAttribute, Descending)

        val sort = Sort(Seq(sortOrder), global = true, filter)
        val limit = Limit(Literal(5), sort)

        val result = sparkSession.sessionState.executePlan(limit).executedPlan.executeCollect()
        if (result.nonEmpty) {
          val numFields = Seq.range(0, result.head.numFields)
          val res = result.map { r =>
            " " * 6 ++ numFields.map(n => r.get(n, limit.output(n).dataType)).mkString(", ")
          }

          Seq(" " * 4 + "table: " + getTableName(plan).getOrElse("")) ++
            Seq(" " * 6 + (exps.map(_._1).map(_.references.map(_.name).mkString(" ")) ++
              Seq(distinctAggExprs.name)).mkString(", ")) ++ res
        } else {
          Nil
        }
    }.toSeq
  }

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {

    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    val analyzedPlan = qe.analyzed
    val optimizedPlan = qe.optimizedPlan

    // Check join data skew and expand
    val joins = optimizedPlan.collect {
      case j @ Join(_, _, _, Some(_), _) => j
    }

    // Check window function data skew
    val windows = optimizedPlan.collect {
      case w @ Window(_, _, _, _) => w
    }

    val joinDataSkewTips = joins.flatMap {
      case j @ Join(_, _, _, Some(condition), _) =>
        val joinCondition = splitConjunctivePredicates(condition).flatMap {
          case BinaryComparison(l, r) =>
            Seq(l, r)
          case _ => Nil
        }

        Seq(s"${" " * 2}Check join: ${j.simpleString(60)}") ++
          getDistinctCount(joinCondition, j, sparkSession)
      case _ => Nil
    }

    val windowDataSkewTips = windows.flatMap {
      case w @ Window(_, partitionSpec, _, child) =>
        Seq(s"${" " * 2}Check window: ${w.simpleString(60)}") ++
          getDistinctCount(partitionSpec, child, sparkSession)
      case _ => Nil
    }

    // Check join type(string to double, see SPARK-29274)
    val joinTypeTips = analyzedPlan.collect {
      case j @ Join(_, _, _, Some(_), _) => j
    }.flatMap {
      case j @ Join(_, _, _, Some(condition), _) =>
        val joinCondition = splitConjunctivePredicates(condition).flatMap {
          case BinaryComparison(l, r) =>
            Seq(l, r)
          case _ => Nil
        }
        val castToDoubles = joinCondition.filter {
          case Cast(child, DoubleType, _) if child.dataType == StringType => true
          case _ => false
        }
        if (castToDoubles.nonEmpty) {
          Seq(s"${" " * 2}Check join: ${j.simpleString(60)}") ++
            joinCondition.map(s => " " * 4 ++ s.sql)
        } else {
          Nil
        }
      case _ => Nil
    }

    // NOT EXISTS instead of NOT IN
    val checkUseNotExists = analyzedPlan.collect {
      case f @ Filter(Not(InSubquery(_, _)), _) => f
    } match {
      case Nil =>
        Nil
      case other =>
        Seq(s"${" " * 2}There are ${other.size} NOT IN subquery, " +
          "consider rewrite it to NOT EXISTS.")
    }

    // Check avoid shuffle
    val bucketReadTips = qe.executedPlan.collect {
      case j @ SortMergeJoinExec(_, _, _, _, _, _, _) =>
        j
    }.flatMap {
      case j @ SortMergeJoinExec(_, _, _, _, left, right, _) =>
        Seq(s"${" " * 2}Check join: ${j.simpleString(60)}") ++
          getBucketReadTips(left) ++ getBucketReadTips(right)
    }

    // Check sensitive config
    val confTips = if (qe.executedPlan.conf.getConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD) <
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get) {
      Seq(s"${" " * 2}Please do not set ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}.")
    } else {
      Nil
    }

    // TODO: Check range join

    (Seq("1. Check join data skew") ++ joinDataSkewTips ++ Seq("") ++
      Seq("2. Check window function data skew") ++ windowDataSkewTips ++ Seq("") ++
      Seq("3. Check join condition") ++ joinTypeTips ++ Seq("") ++
      Seq("4. NOT EXISTS instead of NOT IN") ++ checkUseNotExists ++ Seq("") ++
      Seq("5. Check bucket read") ++ bucketReadTips ++ Seq("") ++
      Seq("6. Check sensitive config") ++ confTips).map(Row(_))

  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}
