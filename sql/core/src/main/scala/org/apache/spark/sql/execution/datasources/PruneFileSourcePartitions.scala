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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Prune the partitions of file source based table using partition filters. Currently, this rule
 * is applied to [[HadoopFsRelation]] with [[CatalogFileIndex]].
 *
 * For [[HadoopFsRelation]], the location will be replaced by pruned file index, and corresponding
 * statistics will be updated. And the partition filters will be kept in the filters of returned
 * logical plan.
 *
 * When a real subquery-based partition filter is present (e.g. `p <= (SELECT ...)`) and there is
 * no other, meaningful static predicate to prune on, nothing is replaced: the location stays the
 * [[CatalogFileIndex]] and the file listing is deferred to physical execution time, when the
 * subquery's result is known and can be pushed to the metastore; see
 * [[FileSourceScanLike.dynamicallySelectedPartitions]]. A classic broadcast-join dynamic
 * partition pruning filter is not visible to this rule -- it is attached later by the separate
 * `PartitionPruning` batch -- so it never triggers this deferral; that case keeps the eager,
 * pre-existing pruning behavior below.
 */
private[sql] object PruneFileSourcePartitions extends Rule[LogicalPlan] {

  private def rebuildPhysicalOperation(
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      relation: LeafNode): Project = {
    val withFilter = if (filters.nonEmpty) {
      val filterExpression = filters.reduceLeft(And)
      Filter(filterExpression, relation)
    } else {
      relation
    }
    Project(projects, withFilter)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case op @ PhysicalOperation(projects, filters,
        logicalRelation @ LogicalRelationWithTable(
          fsRelation @
            HadoopFsRelation(
            catalogFileIndex: CatalogFileIndex,
            partitionSchema,
            _,
            _,
            _,
            _),
          _)
        )
        if filters.nonEmpty && fsRelation.partitionSchema.nonEmpty =>
      val normalizedFilters = DataSourceStrategy.normalizeExprs(
        filters.filter { f =>
          f.deterministic &&
            // Python UDFs might exist because this rule is applied before ``ExtractPythonUDFs``.
            !f.exists(_.isInstanceOf[PythonUDF])
        },
        logicalRelation.output)
      val (subqueryFilters, staticFilters) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)
      val (partitionKeyFilters, _) = DataSourceUtils
        .getPartitionFiltersAndDataFilters(partitionSchema, staticFilters)
      val (subqueryPartitionKeyFilters, _) = DataSourceUtils
        .getPartitionFiltersAndDataFilters(partitionSchema, subqueryFilters)
      // A bare `IsNotNull` on a partition column is routinely added by constraint inference
      // alongside any comparison on that column -- including one buried inside a subquery filter
      // (e.g. `p <= (SELECT ...)`). Its presence alone does not mean there is a real static
      // predicate to push, so it alone must not force eager pruning while a subquery partition
      // filter is still pending: doing so would permanently replace the `CatalogFileIndex` before
      // the subquery gets a chance to defer against it. Only an actual narrowing predicate earns
      // eager pruning in that case.
      val hasMeaningfulStaticFilter = partitionKeyFilters.exists(!_.isInstanceOf[IsNotNull])

      if (partitionKeyFilters.nonEmpty &&
          (hasMeaningfulStaticFilter || subqueryPartitionKeyFilters.isEmpty)) {
        // The static filters decide the final partition set, so list the files now: join
        // reordering runs after this rule and uses the resulting file size.
        val prunedFileIndex = catalogFileIndex.filterPartitions(partitionKeyFilters)
        val prunedFsRelation =
          fsRelation.copy(location = prunedFileIndex)(fsRelation.sparkSession)
        // Change table stats based on the sizeInBytes of pruned files
        val filteredStats =
          FilterEstimation(Filter(partitionKeyFilters.reduce(And), logicalRelation)).estimate
        val colStats = filteredStats.map(_.attributeStats.map { case (attr, colStat) =>
          (attr.name, colStat.toCatalogColumnStat(attr.name, attr.dataType))
        })
        val withStats = logicalRelation.catalogTable.map(_.copy(
          stats = Some(CatalogStatistics(
            sizeInBytes = BigInt(prunedFileIndex.sizeInBytes),
            rowCount = filteredStats.flatMap(_.rowCount),
            colStats = colStats.getOrElse(Map.empty)))))
        val prunedLogicalRelation = logicalRelation.copy(
          relation = prunedFsRelation, catalogTable = withStats)
        // Keep partition-pruning predicates so that they are visible in physical planning
        rebuildPhysicalOperation(projects, filters, prunedLogicalRelation)
      } else {
        // There is no meaningful static partition-key predicate to push eagerly. If a real
        // subquery-based partition filter is pending, keep the [[CatalogFileIndex]] and let the
        // physical scan list only the surviving partitions once the subquery's result is known
        // (see [[FileSourceScanLike.dynamicallySelectedPartitions]]); if there is no partition
        // filter at all, this is simply a no-op.
        op
      }
  }
}
