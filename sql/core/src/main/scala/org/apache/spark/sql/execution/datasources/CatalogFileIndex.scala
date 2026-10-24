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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.{InSubqueryExec, ScalarSubquery}
import org.apache.spark.sql.types.StructType


/**
 * A [[FileIndex]] for a metastore catalog table.
 *
 * @param sparkSession a [[SparkSession]]
 * @param table the metadata of the table
 * @param sizeInBytes the table's data size in bytes
 */
class CatalogFileIndex(
    sparkSession: SparkSession,
    val table: CatalogTable,
    override val sizeInBytes: Long) extends FileIndex with Logging {

  protected val hadoopConf: Configuration = sparkSession.sessionState.newHadoopConf()

  /** Globally shared (not exclusive to this table) cache for file statuses to speed up listing. */
  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

  assert(table.identifier.database.isDefined,
    "The table identifier must be qualified in CatalogFileIndex")

  private val baseLocation: Option[URI] = table.storage.locationUri

  override def partitionSchema: StructType = table.partitionSchema

  override def rootPaths: Seq[Path] = baseLocation.map(new Path(_)).toSeq

  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    filterPartitions(partitionFilters).listFiles(Nil, dataFilters)
  }

  override def refresh(): Unit = fileStatusCache.invalidateAll()

  /**
   * Returns a [[InMemoryFileIndex]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters.
   *
   * @param filters partition-pruning filters
   */
  def filterPartitions(filters: Seq[Expression]): InMemoryFileIndex = {
    if (table.partitionColumnNames.nonEmpty) {
      val (partitionSpec, timeNs) = listPartitionSpec(filters)
      new InMemoryFileIndex(sparkSession,
        rootPathsSpecified = partitionSpec.partitions.map(_.path),
        parameters = Map.empty,
        userSpecifiedSchema = Some(partitionSpec.partitionColumns),
        fileStatusCache = fileStatusCache,
        userSpecifiedPartitionSpec = Some(partitionSpec),
        metadataOpsTimeNs = Some(timeNs))
    } else {
      new InMemoryFileIndex(sparkSession, rootPaths, parameters = table.storage.properties,
        userSpecifiedSchema = None, fileStatusCache = fileStatusCache)
    }
  }

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles

  // `CatalogFileIndex` may be a member of `HadoopFsRelation`, `HadoopFsRelation` may be a member
  // of `LogicalRelation`, and `LogicalRelation` may be used as the cache key. So we need to
  // implement `equals` and `hashCode` here, to make it work with cache lookup.
  override def equals(o: Any): Boolean = o match {
    case other: CatalogFileIndex => this.table.identifier == other.table.identifier
    case _ => false
  }

  override def hashCode(): Int = table.identifier.hashCode()

  /**
   * Returns the partition spec matching `filters` along with the metadata operation duration.
   *
   * Subquery expressions that already carry their result are first turned into literals (see
   * `materializeDynamicFilters`), so they can be pushed to the metastore together with the
   * static filters. Each filter ends up in one of these states:
   *   - pushed to the metastore as an exact predicate (static filters, small materialized IN
   *     lists, folded scalar subqueries);
   *   - an IN list larger than `metastorePartitionPruningInSetThreshold` is narrowed to a
   *     `[min, max]` range predicate before being pushed, trading exactness for a bounded
   *     metastore call;
   *   - an empty materialized IN list short-circuits to an empty result without calling the
   *     metastore at all;
   *   - not yet executed (a dynamic partition pruning subquery still pending) or otherwise not
   *     expressible as a metastore predicate, in which case it is evaluated against the
   *     metastore's returned partitions instead.
   *
   * This two-phase execution (planning-time no-op, execution-time listing once dynamic filter
   * values are known) is specific to the Data Source V1 file-based scan path
   * ([[FileSourceScanLike]]); the separate native Hive SerDe scan (`HiveTableScanExec`) always
   * lists partitions eagerly and prunes them client-side via `prunePartitions`. Unifying the two
   * paths is a deliberate non-goal of this feature.
   */
  private[sql] def listPartitionSpec(filters: Seq[Expression]): (PartitionSpec, Long) = {
    val startTime = System.nanoTime()
    if (table.partitionColumnNames.isEmpty) {
      (PartitionSpec(partitionSchema, Nil), 0L)
    } else {
      val materializedFilters = materializeDynamicFilters(filters)
      val (pushableFilters, postFilters) = splitPartitionFilters(materializedFilters)
      val partitions = if (pushableFilters.contains(Literal.FalseLiteral)) {
        logDebug(s"Materialized filters for table ${table.identifier} are unsatisfiable; " +
          "skipping the metastore call and returning an empty partition spec.")
        Nil
      } else {
        logDebug(s"Listing partitions for table ${table.identifier} with " +
          s"${pushableFilters.size} filter(s) pushed to the metastore and " +
          s"${postFilters.size} filter(s) evaluated locally.")
        val selectedPartitions = ExternalCatalogUtils.listPartitionsByFilter(
          sparkSession.sessionState.conf, sparkSession.sessionState.catalog, table,
          pushableFilters)
        val timeZone = sparkSession.sessionState.conf.sessionLocalTimeZone
        val filtered = if (postFilters.nonEmpty) {
          val boundPredicate = PartitioningAwareFileIndex.bindPartitionPredicate(
            postFilters.reduce(And), partitionSchema)
          selectedPartitions.flatMap { p =>
            val row = p.toRow(partitionSchema, timeZone)
            if (boundPredicate.eval(row)) Some((row, p)) else None
          }
        } else {
          selectedPartitions.map(p => (p.toRow(partitionSchema, timeZone), p))
        }
        filtered.map { case (row, p) =>
          val path = new Path(p.location)
          val fs = path.getFileSystem(hadoopConf)
          PartitionPath(row, path.makeQualified(fs.getUri, fs.getWorkingDirectory))
        }
      }
      (PartitionSpec(partitionSchema, partitions), System.nanoTime() - startTime)
    }
  }

  private def splitPartitionFilters(
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    filters.partition { f =>
      f.deterministic &&
        !SubqueryExpression.hasSubquery(f) &&
        !f.exists(_.isInstanceOf[PlanExpression[_]]) &&
        !f.exists(_.isInstanceOf[PythonUDF])
    }
  }

  private def materializeDynamicFilters(
      filters: Seq[Expression]): Seq[Expression] = {
    val maxInThreshold = sparkSession.sessionState.conf.metastorePartitionPruningInSetThreshold
    filters.map { filter =>
      val materialized = filter.transformDown {
        case DynamicPruningExpression(in: InSubqueryExec) =>
          materializeInSubquery(in, maxInThreshold).getOrElse(DynamicPruningExpression(in))
        case s: ScalarSubquery if s.hasResult =>
          s.toLiteral
        case in: InSubqueryExec =>
          materializeInSubquery(in, maxInThreshold).getOrElse(in)
      }
      foldConstantExpressions(materialized)
    }
  }

  // `None` only when the subquery has not been executed yet. A failure while building the
  // literal propagates. An IN list larger than `maxInThreshold` is narrowed to a `[min, max]`
  // range predicate instead of giving up, mirroring `HiveShim`'s own InSet-pushdown convention
  // for the same threshold: the range is a safe, bounded over-approximation that the metastore
  // can still push down, trading exactness for avoiding an unbounded IN-list round trip.
  private def materializeInSubquery(
      in: InSubqueryExec,
      maxInThreshold: Int): Option[Expression] = {
    val child = in.child
    in.values().flatMap { arr =>
      val distinctVals = arr.filter(_ != null).distinct
      if (distinctVals.isEmpty) {
        Some(Literal.FalseLiteral)
      } else if (distinctVals.length <= maxInThreshold) {
        Some(In(child, distinctVals.toSeq.map(v => Literal.create(v, child.dataType))))
      } else {
        logDebug(s"Materialized IN list of ${distinctVals.length} values for " +
          s"${table.identifier} exceeds $maxInThreshold; narrowing to a min/max range " +
          s"predicate on ${child.sql}.")
        val ordering = TypeUtils.getInterpretedOrdering(child.dataType)
        val sorted = distinctVals.sorted(ordering)
        Some(And(
          GreaterThanOrEqual(child, Literal.create(sorted.head, child.dataType)),
          LessThanOrEqual(child, Literal.create(sorted.last, child.dataType))))
      }
    }
  }

  private def foldConstantExpressions(expr: Expression): Expression = {
    expr.transform {
      case e if !e.isInstanceOf[Literal] && e.foldable =>
        Literal.create(e.eval(EmptyRow), e.dataType)
    }
  }
}
