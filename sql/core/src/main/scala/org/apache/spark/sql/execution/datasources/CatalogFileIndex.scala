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
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions._
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
    override val sizeInBytes: Long) extends FileIndex {

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

  /**
   * Returns the partition spec matching `filters` along with the metadata operation duration.
   * Subquery expressions that already carry their result are first turned into literals, so
   * they can be pushed to the metastore together with the static filters; those that do not
   * (not yet executed, or an IN list too large to push) are evaluated against the returned
   * partitions instead.
   */
  def listPartitionSpec(filters: Seq[Expression]): (PartitionSpec, Long) = {
    val startTime = System.nanoTime()
    if (table.partitionColumnNames.nonEmpty) {
      val materializedFilters = materializeDynamicFilters(filters)
      val (pushableFilters, postFilters) = splitPartitionFilters(materializedFilters)
      val selectedPartitions = ExternalCatalogUtils.listPartitionsByFilter(
        sparkSession.sessionState.conf, sparkSession.sessionState.catalog, table, pushableFilters)
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
      var cachedFs: FileSystem = null
      val partitions = filtered.map { case (row, p) =>
        val path = new Path(p.location)
        val uri = path.toUri
        val fs = if (cachedFs != null &&
            (uri.getScheme == null || uri.getScheme == cachedFs.getScheme) &&
            (uri.getAuthority == null || uri.getAuthority == cachedFs.getUri.getAuthority)) {
          cachedFs
        } else {
          cachedFs = path.getFileSystem(hadoopConf)
          cachedFs
        }
        PartitionPath(row, path.makeQualified(fs.getUri, fs.getWorkingDirectory))
      }
      (PartitionSpec(partitionSchema, partitions), System.nanoTime() - startTime)
    } else {
      (PartitionSpec(partitionSchema, Nil), 0L)
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
          materializeInSubquery(in, in.child, maxInThreshold)
            .getOrElse(DynamicPruningExpression(in))
        case s: ScalarSubquery if s.hasResult =>
          s.toLiteral
        case in: InSubqueryExec =>
          materializeInSubquery(in, in.child, maxInThreshold).getOrElse(in)
      }
      foldConstantExpressions(materialized)
    }
  }

  // `None` only when the subquery has not been executed yet, or its literal form is too
  // large to push into the metastore. A failure while building the literal propagates.
  private def materializeInSubquery(
      in: InSubqueryExec,
      child: Expression,
      maxInThreshold: Int): Option[Expression] = {
    in.values().flatMap { arr =>
      val distinctVals = arr.filter(_ != null).distinct
      if (distinctVals.isEmpty) {
        Some(Literal.FalseLiteral)
      } else if (distinctVals.length <= maxInThreshold) {
        Some(In(child, distinctVals.toSeq.map(v => Literal.create(v, child.dataType))))
      } else {
        None
      }
    }
  }

  private def foldConstantExpressions(expr: Expression): Expression = {
    expr.transform {
      case e if !e.isInstanceOf[Literal] && e.foldable =>
        Literal.create(e.eval(EmptyRow), e.dataType)
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
}
