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

import org.apache.spark.internal.Logging
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
 * @param prunedPartitionSpec optional metastore-pruned partition spec, when known
 * @param metadataOpsTimeNs time taken in nanoseconds for metadata operations
 */
class CatalogFileIndex(
    sparkSession: SparkSession,
    val table: CatalogTable,
    override val sizeInBytes: Long,
    val prunedPartitionSpec: Option[PartitionSpec] = None,
    override val metadataOpsTimeNs: Option[Long] = None) extends FileIndex with Logging {

  protected val hadoopConf: Configuration = sparkSession.sessionState.newHadoopConf()

  /** Globally shared (not exclusive to this table) cache for file statuses to speed up listing. */
  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

  assert(table.identifier.database.isDefined,
    "The table identifier must be qualified in CatalogFileIndex")

  private val baseLocation: Option[URI] = table.storage.locationUri

  override def partitionSchema: StructType =
    prunedPartitionSpec.map(_.partitionColumns).getOrElse(table.partitionSchema)

  override def rootPaths: Seq[Path] = {
    prunedPartitionSpec match {
      case Some(spec) => spec.partitions.map(_.path)
      case None => baseLocation.map(new Path(_)).toSeq
    }
  }

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
    if (table.partitionColumnNames.nonEmpty || prunedPartitionSpec.isDefined) {
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
   * Returns a [[CatalogFileIndex]] for this table restricted to the subset of partitions
   * specified by the given partition-pruning filters, without listing physical files on storage.
   */
  def filterPartitionsWithoutListing(filters: Seq[Expression]): CatalogFileIndex = {
    val (spec, timeNs) = listPartitionSpec(filters)
    new CatalogFileIndex(
      sparkSession,
      table,
      sizeInBytes,
      prunedPartitionSpec = Some(spec),
      metadataOpsTimeNs = Some(timeNs))
  }

  /**
   * Returns the partition spec matching `filters` along with the metadata operation duration.
   */
  def listPartitionSpec(filters: Seq[Expression]): (PartitionSpec, Long) = {
    val startTime = System.nanoTime()
    prunedPartitionSpec match {
      case Some(spec) =>
        val pruned = PartitioningAwareFileIndex.prunePartitionSpec(filters, spec)
        (pruned, System.nanoTime() - startTime)
      case None if table.partitionColumnNames.nonEmpty =>
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
      case _ =>
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
        case s: ScalarSubquery =>
          literalIfSubqueryReady(s)
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

  // `toLiteral` throws IllegalArgumentException("... has not finished") before the subquery
  // runs. Any other failure is a real evaluation error.
  private def literalIfSubqueryReady(subquery: ScalarSubquery): Expression = {
    try {
      subquery.toLiteral
    } catch {
      case e: IllegalArgumentException if subqueryHasNotFinished(e) => subquery
    }
  }

  private def subqueryHasNotFinished(e: IllegalArgumentException): Boolean = {
    val message = e.getMessage
    message != null && message.contains("has not finished")
  }

  private def foldConstantExpressions(expr: Expression): Expression = {
    expr.transform {
      case e if !e.isInstanceOf[Literal] && e.foldable =>
        try {
          Literal.create(e.eval(EmptyRow), e.dataType)
        } catch {
          case ex: IllegalArgumentException if subqueryHasNotFinished(ex) => e
        }
    }
  }

  override def inputFiles: Array[String] = filterPartitions(Nil).inputFiles

  // `CatalogFileIndex` may be a member of `HadoopFsRelation`, `HadoopFsRelation` may be a member
  // of `LogicalRelation`, and `LogicalRelation` may be used as the cache key. So we need to
  // implement `equals` and `hashCode` here, to make it work with cache lookup.
  override def equals(o: Any): Boolean = o match {
    case other: CatalogFileIndex =>
      this.table.identifier == other.table.identifier &&
        this.prunedPartitionSpec == other.prunedPartitionSpec
    case _ => false
  }

  override def hashCode(): Int = {
    31 * table.identifier.hashCode() + prunedPartitionSpec.hashCode()
  }
}
