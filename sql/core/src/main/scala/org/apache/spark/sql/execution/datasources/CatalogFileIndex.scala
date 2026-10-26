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
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.{InSubqueryExec, ScalarSubquery}
import org.apache.spark.sql.types.{BooleanType, StructType}


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
   *     `[min, max]` range predicate for the metastore push (a bounded call instead of an
   *     unbounded IN list), while the exact value set is kept and applied as a local
   *     post-filter on whatever the metastore returns for that range. Note this range is
   *     evaluated metastore-side using the metastore backend's own ordering, which for a
   *     string-typed partition column may not agree with the `TypeUtils.getInterpretedOrdering`
   *     (UTF-8 binary) ordering used to compute the bounds here -- the same caveat that already
   *     applies to `HiveShim`'s pre-existing InSet-to-range conversion for literal IN lists;
   *   - an empty materialized IN list short-circuits to an empty result without calling the
   *     metastore at all;
   *   - not yet executed (a dynamic partition pruning subquery still pending) or otherwise not
   *     expressible as a metastore predicate, in which case it is evaluated against the
   *     metastore's returned partitions instead.
   *
   * This two-phase execution (planning-time no-op, execution-time listing once dynamic filter
   * values are known) is specific to the Data Source V1 file-based scan path
   * ([[org.apache.spark.sql.execution.FileSourceScanLike]]); the separate native Hive SerDe scan
   * (`HiveTableScanExec`) always lists partitions eagerly and prunes them client-side via
   * `prunePartitions`. Unifying the two paths is a deliberate non-goal of this feature.
   *
   * Materializing a subquery-derived filter here means its value -- not only literals already
   * present in the query text -- is sent to the external Hive Metastore service as part of the
   * pushed predicate. Since `filters` here is every partition filter still pending on the scan,
   * not only the one that triggered deferral, a dynamic partition pruning filter from a
   * co-present broadcast join is materialized and pushed the same way; this may be undesirable
   * in a deployment with a strict trust/governance model for what reaches the metastore service.
   */
  private def listPartitionSpec(filters: Seq[Expression]): (PartitionSpec, Long) = {
    val startTime = System.nanoTime()
    if (table.partitionColumnNames.isEmpty) {
      (PartitionSpec(partitionSchema, Nil), 0L)
    } else {
      val (materializedFilters, exactFilters) = materializeDynamicFilters(filters)
      val (pushableFilters, postFilters0) = splitPartitionFilters(materializedFilters)
      val postFilters = postFilters0 ++ exactFilters
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

  // Returns the materialized filters alongside any exact predicates that must still be applied
  // locally to the metastore's returned partitions -- see `materializeInSubquery`.
  //
  // Only the `DynamicPruningExpression`-wrapped shape is materialized here: that node is always
  // injected by the `PartitionPruning` rule as a standalone top-level AND conjunct, so replacing
  // it with a literal can never change the meaning of the rest of the filter tree it sits in. A
  // bare `InSubqueryExec` (e.g. from an IN-subquery that `RewritePredicateSubquery` could not
  // rewrite into a join, surviving instead inside a surrounding `Not`/`Or`) has no such guarantee
  // -- substituting a literal in place of it can silently change which rows the surrounding
  // expression keeps. Such a filter is left untouched here and falls through to
  // `splitPartitionFilters`, which routes it to the local post-filter evaluated against the
  // metastore's returned partitions, exactly as the non-deferred path would evaluate it.
  private def materializeDynamicFilters(
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    val maxInThreshold = sparkSession.sessionState.conf.metastorePartitionPruningInSetThreshold
    val exactFilters = scala.collection.mutable.ArrayBuffer.empty[Expression]
    val materialized = filters.map { filter =>
      val transformed = filter.transformDown {
        case DynamicPruningExpression(in: InSubqueryExec) =>
          val (materializedIn, exact) = materializeInSubquery(in, maxInThreshold)
          exactFilters ++= exact
          materializedIn.getOrElse(DynamicPruningExpression(in))
        case s: ScalarSubquery if s.hasResult =>
          s.toLiteral
      }
      ConstantFolding.constantFolding(transformed)
    }
    (materialized, exactFilters.toSeq)
  }

  // `in.values()` returns `None` in two cases: the subquery has not been executed yet, or it has
  // already been executed but its result was marked unavailable (`InSubqueryExec.
  // isResultUnavailable`, e.g. a reused broadcast whose value domain was not projected) -- either
  // way there is nothing to materialize here, and the caller falls back to keeping `in` as a
  // still-dynamic filter. A failure while building the literal propagates. An IN list larger than
  // `maxInThreshold` is narrowed to a `[min, max]` range predicate for the metastore push,
  // mirroring `HiveShim`'s own InSet-pushdown convention for the same threshold -- but unlike a
  // `None`/full-fetch fallback, the range is still a pushable, bounded over-approximation. The
  // exact value set is not discarded: it is returned alongside the range so the caller applies it
  // as a local post-filter on whatever the metastore returns for the range, exactly as
  // `HiveExternalCatalog.listPartitionsByFilter` always re-applies the original, un-narrowed
  // predicate locally regardless of what was pushed.
  //
  // A multi-column (struct-typed) dynamic filter -- `child` is a `CreateNamedStruct` combining
  // several partition columns -- can never be expressed as a metastore-pushable predicate either:
  // `HiveShim.convertFilters`'s `ExtractAttribute` extractor only recognizes a plain partition
  // column `Attribute` (or an integral upcast of one) on the left-hand side, never a
  // `CreateNamedStruct`. Rather than materialize an `In(CreateNamedStruct(...), ...)` that would
  // silently fail to convert and get dropped from the pushed filter string, keep the exact
  // predicate as a local post-filter up front.
  private def materializeInSubquery(
      in: InSubqueryExec,
      maxInThreshold: Int): (Option[Expression], Seq[Expression]) = {
    val child = in.child
    in.values() match {
      case None => (None, Nil)
      case Some(arr) =>
        val distinctVals = arr.filter(_ != null).distinct
        if (distinctVals.isEmpty) {
          // Mirror `In.eval`'s own empty-list semantics (see `legacyNullInEmptyBehavior`):
          // under the legacy/default non-ANSI behavior, `child IN (<empty>)` evaluates to
          // null -- not false -- when `child` itself is null, so the materialized literal
          // must preserve that null-propagation rather than unconditionally folding to false.
          val result = if (sparkSession.sessionState.conf.legacyNullInEmptyBehavior) {
            If(IsNull(child), Literal(null, BooleanType), Literal.FalseLiteral)
          } else {
            Literal.FalseLiteral
          }
          (Some(result), Nil)
        } else if (child.isInstanceOf[CreateNamedStruct]) {
          val exact = In(child, distinctVals.toSeq.map(v => Literal.create(v, child.dataType)))
          (Some(Literal.TrueLiteral), Seq(exact))
        } else if (distinctVals.length <= maxInThreshold) {
          (Some(In(child, distinctVals.toSeq.map(v => Literal.create(v, child.dataType)))), Nil)
        } else {
          logDebug(s"Materialized IN list of ${distinctVals.length} values for " +
            s"${table.identifier} exceeds $maxInThreshold; narrowing to a min/max range " +
            s"predicate on ${child.sql} for the metastore push, and keeping the exact value " +
            "set as a local post-filter.")
          val ordering = TypeUtils.getInterpretedOrdering(child.dataType)
          val sorted = distinctVals.sorted(ordering)
          val range = And(
            GreaterThanOrEqual(child, Literal.create(sorted.head, child.dataType)),
            LessThanOrEqual(child, Literal.create(sorted.last, child.dataType)))
          (Some(range), Seq(InSet(child, distinctVals.toSet)))
        }
    }
  }

  private def splitPartitionFilters(
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    filters.partition { f =>
      f.deterministic &&
        !f.exists(_.isInstanceOf[PlanExpression[_]]) &&
        !f.exists(_.isInstanceOf[PythonUDF])
    }
  }
}
