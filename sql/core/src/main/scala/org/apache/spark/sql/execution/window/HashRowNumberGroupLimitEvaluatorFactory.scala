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

package org.apache.spark.sql.execution.window

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences,
  BoundReference, Expression, GenericInternalRow, MutableProjection, RowOrdering, SortOrder,
  SortPrefix, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{SortPrefixUtils, UnsafeExternalRowSorter,
  UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.unsafe.KVIterator

/**
 * One task's worth of state for the hash-based top-1 `ROW_NUMBER()` accumulation described on
 * [[WindowGroupLimitExec.isHashEligible]]. Used by both the interpreted path
 * ([[HashRowNumberGroupLimitEvaluatorFactory]]) and [[WindowGroupLimitExec]]'s whole-stage codegen
 * (`doConsume`/`doProduce`), so the two paths share one implementation instead of two that could
 * drift apart.
 *
 * Usage: call [[consumeRow]] once per input row (order doesn't matter, unlike the sort-based
 * iterators), then call [[finish]] exactly once to get the output rows.
 *
 * For each partition key it keeps only the single best-so-far row (by `orderSpec`) in a
 * [[UnsafeFixedWidthAggregationMap]]-backed hash map. The aggregation buffer stores `orderSpec`'s
 * values (one field per order column, every one of which [[WindowGroupLimitExec.isHashEligible]]
 * already guarantees is `UnsafeRow`-mutable) plus a trailing `winnerAddress` field: a pointer into
 * `rowArena` ([[UnsafeRowArena]]), which holds the actual best-so-far row for each key in
 * Tungsten-managed pages (full rows are generally variable-width, so they cannot be the map's own
 * fixed-width value). The map is only ever used to decide "is this row better than the current
 * winner for its key, and if so where is its data" -- it never stores the row bytes itself.
 *
 * Falls back from hash accumulation to sorting -- the same [[UnsafeExternalRowSorter]]
 * [[org.apache.spark.sql.execution.SortExec]] itself uses, drained through the existing,
 * already-correct [[GroupedLimitIterator]]/[[SimpleLimitIterator]] -- for three reasons:
 *   - the hash map could no longer acquire memory for a key's fixed-width entry
 *     ([[switchToFallback]]); or
 *   - `rowArena` could no longer acquire memory for a row's bytes (also [[switchToFallback]],
 *     same trigger, see [[consumeRow]]); or
 *   - periodically, the ratio of rows processed to distinct keys seen falls below
 *     `minCompactionRatio`, meaning hash accumulation isn't pruning enough duplicate rows to be
 *     worth its per-row overhead over just sorting ([[switchToFallbackProactively]]) -- mirrors
 *     `HashAggregateExec`'s adaptive partial aggregation, which uses the same compaction-ratio
 *     heuristic (`spark.sql.execution.aggregate.adaptivePartialAggregation.*`) for the same
 *     reason, against a different (and already-existing, OOM-tested) fallback mechanism here.
 */
class HashRowNumberGroupLimitAccumulator(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric,
    numTasksFallBacked: SQLMetric,
    minCompactionRatio: Double = 1.05,
    compactionCheckInterval: Long = 100000L,
    enableRadixSort: Boolean = true) extends Logging {

  private val numOrderCols = orderSpec.length

  // One field per order column, plus a trailing pointer into `rowArena`.
  private val bufferSchema = StructType(
    orderSpec.zipWithIndex.map { case (order, i) =>
      StructField(s"o$i", order.child.dataType, order.child.nullable)
    }.toArray :+ StructField("winnerAddress", LongType, nullable = false))

  private val groupingKeySchema = StructType(partitionSpec.zipWithIndex.map { case (e, i) =>
    StructField(s"k$i", e.dataType, e.nullable)
  }.toArray)

  // -1L in the trailing field marks "key inserted, but no winner successfully recorded yet" --
  // either genuinely just-inserted, or left behind by a row whose rowArena.append failed (see
  // consumeRow): in both cases there is no valid row at this key to emit or drain, so finish()
  // and buildFallbackSorter() must skip it.
  private val emptyBuffer: InternalRow = {
    val values = new Array[Any](numOrderCols + 1)
    values(numOrderCols) = -1L
    UnsafeProjection.create(bufferSchema).apply(new GenericInternalRow(values))
  }

  private val groupingProjection = UnsafeProjection.create(partitionSpec, childOutput)
  private val orderKeyProjection = UnsafeProjection.create(orderSpec.map(_.child), childOutput)

  // Writes orderSpec's values (evaluated against childOutput) into the leading fields of
  // whatever row `.target(...)` was last called with; never touches the trailing winnerAddress
  // field, since it was only given numOrderCols expressions.
  private val bufferUpdater: MutableProjection =
    MutableProjection.create(orderSpec.map(_.child), childOutput)

  // Compares an orderKeyProjection-shaped row (numOrderCols fields) against a buffer row
  // (numOrderCols + 1 fields) by reading ordinals 0 until numOrderCols from each side
  // independently -- safe even though the two rows have different total field counts, since
  // the trailing winnerAddress field is never read by this ordering.
  private val boundOrdering =
    GenerateOrdering.generate(orderSpec.zipWithIndex.map { case (order, i) =>
      order.copy(child = BoundReference(i, order.child.dataType, order.child.nullable))
    })

  private val taskMemoryManager = TaskContext.get().taskMemoryManager()

  private val map = new UnsafeFixedWidthAggregationMap(
    emptyBuffer,
    bufferSchema,
    groupingKeySchema,
    TaskContext.get(),
    1024 * 16,
    taskMemoryManager.pageSizeBytes)

  // Holds the actual candidate rows in Tungsten pages (see UnsafeRowArena's class doc for why this
  // replaced a plain JVM-heap `ArrayBuffer[UnsafeRow]`: avoids unaccounted heap allocation and GC
  // pressure on every winner/replacement, and makes this memory visible to the same backpressure
  // signal (SparkOutOfMemoryError) the map itself already reacts to).
  private var rowArena: UnsafeRowArena = new UnsafeRowArena(taskMemoryManager)

  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  // Rows consumed by the hash path (not counting rows already routed to fallbackSorter), used
  // only to decide when to run the next periodic compaction-ratio check.
  private var rowsConsumed: Long = 0L
  private var nextCompactionCheckAt: Long = compactionCheckInterval

  // Non-null once the map or the row arena could no longer acquire memory, or the compaction
  // ratio proved too low; from then on every row -- including ones already recorded as winners --
  // is routed here instead, and the hash map/row arena are abandoned.
  private var fallbackSorter: UnsafeExternalRowSorter = null

  /** Consumes one input row. Safe to call in any order; the hash path doesn't need sorted input. */
  def consumeRow(row: UnsafeRow): Unit = {
    if (fallbackSorter != null) {
      fallbackSorter.insertRow(row)
      return
    }
    val key = groupingProjection(row)
    val buffer = map.getAggregationBufferFromUnsafeRow(key)
    if (buffer == null) {
      switchToFallback(row)
      return
    } else if (buffer.getLong(numOrderCols) == -1L) {
      storeWinner(row, buffer)
      if (fallbackSorter != null) return
    } else {
      val orderKey = orderKeyProjection(row)
      if (boundOrdering.compare(orderKey, buffer) < 0) {
        storeWinner(row, buffer)
        if (fallbackSorter != null) return
      }
    }
    rowsConsumed += 1
    if (compactionCheckInterval > 0 && rowsConsumed >= nextCompactionCheckAt) {
      maybeBailOutForLowCompaction()
      nextCompactionCheckAt = rowsConsumed + compactionCheckInterval
    }
  }

  /**
   * Appends `row` to `rowArena` and points `buffer`'s order-key fields and winnerAddress at it.
   * Appends *before* touching `buffer`: if `rowArena.append` fails, `buffer` must be left exactly
   * as it was (either the untouched -1L sentinel for a new key, or the previous winner's valid
   * state for a replacement), since [[switchToFallback]] -- reached right after this throws --
   * drains the map's current, still-consistent state as part of its fallback seed set.
   */
  private def storeWinner(row: UnsafeRow, buffer: UnsafeRow): Unit = {
    val address =
      try {
        rowArena.append(row)
      } catch {
        case _: SparkOutOfMemoryError =>
          switchToFallback(row)
          return
      }
    bufferUpdater.target(buffer)(row)
    buffer.setLong(numOrderCols, address)
  }

  /** Call once, after every row has been passed to [[consumeRow]], to get the output rows. */
  def finish(): Iterator[InternalRow] = {
    if (fallbackSorter != null) {
      peakMemory.set(math.max(peakMemory.value, fallbackSorter.getPeakMemoryUsage))
      spillSize.set(
        TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      val sortedIter = fallbackSorter.sort()
      val limitFunc = (iter: Iterator[InternalRow]) => SimpleLimitIterator(iter, 1, numOutputRows)
      if (partitionSpec.isEmpty) {
        limitFunc(sortedIter)
      } else {
        new GroupedLimitIterator(sortedIter, childOutput, partitionSpec, limitFunc)
      }
    } else {
      peakMemory.set(math.max(peakMemory.value, map.getPeakMemoryUsedBytes + rowArena.bytesUsed))
      // Lazy: the map's own TaskCompletionListener (registered in its constructor) and
      // rowArena's (registered in UnsafeRowArena's constructor) guarantee eventual cleanup even
      // if this iterator is never fully drained (e.g. a downstream LIMIT), so there is no eager
      // map.free()/rowArena.free() call here the way the fallback branch above has for the
      // sorter -- both structures must stay alive for as long as the returned rows are read.
      kvIteratorToScala(map.iterator())
    }
  }

  /**
   * Adapts UnsafeFixedWidthAggregationMap's KVIterator (`next(): Boolean` advances, then
   * `getKey`/`getValue` read the current entry) into a Scala `Iterator`, skipping any key whose
   * buffer still holds the -1L sentinel (see `emptyBuffer`'s doc: never got a valid winner
   * recorded, e.g. due to a rowArena.append failure that was redirected to switchToFallback).
   */
  private def kvIteratorToScala(kvIter: KVIterator[UnsafeRow, UnsafeRow]): Iterator[InternalRow] = {
    val resultRow = new UnsafeRow(childOutput.length)
    new Iterator[InternalRow] {
      // True once kvIter is positioned on a valid (non-sentinel) entry that next() hasn't
      // consumed yet; false both before the first advance and right after next() consumes one.
      private var positioned = false
      private var exhausted = false

      private def advanceToValidEntry(): Unit = {
        while (!positioned && !exhausted) {
          if (kvIter.next()) {
            positioned = kvIter.getValue.getLong(numOrderCols) != -1L
          } else {
            exhausted = true
          }
        }
      }

      override def hasNext: Boolean = {
        if (!positioned && !exhausted) {
          advanceToValidEntry()
        }
        positioned
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException
        val address = kvIter.getValue.getLong(numOrderCols)
        positioned = false
        numOutputRows += 1
        rowArena.getRow(address, resultRow)
      }
    }
  }

  /**
   * Checks whether hash accumulation is still pruning enough duplicate rows to be worth it: if
   * rows-processed-per-distinct-key has fallen below `minCompactionRatio`, proactively falls
   * back rather than waiting for the hash map to genuinely run out of memory (which, for
   * high-cardinality keys, may happen far later than it should, if at all -- see
   * [[HashRowNumberGroupLimitAccumulator]]'s class doc).
   */
  private def maybeBailOutForLowCompaction(): Unit = {
    val numKeys = map.getNumKeys()
    if (numKeys > 0 && rowsConsumed.toDouble / numKeys < minCompactionRatio) {
      val ratio = rowsConsumed.toDouble / numKeys
      logInfo(s"Hash-based window group limit falling back to sort-based: compaction ratio " +
        s"$ratio after $rowsConsumed rows is below the minimum $minCompactionRatio.")
      switchToFallbackProactively()
    }
  }

  /**
   * Reached when the hash map or the row arena could no longer acquire memory. Dumps every row
   * already known to be a winner, plus `pendingRow` (the row that just failed to be
   * inserted/updated), into the fallback sorter. Every row consumed after this point (see
   * [[consumeRow]]) goes straight to that sorter instead of the hash map.
   */
  private def switchToFallback(pendingRow: UnsafeRow): Unit = {
    logInfo("Hash-based window group limit falling back to sort-based: could not acquire more " +
      "memory for the hash map or the row arena.")
    val sorter = buildFallbackSorter()
    sorter.insertRow(pendingRow)
    fallbackSorter = sorter
    numTasksFallBacked += 1
  }

  /**
   * Reached when [[maybeBailOutForLowCompaction]] decides hash accumulation isn't paying off.
   * Unlike [[switchToFallback]], there is no extra pending row to insert: the row that triggered
   * this check was already fully processed into `rowArena`/the map.
   */
  private def switchToFallbackProactively(): Unit = {
    fallbackSorter = buildFallbackSorter()
    numTasksFallBacked += 1
  }

  /**
   * Builds the fallback sorter -- the same (spill-capable) sorter
   * [[org.apache.spark.sql.execution.SortExec]] itself uses -- seeded with every row already
   * known to be a winner (skipping any key whose buffer still holds the -1L sentinel, see
   * `emptyBuffer`'s doc).
   *
   * Drains the map/arena into a temporary, bounded (by distinct-key count, not total row count)
   * JVM-heap buffer *before* constructing the new sorter, and frees the map and arena immediately
   * after -- deliberately in that order. Building the new sorter needs its own fresh execution
   * memory, and map+arena may together already be holding nearly all of the task's budget (that
   * is, after all, routinely the very reason this method is being called); constructing the new
   * sorter while they're still alive and holding that memory can itself fail to acquire memory,
   * which would turn an attempt to gracefully degrade into an uncaught `SparkOutOfMemoryError`
   * instead. The temporary heap copy this requires is a one-time cost at this (hopefully rare)
   * transition, not a per-row cost on the normal hot path -- unlike the per-row heap copy
   * `UnsafeRowArena` replaced.
   */
  private def buildFallbackSorter(): UnsafeExternalRowSorter = {
    val winners = new ArrayBuffer[UnsafeRow](map.getNumKeys())
    val scratchRow = new UnsafeRow(childOutput.length)
    val mapIter = map.iterator()
    while (mapIter.next()) {
      val address = mapIter.getValue.getLong(numOrderCols)
      if (address != -1L) {
        winners += rowArena.getRow(address, scratchRow).copy()
      }
    }
    peakMemory.set(math.max(peakMemory.value, map.getPeakMemoryUsedBytes + rowArena.bytesUsed))
    map.free()
    rowArena.free()

    val sortOrder = partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec
    val ordering = RowOrdering.create(sortOrder, childOutput)
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, childOutput)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)
    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(
          row: InternalRow): UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }
    val schema = DataTypeUtils.fromAttributes(childOutput)
    val sorter = UnsafeExternalRowSorter.create(
      schema,
      ordering,
      prefixComparator,
      prefixComputer,
      taskMemoryManager.pageSizeBytes,
      canUseRadixSort)

    winners.foreach(sorter.insertRow)
    sorter
  }
}

/**
 * Hash-based evaluator for [[WindowGroupLimitExec]] when
 * [[WindowGroupLimitExec.isHashEligible]] is true. Unlike [[WindowGroupLimitEvaluatorFactory]],
 * this does not require the input to be sorted -- see [[HashRowNumberGroupLimitAccumulator]] for
 * the algorithm.
 */
class HashRowNumberGroupLimitEvaluatorFactory(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric,
    numTasksFallBacked: SQLMetric,
    minCompactionRatio: Double,
    compactionCheckInterval: Long,
    enableRadixSort: Boolean)
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new HashRowNumberGroupLimitPartitionEvaluator

  /**
   * Builds a fresh, per-task [[HashRowNumberGroupLimitAccumulator]]. Used directly by
   * [[WindowGroupLimitExec]]'s whole-stage codegen, which drives the accumulator with generated
   * code instead of going through [[createEvaluator]]'s pull-based [[PartitionEvaluator]].
   */
  def createAccumulator(): HashRowNumberGroupLimitAccumulator =
    new HashRowNumberGroupLimitAccumulator(
      partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
      numTasksFallBacked, minCompactionRatio, compactionCheckInterval, enableRadixSort)

  class HashRowNumberGroupLimitPartitionEvaluator
    extends PartitionEvaluator[InternalRow, InternalRow] {

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val input = inputs.head
      val accumulator = createAccumulator()
      while (input.hasNext) {
        accumulator.consumeRow(input.next().asInstanceOf[UnsafeRow])
      }
      accumulator.finish()
    }
  }
}
