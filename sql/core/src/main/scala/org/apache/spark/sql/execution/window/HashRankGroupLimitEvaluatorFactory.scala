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

import org.apache.spark.{PartitionEvaluator, TaskContext}
import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.internal.Logging
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences,
  BoundReference, DenseRank, Expression, GenericInternalRow, RowOrdering, SortOrder, SortPrefix,
  UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SortPrefixUtils,
  UnsafeExternalRowSorter, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.unsafe.KVIterator

/**
 * One task's worth of state for the hash-based `RANK()`/`DENSE_RANK()` top-`limit` accumulation
 * described on [[WindowGroupLimitExec.isHashEligible]]. Kept as a separate implementation from
 * [[HashRowNumberGroupLimitAccumulator]] rather than generalizing that one -- `ROW_NUMBER() <=
 * limit` only ever keeps exactly `limit` *rows* (ties broken arbitrarily), while `RANK()`/
 * `DENSE_RANK() <= limit` must keep *every* row tied at any of the qualifying distinct order-key
 * values, which can vastly outnumber `limit`. Trino's own TopN implementation makes the same
 * split (a plain row-count builder for `row_number()`, a peer-group-aware one for `rank()`/
 * `dense_rank()`) rather than paying tie-tracking overhead on the simpler, more common path.
 *
 * Usage: call [[consumeRow]] once per input row (order doesn't matter), then call [[finish]]
 * exactly once to get the output rows.
 *
 * == Why `RANK()` and `DENSE_RANK()` need different admission math, not just different output
 * numbering ==
 *
 * It's tempting to think both functions just keep "the `limit` smallest distinct order-key
 * values" and differ only in how the final number is computed downstream (by the real
 * `WindowExec` this operator merely pre-filters for -- see [[WindowGroupLimitExec]]'s class doc).
 * That's true for `DENSE_RANK` but not for `RANK`: `RANK`'s value is `1 +`
 * (count of rows strictly better than it), so a value's eligibility depends on the *row counts*
 * of better tie-groups, not just how many distinct groups precede it. Example: group A has 3 tied
 * rows, group B has 1; with `limit = 2`, `DENSE_RANK() <= 2` keeps both A and B (dense ranks 1
 * and 2), but `RANK() <= 2` keeps only A (`RANK`s are 1 and 4). Treating them identically would
 * silently over-include rows for `RANK()`.
 *
 * == Data structure ==
 *
 * Per partition key, a [[UnsafeFixedWidthAggregationMap]] buffer holds up to `limit` slots, kept
 * sorted best-to-worst, each storing a distinct order-key value plus the number of rows seen for
 * it so far (`rowCount`) -- *not* the rows themselves. A trailing `slotCount` field tracks how
 * many slots are populated, and a trailing `headAddress` tracks the head of a single, per-key,
 * arena-threaded singly-linked list ([[UnsafeRowArena.appendLinked]]) holding every candidate row
 * ever admitted for that key, regardless of which slot its value maps to.
 *
 * For a new distinct value `v` arriving at sorted position `pos`, define its "preceding weight"
 * as the sum, over slots `0 until pos`, of each slot's weight -- `rowCount` for `RANK`, or a flat
 * `1` per slot for `DENSE_RANK` (which only cares about distinct-group position, not row counts).
 * `v` is admitted iff preceding weight `< limit`: this is exactly `RANK`/`DENSE_RANK() <= limit`'s
 * admission test for an as-yet-unseen value, and *rejection* is permanent -- preceding weight for
 * a fixed `v` can only grow as more rows/values arrive, so a value rejected once can never
 * subsequently qualify (letting [[consumeRow]] discard it immediately, at zero `rowArena` cost).
 *
 * *Admission*, in contrast, is not permanent for `RANK`: growing an earlier slot's row count --
 * via a later tie, or via a brand-new slot inserted ahead of it -- raises every later slot's
 * preceding weight, which can push an already-admitted slot past `limit` with no new distinct
 * value ever arriving to trigger re-evaluation. [[consumeRow]] handles this with
 * [[revalidateTrailingSlots]], called after either kind of weight increase: because weights are
 * positive and slots stay sorted, once one slot's preceding weight reaches `limit`, every slot
 * after it does too, so finding the first invalidated position and truncating `slotCount` there
 * is sufficient -- never more than one scan, and a no-op for `DENSE_RANK`, whose flat per-slot
 * weight of 1 cannot change.
 *
 * Every row matching an admitted value (whether newly admitted or an existing tie) is appended to
 * the per-key candidate list. A row's own slot can later be evicted by a better value arriving;
 * its already-appended candidate-list entries simply become dead `rowArena` space (never
 * individually reclaimed, like every other append in [[UnsafeRowArena]]) until [[finish]] filters
 * the whole list against the *final* slot contents.
 *
 * Falls back to sorting exactly like [[HashRowNumberGroupLimitAccumulator]] (same three triggers:
 * the map or the arena running out of memory, or the periodic compaction-ratio/append-ratio
 * efficiency check) -- draining through the real [[RankLimitIterator]]/[[DenseRankLimitIterator]]
 * (picked by `isDenseRank`) instead of [[SimpleLimitIterator]], since the fallback's sorted stream
 * needs the same `RANK`/`DENSE_RANK`-aware cutoff the hash path implements.
 */
class HashRankGroupLimitAccumulator(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric,
    numTasksFallBacked: SQLMetric,
    minCompactionRatio: Double = 1.05,
    minRowsBetweenCompactionChecks: Long = 100000L,
    enableRadixSort: Boolean = true,
    limit: Int = 1,
    maxAppendRatio: Double = 0.9,
    mode: WindowGroupLimitMode = Partial) extends HashGroupLimitAccumulator with Logging {

  private val isDenseRank = rankLikeFunction.isInstanceOf[DenseRank]
  private val numOrderCols = orderSpec.length

  // Field width of one slot: one field per order column, plus a trailing `rowCount` field.
  private val slotWidth = numOrderCols + 1

  private def slotOrderOrdinal(slot: Int, col: Int): Int = slot * slotWidth + col
  private def slotRowCountOrdinal(slot: Int): Int = slot * slotWidth + numOrderCols
  private val slotCountOrdinal: Int = limit * slotWidth
  private val headAddressOrdinal: Int = slotCountOrdinal + 1

  private val bufferSchema = StructType(
    (0 until limit).flatMap { slot =>
      orderSpec.zipWithIndex.map { case (order, i) =>
        StructField(s"o${slot}_$i", order.child.dataType, order.child.nullable)
      } :+ StructField(s"rowCount_$slot", LongType, nullable = false)
    }.toArray :+ StructField("slotCount", LongType, nullable = false) :+
      StructField("headAddress", LongType, nullable = false))

  private val groupingKeySchema = StructType(partitionSpec.zipWithIndex.map { case (e, i) =>
    StructField(s"k$i", e.dataType, e.nullable)
  }.toArray)

  // slotCount = 0 and headAddress = NO_NEXT mark "key inserted, but no candidate row admitted
  // yet" -- either genuinely just-inserted, or left behind by a row whose rowArena.appendLinked
  // failed (see consumeRow): in both cases finish() and buildFallbackSorter() have nothing to
  // drain for this key.
  private val emptyBuffer: InternalRow = {
    val values = new Array[Any](headAddressOrdinal + 1)
    values(slotCountOrdinal) = 0L
    values(headAddressOrdinal) = UnsafeRowArena.NO_NEXT
    UnsafeProjection.create(bufferSchema).apply(new GenericInternalRow(values))
  }

  private val groupingProjection = UnsafeProjection.create(partitionSpec, childOutput)
  private val orderKeyProjection = UnsafeProjection.create(orderSpec.map(_.child), childOutput)

  // Compares two order-key-shaped rows (numOrderCols fields at ordinals 0 until numOrderCols).
  private val boundOrdering =
    GenerateOrdering.generate(orderSpec.zipWithIndex.map { case (order, i) =>
      order.copy(child = BoundReference(i, order.child.dataType, order.child.nullable))
    })

  // Reusable scratch rows for extracting a slot's order-key fields (at a slot-dependent ordinal
  // offset inside the wide per-key buffer) into the same 0-based ordinal shape orderKeyProjection
  // produces, so boundOrdering can compare them.
  private def newOrderKeyScratchRow(): UnsafeRow = {
    val row = new UnsafeRow(numOrderCols)
    val widthBytes = UnsafeRow.calculateBitSetWidthInBytes(numOrderCols) + 8 * numOrderCols
    row.pointTo(new Array[Byte](widthBytes), widthBytes)
    row
  }
  private val scratchA = newOrderKeyScratchRow()

  private def extractSlotInto(buffer: UnsafeRow, slot: Int, dest: UnsafeRow): Unit = {
    var i = 0
    while (i < numOrderCols) {
      val ord = slotOrderOrdinal(slot, i)
      if (buffer.isNullAt(ord)) dest.setNullAt(i) else dest.setLong(i, buffer.getLong(ord))
      i += 1
    }
  }

  private def writeSlotFrom(buffer: UnsafeRow, slot: Int, orderKey: UnsafeRow): Unit = {
    var i = 0
    while (i < numOrderCols) {
      val ord = slotOrderOrdinal(slot, i)
      if (orderKey.isNullAt(i)) buffer.setNullAt(ord) else buffer.setLong(ord, orderKey.getLong(i))
      i += 1
    }
  }

  /** Copies slot `from`'s order-key fields and rowCount into slot `to`. */
  private def copySlot(buffer: UnsafeRow, from: Int, to: Int): Unit = {
    var i = 0
    while (i < numOrderCols) {
      val fromOrd = slotOrderOrdinal(from, i)
      val toOrd = slotOrderOrdinal(to, i)
      if (buffer.isNullAt(fromOrd)) buffer.setNullAt(toOrd) else {
        buffer.setLong(toOrd, buffer.getLong(fromOrd))
      }
      i += 1
    }
    buffer.setLong(slotRowCountOrdinal(to), buffer.getLong(slotRowCountOrdinal(from)))
  }

  /** `< 0` if `orderKey` is strictly better (should rank earlier) than slot `slot`'s value. */
  private def compareToSlot(orderKey: UnsafeRow, buffer: UnsafeRow, slot: Int): Int = {
    extractSlotInto(buffer, slot, scratchA)
    boundOrdering.compare(orderKey, scratchA)
  }

  /** The slot index whose value equals `orderKey`, or -1 if no slot in `0 until slotCount` does. */
  private def findMatchingSlot(buffer: UnsafeRow, orderKey: UnsafeRow, slotCount: Int): Int = {
    var i = 0
    while (i < slotCount) {
      if (compareToSlot(orderKey, buffer, i) == 0) return i
      i += 1
    }
    -1
  }

  /** First slot index whose value is worse than `orderKey` -- i.e. where `orderKey` sorts in. */
  private def findInsertionPos(buffer: UnsafeRow, orderKey: UnsafeRow, slotCount: Int): Int = {
    var i = 0
    while (i < slotCount && compareToSlot(orderKey, buffer, i) > 0) i += 1
    i
  }

  /** `RANK`: each slot's own row count. `DENSE_RANK`: a flat 1 -- only position matters. */
  private def slotWeight(buffer: UnsafeRow, slot: Int): Long =
    if (isDenseRank) 1L else buffer.getLong(slotRowCountOrdinal(slot))

  private def sumWeights(buffer: UnsafeRow, until: Int): Long = {
    var sum = 0L
    var i = 0
    while (i < until) {
      sum += slotWeight(buffer, i)
      i += 1
    }
    sum
  }

  /**
   * After slot `fromSlot`'s weight increases -- either a tie bumped its row count, or a brand-new
   * slot was just inserted there, shifting every later slot's preceding weight up by the new
   * slot's own weight -- a slot already admitted *after* `fromSlot` can become invalid: `RANK`'s
   * weight is a slot's row count, so either event can raise a later slot's preceding weight past
   * `limit`, something the one-time admission test in [[consumeRow]] (run only when a value is
   * first seen) never re-checks. By the same weight-monotonicity argument the class doc already
   * relies on, once one slot's preceding weight reaches `limit`, every slot after it does too, so
   * finding the first invalidated position and truncating `slotCount` there is sufficient --
   * `kvIteratorToScala` already filters candidates purely by the live `slotCount` at drain time,
   * so no arena bookkeeping is needed: an evicted slot's already-appended rows simply stop
   * matching anything once `finish()` runs. A no-op for `DENSE_RANK`, whose flat per-slot weight
   * of 1 never changes, and for a change at the already-last slot, which has nothing after it to
   * invalidate.
   */
  private def revalidateTrailingSlots(buffer: UnsafeRow, fromSlot: Int, slotCount: Int): Unit = {
    if (isDenseRank || fromSlot == slotCount - 1) return
    var i = fromSlot + 1
    while (i < slotCount && sumWeights(buffer, i) < limit) i += 1
    if (i < slotCount) {
      buffer.setLong(slotCountOrdinal, i)
    }
  }

  /**
   * Inserts a brand-new distinct value at sorted position `pos`, shifting slots `[pos, lastIndex)`
   * right by one. If the slot array is already full (`slotCount == limit`), the current last slot
   * is dropped entirely rather than shifted, enforcing the `limit`-slot capacity bound; the
   * caller is responsible for additionally calling [[revalidateTrailingSlots]] afterward, since
   * shifting slots can also invalidate one that remains within the (possibly still-growing)
   * array, not only the one truncated by capacity -- see [[revalidateTrailingSlots]]'s doc.
   */
  private def insertSlotAt(
      buffer: UnsafeRow, pos: Int, orderKey: UnsafeRow, slotCount: Int): Unit = {
    val lastIndex = math.min(slotCount, limit - 1)
    var i = lastIndex
    while (i > pos) {
      copySlot(buffer, i - 1, i)
      i -= 1
    }
    writeSlotFrom(buffer, pos, orderKey)
    buffer.setLong(slotRowCountOrdinal(pos), 1L)
  }

  private val taskMemoryManager = TaskContext.get().taskMemoryManager()

  private val map = new UnsafeFixedWidthAggregationMap(
    emptyBuffer,
    bufferSchema,
    groupingKeySchema,
    TaskContext.get(),
    1024 * 16,
    taskMemoryManager.pageSizeBytes,
    // See the identical choice (and its rationale) in HashRowNumberGroupLimitAccumulator.
    HashRowNumberGroupLimitAccumulator.INITIAL_PAGE_SIZE_BYTES)

  // linked = true: every appended row carries a trailing "next" pointer, threading all of a key's
  // candidate rows into one singly-linked list headed by that key's headAddress buffer field.
  private var rowArena: UnsafeRowArena = new UnsafeRowArena(taskMemoryManager, linked = true)

  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  private var rowsConsumed: Long = 0L
  // See the identical field (and its doc) in HashRowNumberGroupLimitAccumulator.
  private var currentCheckInterval: Long =
    if (minRowsBetweenCompactionChecks >
        HashRowNumberGroupLimitAccumulator.COMPACTION_CHECK_RAMP_UP_THRESHOLD) {
      math.max(1L, minRowsBetweenCompactionChecks / 4)
    } else {
      minRowsBetweenCompactionChecks
    }
  private var nextCompactionCheckAt: Long = currentCheckInterval
  private var appendsSinceLastCheck: Long = 0L

  // See the identical field (and its doc) in HashRowNumberGroupLimitAccumulator: a
  // SorterFallbackDestination is used for mode == Partial (must still filter before the shuffle),
  // an ArrayFallbackDestination for mode == Final (correctness is guaranteed by the mandatory
  // downstream Sort + WindowExec + Filter, so there is no benefit to filtering, or therefore
  // sorting, again here).
  private var fallbackDestination: FallbackDestination = null

  override def fallbackDestinationOrNull: FallbackDestination = fallbackDestination

  /** Consumes one input row. Safe to call in any order; the hash path doesn't need sorted input. */
  def consumeRow(row: UnsafeRow): Unit = {
    if (fallbackDestination != null) {
      fallbackDestination.insertRow(row)
      return
    }
    val key = groupingProjection(row)
    val buffer = map.getAggregationBufferFromUnsafeRow(key)
    if (buffer == null) {
      switchToFallback(row)
      return
    }
    val slotCount = buffer.getLong(slotCountOrdinal).toInt
    val orderKey = orderKeyProjection(row)
    val matchSlot = findMatchingSlot(buffer, orderKey, slotCount)
    if (matchSlot >= 0) {
      appendCandidate(row, buffer)
      if (fallbackDestination != null) return
      buffer.setLong(
        slotRowCountOrdinal(matchSlot), buffer.getLong(slotRowCountOrdinal(matchSlot)) + 1)
      appendsSinceLastCheck += 1
      revalidateTrailingSlots(buffer, matchSlot, slotCount)
    } else {
      val pos = findInsertionPos(buffer, orderKey, slotCount)
      if (sumWeights(buffer, pos) < limit) {
        appendCandidate(row, buffer)
        if (fallbackDestination != null) return
        insertSlotAt(buffer, pos, orderKey, slotCount)
        val newSlotCount = math.min(slotCount + 1, limit)
        buffer.setLong(slotCountOrdinal, newSlotCount)
        appendsSinceLastCheck += 1
        // The new slot at `pos` only shares its own weight (1) with every slot shifted after it
        // (see insertSlotAt), which can push one of *those* slots' preceding weight to `limit`
        // even though the array wasn't at capacity -- a slot ejected purely by running out of the
        // fixed `limit` capacity (handled by the newSlotCount cap above) is a different, already-
        // handled case from a slot that remains in-bounds but is no longer valid under the new
        // weight distribution. Both are covered by the same scan.
        revalidateTrailingSlots(buffer, pos, newSlotCount)
      }
      // else: orderKey's true rank already exceeds limit and can only get worse from here (see
      // class doc) -- discarded without ever touching the arena.
    }
    rowsConsumed += 1
    if (minRowsBetweenCompactionChecks > 0 && rowsConsumed >= nextCompactionCheckAt) {
      maybeBailOutForPoorEfficiency(currentCheckInterval)
      currentCheckInterval = math.min(currentCheckInterval * 2, minRowsBetweenCompactionChecks)
      nextCompactionCheckAt = rowsConsumed + currentCheckInterval
      appendsSinceLastCheck = 0L
    }
  }

  /**
   * Prepends `row` to `buffer`'s key's candidate list in `rowArena`. Appends *before* touching
   * `buffer`: if `rowArena.appendLinked` fails, `buffer` must be left exactly as it was, since
   * [[switchToFallback]] -- reached right after this throws -- drains the map's current,
   * still-consistent state as part of its fallback seed set. Falls back on either of
   * `rowArena.appendLinked`'s two failure modes, same as [[HashRowNumberGroupLimitAccumulator]].
   */
  private def appendCandidate(row: UnsafeRow, buffer: UnsafeRow): Unit = {
    val prevHead = buffer.getLong(headAddressOrdinal)
    val address =
      try {
        rowArena.appendLinked(row, prevHead)
      } catch {
        case _: SparkOutOfMemoryError | _: SparkUnsupportedOperationException =>
          switchToFallback(row)
          return
      }
    buffer.setLong(headAddressOrdinal, address)
  }

  /**
   * Call once, after every row has been passed to [[consumeRow]], to get the output rows. Mirrors
   * [[HashRowNumberGroupLimitAccumulator.finish]] in pattern-matching on the concrete
   * [[FallbackDestination]] rather than going through [[FallbackDestination.insertRow]]: see that
   * method's doc for why.
   */
  def finish(): Iterator[InternalRow] = fallbackDestination match {
    case ArrayFallbackDestination(array) =>
      peakMemory.set(math.max(peakMemory.value, array.spillSize))
      spillSize.set(TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      val rawIter = array.generateIterator()
      new Iterator[InternalRow] {
        override def hasNext: Boolean = rawIter.hasNext
        override def next(): InternalRow = {
          numOutputRows += 1
          rawIter.next()
        }
      }
    case SorterFallbackDestination(sorter) =>
      peakMemory.set(math.max(peakMemory.value, sorter.getPeakMemoryUsage))
      spillSize.set(TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      val sortedIter = sorter.sort()
      val limitFunc = (iter: Iterator[InternalRow]) =>
        if (isDenseRank) {
          DenseRankLimitIterator(childOutput, iter, orderSpec, limit, numOutputRows)
        } else {
          RankLimitIterator(childOutput, iter, orderSpec, limit, numOutputRows)
        }
      if (partitionSpec.isEmpty) {
        limitFunc(sortedIter)
      } else {
        new GroupedLimitIterator(sortedIter, childOutput, partitionSpec, limitFunc)
      }
    case null =>
      peakMemory.set(math.max(peakMemory.value, map.getPeakMemoryUsedBytes + rowArena.bytesUsed))
      kvIteratorToScala(map.iterator())
  }

  /**
   * Adapts UnsafeFixedWidthAggregationMap's KVIterator into a Scala `Iterator`, walking each
   * key's candidate list and emitting only rows whose order-key value still matches one of that
   * key's *final* slots (see the class doc: a candidate's slot may have since been evicted by a
   * better value, in which case it's silently skipped here rather than physically removed from
   * the arena).
   */
  private def kvIteratorToScala(kvIter: KVIterator[UnsafeRow, UnsafeRow]): Iterator[InternalRow] = {
    val resultRow = new UnsafeRow(childOutput.length)
    new Iterator[InternalRow] {
      private var currentBuffer: UnsafeRow = null
      private var currentSlotCount: Int = 0
      private var nextAddress: Long = UnsafeRowArena.NO_NEXT
      private var pending: InternalRow = null
      private var exhausted = false

      private def advance(): Unit = {
        while (pending == null && !exhausted) {
          while (nextAddress == UnsafeRowArena.NO_NEXT && !exhausted) {
            if (kvIter.next()) {
              currentBuffer = kvIter.getValue
              currentSlotCount = currentBuffer.getLong(slotCountOrdinal).toInt
              nextAddress = currentBuffer.getLong(headAddressOrdinal)
            } else {
              exhausted = true
            }
          }
          if (!exhausted) {
            val address = nextAddress
            val candidateRow = rowArena.getRow(address, resultRow)
            val orderKey = orderKeyProjection(candidateRow)
            if (findMatchingSlot(currentBuffer, orderKey, currentSlotCount) >= 0) {
              pending = candidateRow
            }
            nextAddress = rowArena.getNext(address)
          }
        }
      }

      override def hasNext: Boolean = {
        advance()
        pending != null
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException
        val result = pending
        pending = null
        numOutputRows += 1
        result
      }
    }
  }

  /** Mirrors [[HashRowNumberGroupLimitAccumulator.maybeBailOutForPoorEfficiency]]. */
  private def maybeBailOutForPoorEfficiency(completedWindowSize: Long): Unit = {
    val numKeys = map.getNumKeys()
    if (numKeys > 0 && rowsConsumed.toDouble / numKeys < minCompactionRatio) {
      val ratio = rowsConsumed.toDouble / numKeys
      logInfo(s"Hash-based window group limit falling back to sort-based: compaction ratio " +
        s"$ratio after $rowsConsumed rows is below the minimum $minCompactionRatio.")
      switchToFallbackProactively()
      return
    }
    val appendRatio = appendsSinceLastCheck.toDouble / completedWindowSize
    if (appendRatio > maxAppendRatio) {
      logInfo(s"Hash-based window group limit falling back to sort-based: $appendRatio of the " +
        s"last $completedWindowSize rows triggered a candidate append, above the " +
        s"maximum $maxAppendRatio.")
      switchToFallbackProactively()
    }
  }

  /** Mirrors [[HashRowNumberGroupLimitAccumulator.switchToFallback]]. */
  private def switchToFallback(pendingRow: UnsafeRow): Unit = {
    logInfo("Hash-based window group limit falling back to sort-based: could not acquire more " +
      "memory for the hash map or the row arena.")
    fallbackDestination = if (mode == Final) {
      val array = buildFallbackArray()
      array.add(pendingRow)
      ArrayFallbackDestination(array)
    } else {
      val sorter = buildFallbackSorter()
      sorter.insertRow(pendingRow)
      SorterFallbackDestination(sorter)
    }
    numTasksFallBacked += 1
  }

  /** Mirrors [[HashRowNumberGroupLimitAccumulator.switchToFallbackProactively]]. */
  private def switchToFallbackProactively(): Unit = {
    fallbackDestination = if (mode == Final) {
      ArrayFallbackDestination(buildFallbackArray())
    } else {
      SorterFallbackDestination(buildFallbackSorter())
    }
    numTasksFallBacked += 1
  }

  /**
   * Drains *every* candidate row ever admitted (not filtered by current slot membership, unlike
   * [[kvIteratorToScala]]): a row discarded by [[consumeRow]] (never appended at all) is
   * permanently, correctly excluded regardless of future data (see the class doc's "preceding
   * weight" argument), but a row that *was* appended and later had its slot evicted is not
   * necessarily excluded from the *true*, whole-stream answer -- re-sorting and re-running it
   * through [[RankLimitIterator]]/[[DenseRankLimitIterator]] alongside every row consumed after
   * the fallback switch recomputes the correct cutoff from scratch, so including a few extra
   * (ultimately filtered-out) rows here is harmless, merely redundant. Shared by
   * [[buildFallbackSorter]] and [[buildFallbackArray]] -- see
   * [[HashRowNumberGroupLimitAccumulator.drainWinnersAndFreeStructures]] for why the drain-then-
   * free ordering matters.
   */
  private def drainCandidatesAndFreeStructures(): ArrayBuffer[UnsafeRow] = {
    val candidates = new ArrayBuffer[UnsafeRow](map.getNumKeys() * limit)
    val scratchRow = new UnsafeRow(childOutput.length)
    val mapIter = map.iterator()
    while (mapIter.next()) {
      var address = mapIter.getValue.getLong(headAddressOrdinal)
      while (address != UnsafeRowArena.NO_NEXT) {
        candidates += rowArena.getRow(address, scratchRow).copy()
        address = rowArena.getNext(address)
      }
    }
    peakMemory.set(math.max(peakMemory.value, map.getPeakMemoryUsedBytes + rowArena.bytesUsed))
    map.free()
    rowArena.free()
    candidates
  }

  /**
   * Builds the `Final`-mode fallback destination: an unsorted, append-only, spill-capable row
   * array. See [[HashRowNumberGroupLimitAccumulator.buildFallbackArray]] for why `Final` never
   * needs a sorter.
   */
  private def buildFallbackArray(): ExternalAppendOnlyUnsafeRowArray = {
    val candidates = drainCandidatesAndFreeStructures()
    val array = new ExternalAppendOnlyUnsafeRowArray(0, 0L, Int.MaxValue, Long.MaxValue)
    candidates.foreach(array.add)
    array
  }

  /** Builds the `Partial`-mode fallback sorter, seeded with every drained candidate. */
  private def buildFallbackSorter(): UnsafeExternalRowSorter = {
    val candidates = drainCandidatesAndFreeStructures()

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

    candidates.foreach(sorter.insertRow)
    sorter
  }
}

/**
 * Hash-based evaluator for [[WindowGroupLimitExec]] when [[WindowGroupLimitExec.isHashEligible]]
 * is true and `rankLikeFunction` is `Rank` or `DenseRank`. See
 * [[HashRowNumberGroupLimitAccumulator]]'s sibling for `RowNumber`.
 */
class HashRankGroupLimitEvaluatorFactory(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric,
    numTasksFallBacked: SQLMetric,
    minCompactionRatio: Double,
    minRowsBetweenCompactionChecks: Long,
    enableRadixSort: Boolean,
    limit: Int,
    maxAppendRatio: Double = 0.9,
    mode: WindowGroupLimitMode = Partial)
  extends HashGroupLimitEvaluatorFactory {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new HashRankGroupLimitPartitionEvaluator

  /**
   * Builds a fresh, per-task [[HashRankGroupLimitAccumulator]]. Used directly by
   * [[WindowGroupLimitExec]]'s whole-stage codegen, which drives the accumulator with generated
   * code instead of going through [[createEvaluator]]'s pull-based [[PartitionEvaluator]].
   */
  override def createAccumulator(): HashRankGroupLimitAccumulator =
    new HashRankGroupLimitAccumulator(
      partitionSpec, orderSpec, rankLikeFunction, childOutput, numOutputRows, peakMemory,
      spillSize, numTasksFallBacked, minCompactionRatio, minRowsBetweenCompactionChecks,
      enableRadixSort, limit, maxAppendRatio, mode)

  class HashRankGroupLimitPartitionEvaluator
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
