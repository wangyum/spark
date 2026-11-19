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
  BoundReference, Expression, GenericInternalRow, RowOrdering, SortOrder, SortPrefix,
  UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SortPrefixUtils,
  UnsafeExternalRowSorter, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.unsafe.KVIterator

/**
 * One task's worth of state for the hash-based top-`limit` `ROW_NUMBER()` accumulation described
 * on [[WindowGroupLimitExec.isHashEligible]]. Used by both the interpreted path
 * ([[HashRowNumberGroupLimitEvaluatorFactory]]) and [[WindowGroupLimitExec]]'s whole-stage codegen
 * (`doConsume`/`doProduce`), so the two paths share one implementation instead of two that could
 * drift apart.
 *
 * Usage: call [[consumeRow]] once per input row (order doesn't matter, unlike the sort-based
 * iterators), then call [[finish]] exactly once to get the output rows.
 *
 * For each partition key it keeps only the `limit` best-so-far rows (by `orderSpec`) in a bounded,
 * in-place binary max-heap stored directly in a [[UnsafeFixedWidthAggregationMap]] buffer -- "max"
 * meaning the heap root always holds the *worst* of the currently-kept rows, so a new row only
 * needs one comparison (against the root) to know whether it can possibly displace anything. The
 * aggregation buffer holds `limit` slots, each storing a copy of `orderSpec`'s values (one field
 * per order column, every one of which [[WindowGroupLimitExec.isHashEligible]] already guarantees
 * is `UnsafeRow`-mutable) plus that slot's `address` field: a pointer into `rowArena`
 * ([[UnsafeRowArena]]), which holds the actual row for each kept slot in Tungsten-managed pages
 * (full rows are generally variable-width, so they cannot be the map's own fixed-width value).
 * A trailing `heapSize` field tracks how many of the `limit` slots are currently populated (fewer
 * than `limit` until a key has seen at least `limit` rows). The map is only ever used to decide
 * "is this row better than the current worst-kept row for its key, and if so where should it go"
 * -- it never stores the row bytes itself.
 *
 * Falls back from hash accumulation to sorting -- the same [[UnsafeExternalRowSorter]]
 * [[org.apache.spark.sql.execution.SortExec]] itself uses, drained through the existing,
 * already-correct [[GroupedLimitIterator]]/[[SimpleLimitIterator]] -- for three reasons:
 *   - the hash map could no longer acquire memory for a key's fixed-width entry
 *     ([[switchToFallback]]); or
 *   - `rowArena` could no longer acquire memory for a row's bytes (also [[switchToFallback]],
 *     same trigger, see [[consumeRow]]); or
 *   - periodically ([[maybeBailOutForPoorEfficiency]], [[switchToFallbackProactively]]), either
 *     the ratio of rows processed to distinct keys seen falls below `minCompactionRatio` (hash
 *     accumulation isn't pruning enough duplicate rows to be worth its per-row overhead -- mirrors
 *     `HashAggregateExec`'s adaptive partial aggregation, which uses the same compaction-ratio
 *     heuristic, `spark.sql.execution.aggregate.adaptivePartialAggregation.*`, for the same
 *     reason, against a different fallback mechanism here), or the fraction of rows that actually
 *     triggered a heap write exceeds `maxAppendRatio` (a healthy distinct-key ratio alone misses
 *     a key whose kept rows keep getting replaced/extended by almost every row, e.g. taking the
 *     latest `limit` rows per key from a roughly time-ordered stream).
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
    minRowsBetweenCompactionChecks: Long = 100000L,
    enableRadixSort: Boolean = true,
    limit: Int,
    maxAppendRatio: Double = 0.9,
    mode: WindowGroupLimitMode = Partial) extends HashGroupLimitAccumulator with Logging {

  private val numOrderCols = orderSpec.length

  // Field width of one heap slot: one field per order column, plus a trailing `address` field.
  private val slotWidth = numOrderCols + 1

  private def slotOrderOrdinal(slot: Int, col: Int): Int = slot * slotWidth + col
  private def slotAddrOrdinal(slot: Int): Int = slot * slotWidth + numOrderCols
  private val heapSizeOrdinal: Int = limit * slotWidth

  // `limit` slots (each: numOrderCols order fields + an address field), plus a trailing heapSize.
  private val bufferSchema = StructType(
    (0 until limit).flatMap { slot =>
      orderSpec.zipWithIndex.map { case (order, i) =>
        StructField(s"o${slot}_$i", order.child.dataType, order.child.nullable)
      } :+ StructField(s"addr_$slot", LongType, nullable = false)
    }.toArray :+ StructField("heapSize", LongType, nullable = false))

  private val groupingKeySchema = StructType(partitionSpec.zipWithIndex.map { case (e, i) =>
    StructField(s"k$i", e.dataType, e.nullable)
  }.toArray)

  // heapSize = 0 marks "key inserted, but no row successfully recorded yet" -- either genuinely
  // just-inserted, or left behind by a row whose rowArena.append failed (see consumeRow): in both
  // cases there is no valid row at this key to emit or drain, so finish() and buildFallbackSorter()
  // must skip it. Slot contents are otherwise never read while outside [0, heapSize).
  private val emptyBuffer: InternalRow = {
    val values = new Array[Any](heapSizeOrdinal + 1)
    values(heapSizeOrdinal) = 0L
    UnsafeProjection.create(bufferSchema).apply(new GenericInternalRow(values))
  }

  private val groupingProjection = UnsafeProjection.create(partitionSpec, childOutput)
  private val orderKeyProjection = UnsafeProjection.create(orderSpec.map(_.child), childOutput)

  // Compares two order-key-shaped rows (numOrderCols fields at ordinals 0 until numOrderCols).
  // Used both to compare orderKeyProjection's output against a slot's extracted key, and to
  // compare two slots' extracted keys against each other during heap sift operations.
  private val boundOrdering =
    GenerateOrdering.generate(orderSpec.zipWithIndex.map { case (order, i) =>
      order.copy(child = BoundReference(i, order.child.dataType, order.child.nullable))
    })

  // Reusable scratch rows for extracting a heap slot's order-key fields (which live at a
  // slot-dependent ordinal offset inside the wide per-key buffer) into the same 0-based ordinal
  // shape orderKeyProjection produces, so boundOrdering can compare them. Two are needed because
  // sift operations compare two different slots against each other.
  private def newOrderKeyScratchRow(): UnsafeRow = {
    val row = new UnsafeRow(numOrderCols)
    val widthBytes = UnsafeRow.calculateBitSetWidthInBytes(numOrderCols) + 8 * numOrderCols
    row.pointTo(new Array[Byte](widthBytes), widthBytes)
    row
  }
  private val scratchA = newOrderKeyScratchRow()
  private val scratchB = newOrderKeyScratchRow()

  /** Copies slot `slot`'s order-key fields out of `buffer` into `dest` (0-based ordinals). */
  private def extractSlotInto(buffer: UnsafeRow, slot: Int, dest: UnsafeRow): Unit = {
    var i = 0
    while (i < numOrderCols) {
      val ord = slotOrderOrdinal(slot, i)
      if (buffer.isNullAt(ord)) dest.setNullAt(i) else dest.setLong(i, buffer.getLong(ord))
      i += 1
    }
  }

  /** Writes `src`'s (0-based) order-key fields into slot `slot` of `buffer`. */
  private def writeSlotFrom(buffer: UnsafeRow, slot: Int, src: UnsafeRow): Unit = {
    var i = 0
    while (i < numOrderCols) {
      val ord = slotOrderOrdinal(slot, i)
      if (src.isNullAt(i)) buffer.setNullAt(ord) else buffer.setLong(ord, src.getLong(i))
      i += 1
    }
  }

  private def compareSlots(buffer: UnsafeRow, a: Int, b: Int): Int = {
    extractSlotInto(buffer, a, scratchA)
    extractSlotInto(buffer, b, scratchB)
    boundOrdering.compare(scratchA, scratchB)
  }

  /** `< 0` if `orderKey` is strictly better (should win) than slot `slot`'s current key. */
  private def compareToSlot(orderKey: UnsafeRow, buffer: UnsafeRow, slot: Int): Int = {
    extractSlotInto(buffer, slot, scratchA)
    boundOrdering.compare(orderKey, scratchA)
  }

  private def swapSlots(buffer: UnsafeRow, a: Int, b: Int): Unit = {
    extractSlotInto(buffer, a, scratchA)
    extractSlotInto(buffer, b, scratchB)
    val addrA = buffer.getLong(slotAddrOrdinal(a))
    val addrB = buffer.getLong(slotAddrOrdinal(b))
    writeSlotFrom(buffer, a, scratchB)
    writeSlotFrom(buffer, b, scratchA)
    buffer.setLong(slotAddrOrdinal(a), addrB)
    buffer.setLong(slotAddrOrdinal(b), addrA)
  }

  /** Standard max-heap sift-up: bubbles slot `start` toward the root while it beats its parent. */
  private def siftUp(buffer: UnsafeRow, start: Int): Unit = {
    var idx = start
    var continue = true
    while (continue && idx > 0) {
      val parent = (idx - 1) / 2
      if (compareSlots(buffer, parent, idx) < 0) {
        // The parent's key is strictly better (smaller) than the child's -- the child is "worse"
        // and must rise toward the root in this worst-on-top max-heap.
        swapSlots(buffer, parent, idx)
        idx = parent
      } else {
        continue = false
      }
    }
  }

  /** Standard max-heap sift-down: sinks slot `start` while a child is worse than it. */
  private def siftDown(buffer: UnsafeRow, start: Int, heapSize: Int): Unit = {
    var idx = start
    var continue = true
    while (continue) {
      val left = 2 * idx + 1
      val right = 2 * idx + 2
      var worst = idx
      if (left < heapSize && compareSlots(buffer, left, worst) > 0) worst = left
      if (right < heapSize && compareSlots(buffer, right, worst) > 0) worst = right
      if (worst == idx) {
        continue = false
      } else {
        swapSlots(buffer, idx, worst)
        idx = worst
      }
    }
  }

  private val taskMemoryManager = TaskContext.get().taskMemoryManager()

  private val map = new UnsafeFixedWidthAggregationMap(
    emptyBuffer,
    bufferSchema,
    groupingKeySchema,
    TaskContext.get(),
    1024 * 16,
    taskMemoryManager.pageSizeBytes,
    // A key's full fixed-width buffer is only numOrderCols+1 longs wide; most tasks never come
    // close to filling even one page. Starting small and growing (capped at pageSizeBytes) avoids
    // committing a full page (commonly tens of MB) up front for what may end up being a handful of
    // keys -- mirrors HashedRelation's identical initialPageSizeBytes split for its build-side map.
    HashRowNumberGroupLimitAccumulator.INITIAL_PAGE_SIZE_BYTES)

  // Holds the actual candidate rows in Tungsten pages (see UnsafeRowArena's class doc for why this
  // replaced a plain JVM-heap `ArrayBuffer[UnsafeRow]`: avoids unaccounted heap allocation and GC
  // pressure on every winner/replacement, and makes this memory visible to the same backpressure
  // signal (SparkOutOfMemoryError) the map itself already reacts to).
  private var rowArena: UnsafeRowArena = new UnsafeRowArena(taskMemoryManager, linked = false)

  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  // Rows consumed by the hash path (not counting rows already routed to fallbackSorter), used
  // only to decide when to run the next periodic efficiency check.
  private var rowsConsumed: Long = 0L

  // The interval before the next periodic check. Below COMPACTION_CHECK_RAMP_UP_THRESHOLD this
  // stays pinned at minRowsBetweenCompactionChecks forever (unchanged original behavior). At or
  // above it, it starts at a quarter of minRowsBetweenCompactionChecks and doubles (capped at
  // minRowsBetweenCompactionChecks) every time a check doesn't bail out: a task whose true row
  // count never reaches minRowsBetweenCompactionChecks (e.g. an all-unique-key task under the
  // large, 100000-row default) would otherwise never run the check at all, paying the full cost of
  // growing the hash map/table for nothing -- see HashRowNumberGroupLimitAccumulator's class doc's
  // "periodically" bullet. A genuinely healthy task (e.g. few distinct keys) passes these smaller
  // early checks just as easily, since their ratio is already high well before minRowsBetween
  // CompactionChecks, and settles into the original, larger steady-state interval after a few
  // doublings.
  private var currentCheckInterval: Long =
    if (minRowsBetweenCompactionChecks >
        HashRowNumberGroupLimitAccumulator.COMPACTION_CHECK_RAMP_UP_THRESHOLD) {
      math.max(1L, minRowsBetweenCompactionChecks / 4)
    } else {
      minRowsBetweenCompactionChecks
    }
  private var nextCompactionCheckAt: Long = currentCheckInterval

  // Rows, within the just-completed check interval (see currentCheckInterval), that actually
  // triggered a write into a heap slot (insert or replace) rather than being discarded outright.
  // Reset to 0 every time the periodic check runs (see consumeRow), so this is always a count over
  // exactly the interval just completed, not a cumulative total -- a windowed count reacts to a
  // mid-stream shift from healthy to unhealthy compaction, where a cumulative ratio would stay
  // diluted by an earlier healthy stretch. See maybeBailOutForPoorEfficiency.
  private var appendsSinceLastCheck: Long = 0L

  // Non-null once the map or the row arena could no longer acquire memory, or the compaction
  // ratio proved too low; from then on every row -- including ones already recorded as kept -- is
  // routed here instead, and the hash map/row arena are abandoned. Used when `mode == Partial`
  // (see `fallbackArray` for `Final`): Partial's fallback output still has to be filtered down to
  // `limit` rows per key, since it is the only thing standing between its task's rows and the
  // shuffle -- skipping that filtering would ship every row across the network, losing the whole
  // point of the pre-filter. A sort-based filter needs sorted input, hence a real sorter here.
  private var fallbackSorter: UnsafeExternalRowSorter = null

  // Non-null once `mode == Final` has fallen back (same two triggers as `fallbackSorter`); from
  // then on every row is appended here, unfiltered and unsorted, instead. Safe specifically
  // because a hash-eligible WindowGroupLimitExec always declares outputOrdering = Nil (see
  // isHashEligible), so EnsureRequirements unconditionally plants a real Sort directly above a
  // Final node feeding the real WindowExec + its Filter(rn <= limit) above that -- which
  // recomputes the correct answer from whatever rows arrive, regardless of how many that is.
  // Skipping Final's own sort-then-filter here avoids sorting the same rows twice: once internally
  // (for no benefit, since nothing downstream relies on Final's output being pre-filtered) and
  // again in that unconditional Sort.
  private var fallbackArray: ExternalAppendOnlyUnsafeRowArray = null

  override def fallbackSorterOrNull: UnsafeExternalRowSorter = fallbackSorter
  override def fallbackArrayOrNull: ExternalAppendOnlyUnsafeRowArray = fallbackArray

  /** Consumes one input row. Safe to call in any order; the hash path doesn't need sorted input. */
  def consumeRow(row: UnsafeRow): Unit = {
    if (fallbackArray != null) {
      fallbackArray.add(row)
      return
    }
    if (fallbackSorter != null) {
      fallbackSorter.insertRow(row)
      return
    }
    val key = groupingProjection(row)
    val buffer = map.getAggregationBufferFromUnsafeRow(key)
    if (buffer == null) {
      switchToFallback(row)
      return
    }
    val heapSize = buffer.getLong(heapSizeOrdinal).toInt
    val orderKey = orderKeyProjection(row)
    if (heapSize < limit) {
      storeAtSlot(row, orderKey, buffer, heapSize)
      if (fallbackSorter != null || fallbackArray != null) return
      appendsSinceLastCheck += 1
      buffer.setLong(heapSizeOrdinal, heapSize + 1)
      siftUp(buffer, heapSize)
    } else if (compareToSlot(orderKey, buffer, 0) < 0) {
      // Heap full and this row beats the current worst-kept (the root): replace it and restore
      // the heap property. A row that doesn't beat the root falls through to the bookkeeping
      // below without being stored anywhere -- it can never become eligible later, since the
      // root only ever gets more selective (smaller) as more rows arrive -- but, exactly like the
      // original K=1 code, still counts toward rowsConsumed: a discard is the expected, desired
      // outcome for a well-compacting key, not an exceptional case to exclude from the ratio.
      storeAtSlot(row, orderKey, buffer, 0)
      if (fallbackSorter != null || fallbackArray != null) return
      appendsSinceLastCheck += 1
      siftDown(buffer, 0, limit)
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
   * Appends `row` to `rowArena` and writes its order-key fields and address into slot `slot` of
   * `buffer`. Appends *before* touching `buffer`: if `rowArena.append` fails, `buffer` must be
   * left exactly as it was (either the untouched state for a not-yet-populated slot, or the
   * previous occupant's valid state for a replacement), since [[switchToFallback]] -- reached
   * right after this throws -- drains the map's current, still-consistent state as part of its
   * fallback seed set.
   *
   * Falls back on either of `rowArena.append`'s two failure modes: genuine OOM
   * (`SparkOutOfMemoryError`), or `row` itself being too large for the arena's address packing
   * (`SparkUnsupportedOperationException`, see [[UnsafeRowArena.append]]) -- the sort-based
   * fallback sorter has no analogous per-row size limit, so a row that defeats the arena can
   * still be handled by falling back, same as a genuine OOM.
   */
  private def storeAtSlot(
      row: UnsafeRow, orderKey: UnsafeRow, buffer: UnsafeRow, slot: Int): Unit = {
    val address =
      try {
        rowArena.append(row)
      } catch {
        case _: SparkOutOfMemoryError | _: SparkUnsupportedOperationException =>
          switchToFallback(row)
          return
      }
    writeSlotFrom(buffer, slot, orderKey)
    buffer.setLong(slotAddrOrdinal(slot), address)
  }

  /** Call once, after every row has been passed to [[consumeRow]], to get the output rows. */
  def finish(): Iterator[InternalRow] = {
    if (fallbackArray != null) {
      peakMemory.set(math.max(peakMemory.value, fallbackArray.spillSize))
      spillSize.set(
        TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      val rawIter = fallbackArray.generateIterator()
      new Iterator[InternalRow] {
        override def hasNext: Boolean = rawIter.hasNext
        override def next(): InternalRow = {
          numOutputRows += 1
          rawIter.next()
        }
      }
    } else if (fallbackSorter != null) {
      peakMemory.set(math.max(peakMemory.value, fallbackSorter.getPeakMemoryUsage))
      spillSize.set(
        TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      val sortedIter = fallbackSorter.sort()
      val limitFunc =
        (iter: Iterator[InternalRow]) => SimpleLimitIterator(iter, limit, numOutputRows)
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
   * `getKey`/`getValue` read the current entry) into a Scala `Iterator`, draining every populated
   * slot (`0 until heapSize`, see `emptyBuffer`'s doc) of each key in turn.
   */
  private def kvIteratorToScala(kvIter: KVIterator[UnsafeRow, UnsafeRow]): Iterator[InternalRow] = {
    val resultRow = new UnsafeRow(childOutput.length)
    new Iterator[InternalRow] {
      private var currentBuffer: UnsafeRow = null
      private var currentHeapSize: Int = 0
      private var nextSlot: Int = 0
      private var exhausted = false

      private def advanceToValidEntry(): Unit = {
        while (nextSlot >= currentHeapSize && !exhausted) {
          if (kvIter.next()) {
            currentBuffer = kvIter.getValue
            currentHeapSize = currentBuffer.getLong(heapSizeOrdinal).toInt
            nextSlot = 0
          } else {
            exhausted = true
          }
        }
      }

      override def hasNext: Boolean = {
        advanceToValidEntry()
        nextSlot < currentHeapSize
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException
        val address = currentBuffer.getLong(slotAddrOrdinal(nextSlot))
        nextSlot += 1
        numOutputRows += 1
        rowArena.getRow(address, resultRow)
      }
    }
  }

  /**
   * Checks two independent signals of whether hash accumulation is still paying off, proactively
   * falling back rather than waiting for the hash map to genuinely run out of memory (which, for
   * high-cardinality keys, may happen far later than it should, if at all -- see
   * [[HashRowNumberGroupLimitAccumulator]]'s class doc):
   *   - rows-processed-per-distinct-key has fallen below `minCompactionRatio` -- duplicate keys
   *     aren't being pruned enough to offset the hash map's per-row overhead; or
   *   - the fraction of rows, in the just-completed window, that actually triggered a heap write
   *     exceeds `maxAppendRatio` -- the first signal alone misses a healthy-looking distinct-key
   *     ratio where almost every row still replaces/extends its key's kept state anyway (e.g.
   *     taking the latest `limit` rows per key from a roughly time-ordered stream), which grows
   *     the arena close to linearly in the row count instead of the key count.
   *
   * @param completedWindowSize the number of rows in the just-completed check window -- not
   *                             always `minRowsBetweenCompactionChecks`, see
   *                             `currentCheckInterval`.
   */
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
        s"last $completedWindowSize rows triggered a heap write, above the maximum " +
        s"$maxAppendRatio.")
      switchToFallbackProactively()
    }
  }

  /**
   * Reached when the hash map or the row arena could no longer acquire memory. Dumps every row
   * already known to be kept, plus `pendingRow` (the row that just failed to be
   * inserted/updated), into the fallback destination (a sorter for `Partial`, an unsorted array
   * for `Final`; see `fallbackSorter`/`fallbackArray`). Every row consumed after this point (see
   * [[consumeRow]]) goes straight there instead of the hash map.
   */
  private def switchToFallback(pendingRow: UnsafeRow): Unit = {
    logInfo("Hash-based window group limit falling back to sort-based: could not acquire more " +
      "memory for the hash map or the row arena.")
    if (mode == Final) {
      val array = buildFallbackArray()
      array.add(pendingRow)
      fallbackArray = array
    } else {
      val sorter = buildFallbackSorter()
      sorter.insertRow(pendingRow)
      fallbackSorter = sorter
    }
    numTasksFallBacked += 1
  }

  /**
   * Reached when [[maybeBailOutForPoorEfficiency]] decides hash accumulation isn't paying off.
   * Unlike [[switchToFallback]], there is no extra pending row to insert: the row that triggered
   * this check was already fully processed into `rowArena`/the map.
   */
  private def switchToFallbackProactively(): Unit = {
    if (mode == Final) {
      fallbackArray = buildFallbackArray()
    } else {
      fallbackSorter = buildFallbackSorter()
    }
    numTasksFallBacked += 1
  }

  /**
   * Drains every row already known to be kept (every populated slot, `0 until heapSize`, of every
   * key; see `emptyBuffer`'s doc) into a temporary, bounded (by distinct-key count times `limit`,
   * not total row count) JVM-heap buffer, then frees the map and arena -- deliberately in that
   * order. Shared by [[buildFallbackSorter]] and [[buildFallbackArray]]: whichever destination
   * structure comes next needs its own fresh execution memory, and map+arena may together already
   * be holding nearly all of the task's budget (that is, after all, routinely the very reason this
   * method is being called); constructing that destination while they're still alive and holding
   * that memory can itself fail to acquire memory, which would turn an attempt to gracefully
   * degrade into an uncaught `SparkOutOfMemoryError` instead. The temporary heap copy this
   * requires is a one-time cost at this (hopefully rare) transition, not a per-row cost on the
   * normal hot path -- unlike the per-row heap copy `UnsafeRowArena` replaced.
   */
  private def drainWinnersAndFreeStructures(): ArrayBuffer[UnsafeRow] = {
    val winners = new ArrayBuffer[UnsafeRow](map.getNumKeys() * limit)
    val scratchRow = new UnsafeRow(childOutput.length)
    val mapIter = map.iterator()
    while (mapIter.next()) {
      val buffer = mapIter.getValue
      val heapSize = buffer.getLong(heapSizeOrdinal).toInt
      var slot = 0
      while (slot < heapSize) {
        val address = buffer.getLong(slotAddrOrdinal(slot))
        winners += rowArena.getRow(address, scratchRow).copy()
        slot += 1
      }
    }
    peakMemory.set(math.max(peakMemory.value, map.getPeakMemoryUsedBytes + rowArena.bytesUsed))
    map.free()
    rowArena.free()
    winners
  }

  /**
   * Builds the `Final`-mode fallback destination: an unsorted, append-only, spill-capable row
   * array -- the same kind [[WindowExec]]'s own frame buffering uses -- seeded with every row
   * [[drainWinnersAndFreeStructures]] already knows to be kept. Deliberately not a sorter: see
   * `fallbackArray`'s doc for why `Final` never needs one.
   */
  private def buildFallbackArray(): ExternalAppendOnlyUnsafeRowArray = {
    val winners = drainWinnersAndFreeStructures()
    // 0 in-memory-buffer thresholds: every row goes straight into the spill-capable backing
    // store, which is accounted Tungsten memory like the structures it replaces, rather than
    // accumulating as unaccounted JVM heap first (see UnsafeRowArena's class doc for why that
    // distinction matters at this potentially-large, all-rows-kept size). The spill thresholds
    // stay effectively unbounded, matching WINDOW_EXEC_BUFFER_SPILL_THRESHOLD's own
    // "never force, only spill under genuine memory pressure" default.
    val array = new ExternalAppendOnlyUnsafeRowArray(0, 0L, Int.MaxValue, Long.MaxValue)
    winners.foreach(array.add)
    array
  }

  /**
   * Builds the `Partial`-mode fallback sorter -- the same (spill-capable) sorter
   * [[org.apache.spark.sql.execution.SortExec]] itself uses -- seeded with every row
   * [[drainWinnersAndFreeStructures]] already knows to be kept.
   */
  private def buildFallbackSorter(): UnsafeExternalRowSorter = {
    val winners = drainWinnersAndFreeStructures()

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

object HashRowNumberGroupLimitAccumulator {
  // Matches UnsafeHashedRelation/LongToUnsafeRowMap's own choice of initial page size for a
  // BytesToBytesMap-backed structure expected to often hold little data (see HashedRelation.scala).
  // Shared with HashRankGroupLimitAccumulator, hence package-private rather than private.
  private[window] val INITIAL_PAGE_SIZE_BYTES: Long = 1L << 20

  // Below this, the periodic compaction check always runs at the fixed
  // minRowsBetweenCompactionChecks interval, unchanged from before ramp-up existed -- several
  // low-level tests hand-tune small minRowsBetweenCompactionChecks values (down to single digits)
  // to land the check's first-ever evaluation on an exact row, including one that must coincide
  // with a specific genuine-OOM row found by trial and error; a value this far below any of those
  // is guaranteed not to disturb them. At or above it (in particular, the real, much larger
  // default of 100000), ramp-up's smaller first checks start paying for themselves: see
  // consumeRow's doc.
  private[window] val COMPACTION_CHECK_RAMP_UP_THRESHOLD: Long = 1000L
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
    minRowsBetweenCompactionChecks: Long,
    enableRadixSort: Boolean,
    limit: Int,
    maxAppendRatio: Double = 0.9,
    mode: WindowGroupLimitMode = Partial)
  extends HashGroupLimitEvaluatorFactory {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new HashRowNumberGroupLimitPartitionEvaluator

  /**
   * Builds a fresh, per-task [[HashRowNumberGroupLimitAccumulator]]. Used directly by
   * [[WindowGroupLimitExec]]'s whole-stage codegen, which drives the accumulator with generated
   * code instead of going through [[createEvaluator]]'s pull-based [[PartitionEvaluator]].
   */
  override def createAccumulator(): HashRowNumberGroupLimitAccumulator =
    new HashRowNumberGroupLimitAccumulator(
      partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
      numTasksFallBacked, minCompactionRatio, minRowsBetweenCompactionChecks, enableRadixSort,
      limit = limit, maxAppendRatio = maxAppendRatio, mode = mode)

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
