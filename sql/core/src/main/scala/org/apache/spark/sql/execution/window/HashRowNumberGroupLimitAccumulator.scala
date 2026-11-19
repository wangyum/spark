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

import org.apache.spark.{SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BindReferences,
  BoundReference, Expression, GenericInternalRow, NullsFirst, RowOrdering, SortOrder, SortPrefix,
  UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SortPrefixUtils,
  UnsafeExternalRowSorter, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType,
  IntegerType, LongType, ShortType, StructField, StructType, TimestampNTZType, TimestampType}
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
 *   - periodically ([[maybeBailOutForPoorEfficiency]], [[switchToFallbackProactively]]), the ratio
 *     of rows processed to distinct keys seen falls below `minCompactionRatio` (hash accumulation
 *     isn't pruning enough duplicate rows to be worth its per-row overhead -- mirrors
 *     `HashAggregateExec`'s adaptive partial aggregation, which uses the same compaction-ratio
 *     heuristic, `spark.sql.execution.aggregate.adaptivePartialAggregation.*`, for the same
 *     reason, against a different fallback mechanism here).
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

  private val singleColComparator: ((Long, Long) => Int) =
    if (numOrderCols == 1) {
      orderSpec.head.child.dataType match {
        case LongType | TimestampType | TimestampNTZType =>
          (v1: Long, v2: Long) => java.lang.Long.compare(v1, v2)
        case IntegerType | DateType =>
          (v1: Long, v2: Long) => java.lang.Integer.compare(v1.toInt, v2.toInt)
        case ShortType =>
          (v1: Long, v2: Long) => java.lang.Short.compare(v1.toShort, v2.toShort)
        case ByteType =>
          (v1: Long, v2: Long) => java.lang.Byte.compare(v1.toByte, v2.toByte)
        case BooleanType =>
          (v1: Long, v2: Long) => java.lang.Boolean.compare(v1 != 0, v2 != 0)
        case FloatType =>
          (v1: Long, v2: Long) =>
            java.lang.Float.compare(
              java.lang.Float.intBitsToFloat(v1.toInt),
              java.lang.Float.intBitsToFloat(v2.toInt))
        case DoubleType =>
          (v1: Long, v2: Long) =>
            java.lang.Double.compare(
              java.lang.Double.longBitsToDouble(v1),
              java.lang.Double.longBitsToDouble(v2))
        case _ => null
      }
    } else {
      null
    }

  private val isSingleColAscending = numOrderCols == 1 && orderSpec.head.direction == Ascending
  private val isSingleColNullsFirst = numOrderCols == 1 && orderSpec.head.nullOrdering == NullsFirst

  /** Writes `src`'s (0-based) order-key fields into slot `slot` of `buffer`. */
  private def writeSlotFrom(buffer: UnsafeRow, slot: Int, src: UnsafeRow): Unit = {
    if (numOrderCols == 1) {
      val ord = slotOrderOrdinal(slot, 0)
      if (src.isNullAt(0)) buffer.setNullAt(ord) else buffer.setLong(ord, src.getLong(0))
    } else {
      var i = 0
      while (i < numOrderCols) {
        val ord = slotOrderOrdinal(slot, i)
        if (src.isNullAt(i)) buffer.setNullAt(ord) else buffer.setLong(ord, src.getLong(i))
        i += 1
      }
    }
  }

  private def compareSlots(buffer: UnsafeRow, a: Int, b: Int): Int = {
    if (singleColComparator != null) {
      val ordA = slotOrderOrdinal(a, 0)
      val ordB = slotOrderOrdinal(b, 0)
      val aNull = buffer.isNullAt(ordA)
      val bNull = buffer.isNullAt(ordB)
      if (aNull && bNull) return 0
      if (aNull) return if (isSingleColNullsFirst) -1 else 1
      if (bNull) return if (isSingleColNullsFirst) 1 else -1
      val cmp = singleColComparator(buffer.getLong(ordA), buffer.getLong(ordB))
      if (isSingleColAscending) cmp else -cmp
    } else {
      extractSlotInto(buffer, a, scratchA)
      extractSlotInto(buffer, b, scratchB)
      boundOrdering.compare(scratchA, scratchB)
    }
  }

  /** `< 0` if `orderKey` is strictly better (should win) than slot `slot`'s current key. */
  private def compareToSlot(orderKey: UnsafeRow, buffer: UnsafeRow, slot: Int): Int = {
    if (singleColComparator != null) {
      val ord = slotOrderOrdinal(slot, 0)
      val kNull = orderKey.isNullAt(0)
      val sNull = buffer.isNullAt(ord)
      if (kNull && sNull) return 0
      if (kNull) return if (isSingleColNullsFirst) -1 else 1
      if (sNull) return if (isSingleColNullsFirst) 1 else -1
      val cmp = singleColComparator(orderKey.getLong(0), buffer.getLong(ord))
      if (isSingleColAscending) cmp else -cmp
    } else {
      extractSlotInto(buffer, slot, scratchA)
      boundOrdering.compare(orderKey, scratchA)
    }
  }

  private def swapSlots(buffer: UnsafeRow, a: Int, b: Int): Unit = {
    if (numOrderCols == 1) {
      val ordA = slotOrderOrdinal(a, 0)
      val ordB = slotOrderOrdinal(b, 0)
      val aNull = buffer.isNullAt(ordA)
      val aVal = buffer.getLong(ordA)
      val bNull = buffer.isNullAt(ordB)
      val bVal = buffer.getLong(ordB)
      if (bNull) buffer.setNullAt(ordA) else buffer.setLong(ordA, bVal)
      if (aNull) buffer.setNullAt(ordB) else buffer.setLong(ordB, aVal)
      val addrA = buffer.getLong(slotAddrOrdinal(a))
      val addrB = buffer.getLong(slotAddrOrdinal(b))
      buffer.setLong(slotAddrOrdinal(a), addrB)
      buffer.setLong(slotAddrOrdinal(b), addrA)
    } else {
      extractSlotInto(buffer, a, scratchA)
      extractSlotInto(buffer, b, scratchB)
      val addrA = buffer.getLong(slotAddrOrdinal(a))
      val addrB = buffer.getLong(slotAddrOrdinal(b))
      writeSlotFrom(buffer, a, scratchB)
      writeSlotFrom(buffer, b, scratchA)
      buffer.setLong(slotAddrOrdinal(a), addrB)
      buffer.setLong(slotAddrOrdinal(b), addrA)
    }
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
    HashGroupLimitAccumulator.INITIAL_PAGE_SIZE_BYTES)

  // Holds the actual candidate rows in Tungsten pages (see UnsafeRowArena's class doc for why this
  // replaced a plain JVM-heap `ArrayBuffer[UnsafeRow]`: avoids unaccounted heap allocation and GC
  // pressure on every winner/replacement, and makes this memory visible to the same backpressure
  // signal (SparkOutOfMemoryError) the map itself already reacts to).
  private var rowArena: UnsafeRowArena = new UnsafeRowArena(taskMemoryManager, linked = false)

  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  // Rows consumed by the hash path (not counting rows already routed to fallbackDestination), used
  // only to decide when to run the next periodic efficiency check.
  private var rowsConsumed: Long = 0L

  private var nextCompactionCheckAt: Long = minRowsBetweenCompactionChecks

  // Non-null once the map or the row arena could no longer acquire memory, or the compaction
  // ratio proved too low; from then on every row -- including ones already recorded as kept -- is
  // routed here instead, and the hash map/row arena are abandoned. A [[SorterFallbackDestination]]
  // under `mode == Partial`: its fallback output still has to be filtered down to `limit` rows per
  // key, since it is the only thing standing between its task's rows and the shuffle -- skipping
  // that filtering would ship every row across the network, losing the whole point of the
  // pre-filter, and a sort-based filter needs sorted input, hence a real sorter. An
  // [[ArrayFallbackDestination]] under `mode == Final`, where every row is instead appended
  // unfiltered and unsorted: safe specifically because a hash-eligible WindowGroupLimitExec always
  // declares outputOrdering = Nil (see isHashEligible), so EnsureRequirements unconditionally
  // plants a real Sort directly above a Final node feeding the real WindowExec + its
  // Filter(rn <= limit) above that -- which recomputes the correct answer from whatever rows
  // arrive, regardless of how many that is. Skipping Final's own sort-then-filter here avoids
  // sorting the same rows twice: once internally (for no benefit, since nothing downstream relies
  // on Final's output being pre-filtered) and again in that unconditional Sort.
  private var fallbackDestination: FallbackDestination = null

  override def fallbackDestinationOrNull: FallbackDestination = fallbackDestination

  /**
   * Consumes one input row. Safe to call in any order; the hash path doesn't need sorted input.
   * Used by the interpreted path (see [[HashRowNumberGroupLimitEvaluatorFactory]]), which has
   * only a full `row` to work with -- computes `key`/`orderKey` itself via this accumulator's own
   * projections, then delegates to the shared three-argument overload below.
   */
  def consumeRow(row: UnsafeRow): Unit = {
    if (fallbackDestination != null) {
      fallbackDestination.insertRow(row)
      return
    }
    val key = groupingProjection(row)
    consumeRow(row, key, key.hashCode(), orderKeyProjection(row))
  }

  override def consumeRow(row: UnsafeRow, key: UnsafeRow, orderKey: UnsafeRow): Unit =
    consumeRow(row, key, key.hashCode(), orderKey)

  override def consumeRow(
      row: UnsafeRow, key: UnsafeRow, keyHash: Int, orderKey: UnsafeRow): Unit = {
    val buffer = map.getAggregationBufferFromUnsafeRow(key, keyHash)
    if (buffer == null) {
      switchToFallback(row)
      return
    }
    val heapSize = buffer.getLong(heapSizeOrdinal).toInt
    if (heapSize < limit) {
      storeAtSlot(row, orderKey, buffer, heapSize)
      if (fallbackDestination != null) return
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
      if (fallbackDestination != null) return
      siftDown(buffer, 0, limit)
    }
    rowsConsumed += 1
    if (minRowsBetweenCompactionChecks > 0 && rowsConsumed >= nextCompactionCheckAt) {
      maybeBailOutForPoorEfficiency()
      nextCompactionCheckAt = rowsConsumed + minRowsBetweenCompactionChecks
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

  /**
   * Call once, after every row has been passed to [[consumeRow]], to get the output rows.
   * [[fallbackDestination]]'s two cases finish in genuinely different ways -- `Final`'s array
   * needs no further filtering or sorting (see `fallbackDestination`'s doc), while `Partial`'s
   * sorter output must still be sorted and filtered down to `limit` rows per key -- so, unlike
   * every other [[FallbackDestination]] call site, this one pattern-matches on the concrete
   * destination rather than going through [[FallbackDestination.insertRow]].
   */
  def finish(): Iterator[InternalRow] = fallbackDestination match {
    case ArrayFallbackDestination(array) =>
      peakMemory.set(math.max(peakMemory.value, array.spillSize))
      spillSize.set(
        TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
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
      spillSize.set(
        TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      val sortedIter = sorter.sort()
      val limitFunc =
        (iter: Iterator[InternalRow]) => SimpleLimitIterator(iter, limit, numOutputRows)
      if (partitionSpec.isEmpty) {
        limitFunc(sortedIter)
      } else {
        new GroupedLimitIterator(sortedIter, childOutput, partitionSpec, limitFunc)
      }
    case null =>
      peakMemory.set(math.max(peakMemory.value, map.getPeakMemoryUsedBytes + rowArena.bytesUsed))
      // Lazy: the map's own TaskCompletionListener (registered in its constructor) and
      // rowArena's (registered in UnsafeRowArena's constructor) guarantee eventual cleanup even
      // if this iterator is never fully drained (e.g. a downstream LIMIT), so there is no eager
      // map.free()/rowArena.free() call here the way the fallback branches above have for their
      // own destinations -- both structures must stay alive for as long as the returned rows are
      // read.
      kvIteratorToScala(map.iterator())
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

      private var uncommittedOutputRows = 0

      override def hasNext: Boolean = {
        advanceToValidEntry()
        val hasMore = nextSlot < currentHeapSize
        if (!hasMore && uncommittedOutputRows > 0) {
          numOutputRows += uncommittedOutputRows
          uncommittedOutputRows = 0
        }
        hasMore
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException
        val address = currentBuffer.getLong(slotAddrOrdinal(nextSlot))
        nextSlot += 1
        uncommittedOutputRows += 1
        if (uncommittedOutputRows == 1024) {
          numOutputRows += 1024
          uncommittedOutputRows = 0
        }
        rowArena.getRow(address, resultRow)
      }
    }
  }

  /**
   * Checks whether hash accumulation is still paying off, proactively falling back rather than
   * waiting for the hash map to genuinely run out of memory (which, for high-cardinality keys, may
   * happen far later than it should, if at all -- see [[HashRowNumberGroupLimitAccumulator]]'s
   * class doc): rows-processed-per-distinct-key has fallen below `minCompactionRatio` -- duplicate
   * keys aren't being pruned enough to offset the hash map's per-row overhead.
   */
  private def maybeBailOutForPoorEfficiency(): Unit = {
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
   * already known to be kept, plus `pendingRow` (the row that just failed to be
   * inserted/updated), into the fallback destination (a sorter for `Partial`, an unsorted array
   * for `Final`; see `fallbackDestination`'s doc). Every row consumed after this point (see
   * [[consumeRow]]) goes straight there instead of the hash map.
   */
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

  /**
   * Reached when [[maybeBailOutForPoorEfficiency]] decides hash accumulation isn't paying off.
   * Unlike [[switchToFallback]], there is no extra pending row to insert: the row that triggered
   * this check was already fully processed into `rowArena`/the map.
   */
  private def switchToFallbackProactively(): Unit = {
    fallbackDestination = if (mode == Final) {
      ArrayFallbackDestination(buildFallbackArray())
    } else {
      SorterFallbackDestination(buildFallbackSorter())
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
   * `fallbackDestination`'s doc for why `Final` never needs one.
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
