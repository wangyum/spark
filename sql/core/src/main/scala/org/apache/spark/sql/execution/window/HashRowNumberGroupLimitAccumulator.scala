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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
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
import org.apache.spark.unsafe.{KVIterator, Platform}

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

  private val singleColTypeTag: Int =
    if (numOrderCols == 1) {
      orderSpec.head.child.dataType match {
        case LongType | TimestampType | TimestampNTZType => 1
        case IntegerType | DateType => 2
        case DoubleType => 3
        case FloatType => 4
        case ShortType => 5
        case ByteType => 6
        case BooleanType => 7
        case _ => 0
      }
    } else {
      0
    }

  private val isSingleColAscending = numOrderCols == 1 && orderSpec.head.direction == Ascending
  private val isSingleColNullsFirst = numOrderCols == 1 && orderSpec.head.nullOrdering == NullsFirst
  private val isSingleColNullable = numOrderCols == 1 && orderSpec.head.child.nullable
  private val bitSetWidthInBytes =
    UnsafeRow.calculateBitSetWidthInBytes(bufferSchema.length)
  private val heapSizeByteOffset = bitSetWidthInBytes + heapSizeOrdinal * 8L

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
    if (singleColTypeTag != 0) {
      val ordA = slotOrderOrdinal(a, 0)
      val ordB = slotOrderOrdinal(b, 0)
      val aNull = buffer.isNullAt(ordA)
      val bNull = buffer.isNullAt(ordB)
      if (aNull && bNull) return 0
      if (aNull) return if (isSingleColNullsFirst) -1 else 1
      if (bNull) return if (isSingleColNullsFirst) 1 else -1
      val aVal = buffer.getLong(ordA)
      val bVal = buffer.getLong(ordB)
      val cmp = singleColTypeTag match {
        case 1 => java.lang.Long.compare(aVal, bVal)
        case 2 => java.lang.Integer.compare(aVal.toInt, bVal.toInt)
        case 3 =>
          java.lang.Double.compare(
            java.lang.Double.longBitsToDouble(aVal),
            java.lang.Double.longBitsToDouble(bVal))
        case 4 =>
          java.lang.Float.compare(
            java.lang.Float.intBitsToFloat(aVal.toInt),
            java.lang.Float.intBitsToFloat(bVal.toInt))
        case 5 => java.lang.Short.compare(aVal.toShort, bVal.toShort)
        case 6 => java.lang.Byte.compare(aVal.toByte, bVal.toByte)
        case 7 => java.lang.Boolean.compare(aVal != 0, bVal != 0)
        case _ => 0
      }
      if (isSingleColAscending) cmp else -cmp
    } else {
      extractSlotInto(buffer, a, scratchA)
      extractSlotInto(buffer, b, scratchB)
      boundOrdering.compare(scratchA, scratchB)
    }
  }

  private def compareSlotSingle(
      kVal: Long, kNull: Boolean, baseObj: AnyRef, baseOffset: Long, slot: Int): Int = {
    val ord = slotOrderOrdinal(slot, 0)
    if (isSingleColNullable) {
      val sNull = (Platform.getByte(baseObj, baseOffset + (ord >>> 3)) & (1 << (ord & 7))) != 0
      if (kNull && sNull) return 0
      if (kNull) return if (isSingleColNullsFirst) -1 else 1
      if (sNull) return if (isSingleColNullsFirst) 1 else -1
    }
    val sVal = Platform.getLong(baseObj, baseOffset + bitSetWidthInBytes + ord * 8L)
    val cmp = singleColTypeTag match {
      case 1 => java.lang.Long.compare(kVal, sVal)
      case 2 => java.lang.Integer.compare(kVal.toInt, sVal.toInt)
      case 3 =>
        java.lang.Double.compare(
          java.lang.Double.longBitsToDouble(kVal),
          java.lang.Double.longBitsToDouble(sVal))
      case 4 =>
        java.lang.Float.compare(
          java.lang.Float.intBitsToFloat(kVal.toInt),
          java.lang.Float.intBitsToFloat(sVal.toInt))
      case 5 => java.lang.Short.compare(kVal.toShort, sVal.toShort)
      case 6 => java.lang.Byte.compare(kVal.toByte, sVal.toByte)
      case 7 => java.lang.Boolean.compare(kVal != 0, sVal != 0)
      case _ => 0
    }
    if (isSingleColAscending) cmp else -cmp
  }

  private def compareSlotSingle(
      kVal: Long, kNull: Boolean, buffer: UnsafeRow, slot: Int): Int = {
    compareSlotSingle(kVal, kNull, buffer.getBaseObject, buffer.getBaseOffset, slot)
  }

  private def storeAtSlotSingle(
      row: UnsafeRow, kVal: Long, kNull: Boolean, buffer: UnsafeRow, slot: Int): Unit = {
    val address = rowArena.append(row)
    if (address == UnsafeRowArena.NO_NEXT) {
      switchToFallback(row)
      return
    }
    val ord = slotOrderOrdinal(slot, 0)
    if (kNull) buffer.setNullAt(ord) else buffer.setLong(ord, kVal)
    buffer.setLong(slotAddrOrdinal(slot), address)
  }

  /** `< 0` if `orderKey` is strictly better (should win) than slot `slot`'s current key. */
  private def compareToSlot(orderKey: UnsafeRow, buffer: UnsafeRow, slot: Int): Int = {
    if (singleColTypeTag != 0) {
      compareSlotSingle(orderKey.getLong(0), orderKey.isNullAt(0), buffer, slot)
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

  override def consumeRow(
      row: UnsafeRow,
      key: UnsafeRow,
      keyHash: Int,
      orderKeyVal: Long,
      orderKeyIsNull: Boolean): Unit = {
    val buffer = map.getAggregationBufferFromUnsafeRow(key, keyHash)
    if (buffer == null) {
      switchToFallback(row)
      return
    }
    val heapSize = buffer.getLong(heapSizeOrdinal).toInt
    if (heapSize < limit) {
      storeAtSlotSingle(row, orderKeyVal, orderKeyIsNull, buffer, heapSize)
      if (fallbackDestination != null) return
      buffer.setLong(heapSizeOrdinal, heapSize + 1)
      siftUp(buffer, heapSize)
    } else if (compareSlotSingle(orderKeyVal, orderKeyIsNull, buffer, 0) < 0) {
      storeAtSlotSingle(row, orderKeyVal, orderKeyIsNull, buffer, 0)
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
    val address = rowArena.append(row)
    if (address == UnsafeRowArena.NO_NEXT) {
      switchToFallback(row)
      return
    }
    writeSlotFrom(buffer, slot, orderKey)
    buffer.setLong(slotAddrOrdinal(slot), address)
  }

  private var kvIter: KVIterator[UnsafeRow, UnsafeRow] = _
  private var fallbackIter: Iterator[InternalRow] = _
  private var currentBuffer: UnsafeRow = _
  private var currentHeapSize: Int = 0
  private var nextSlot: Int = 0
  private var uncommittedOutputRows: Int = 0

  override def finishAccumulator(): Unit = fallbackDestination match {
    case ArrayFallbackDestination(array) =>
      peakMemory.set(math.max(peakMemory.value, array.spillSize))
      spillSize.set(
        TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      fallbackIter = array.generateIterator()
    case SorterFallbackDestination(sorter) =>
      peakMemory.set(math.max(peakMemory.value, sorter.getPeakMemoryUsage))
      spillSize.set(
        TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore)
      val sortedIter = sorter.sort()
      val limitFunc =
        (iter: Iterator[InternalRow]) => SimpleLimitIterator(iter, limit, numOutputRows)
      fallbackIter = if (partitionSpec.isEmpty) {
        limitFunc(sortedIter)
      } else {
        new GroupedLimitIterator(sortedIter, childOutput, partitionSpec, limitFunc)
      }
    case null =>
      peakMemory.set(math.max(peakMemory.value, map.getPeakMemoryUsedBytes + rowArena.bytesUsed))
      kvIter = map.iterator()
  }

  override def nextResultRow(resultRow: UnsafeRow): Boolean = {
    if (fallbackIter != null) {
      if (fallbackIter.hasNext) {
        val r = fallbackIter.next().asInstanceOf[UnsafeRow]
        resultRow.pointTo(r.getBaseObject, r.getBaseOffset, r.getSizeInBytes)
        numOutputRows += 1
        true
      } else {
        false
      }
    } else {
      while (true) {
        if (nextSlot < currentHeapSize) {
          val bObj = currentBuffer.getBaseObject
          val bOff = currentBuffer.getBaseOffset
          val addrOffset = bOff + bitSetWidthInBytes + slotAddrOrdinal(nextSlot) * 8L
          val address = Platform.getLong(bObj, addrOffset)
          nextSlot += 1
          uncommittedOutputRows += 1
          if (uncommittedOutputRows == HashGroupLimitAccumulator.OUTPUT_ROWS_METRIC_BATCH_SIZE) {
            numOutputRows += HashGroupLimitAccumulator.OUTPUT_ROWS_METRIC_BATCH_SIZE
            uncommittedOutputRows = 0
          }
          rowArena.getRow(address, resultRow)
          return true
        } else if (kvIter != null && kvIter.next()) {
          currentBuffer = kvIter.getValue
          val bObj = currentBuffer.getBaseObject
          val bOff = currentBuffer.getBaseOffset
          currentHeapSize = Platform.getLong(bObj, bOff + heapSizeByteOffset).toInt
          nextSlot = 0
        } else {
          if (uncommittedOutputRows > 0) {
            numOutputRows += uncommittedOutputRows
            uncommittedOutputRows = 0
          }
          return false
        }
      }
      false
    }
  }

  override def finish(): Iterator[InternalRow] = {
    finishAccumulator()
    val row = new UnsafeRow(childOutput.length)
    new Iterator[InternalRow] {
      private var hasPending = false
      override def hasNext: Boolean = {
        if (hasPending) true
        else {
          hasPending = nextResultRow(row)
          hasPending
        }
      }
      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException
        hasPending = false
        row
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
