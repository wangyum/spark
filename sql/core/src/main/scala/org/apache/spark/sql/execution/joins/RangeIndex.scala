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

package org.apache.spark.sql.execution.joins

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Broadcast payload of a range join. An index only names candidate rows.
 * [[BroadcastRangeJoinExec]] keeps a candidate when the original join condition
 * is true, so inclusivity never lives in the index.
 *
 *  - [[IntervalIndex]] answers "which ranges overlap this window?"
 *  - [[PointIndex]] answers "which points lie on this side of a bound?"
 */
private[execution] sealed trait RangeRelation extends Serializable {
  def sizeInBytes(): Long
}

private[execution] object RangeIndex {

  /** In-memory size of one indexed row. Non-unsafe rows are a test-only estimate. */
  def rowSize(row: InternalRow): Long = row match {
    case u: UnsafeRow => u.getSizeInBytes.toLong
    case _ => row.numFields * 8L
  }

  /**
   * One sweep-line event used to build an [[IntervalIndex]].
   *
   * @param key   the bound value this event occurs at
   * @param flow  [[RangeEvent.Start]], [[RangeEvent.Point]], or [[RangeEvent.End]]
   * @param row   the build-side row that produced this event
   * @param index a unique id for `row` so deactivation is O(1)
   */
  case class RangeEvent(key: Any, flow: Int, row: InternalRow, index: Int)

  object RangeEvent {
    /** Interval start. Sorts after Point at the same key. */
    val Start = 1
    /** Degenerate interval (low == high). */
    val Point = 0
    /** Interval end. Sorts before Point at the same key. */
    val End = -1
  }

  /**
   * A type-specialized accessor that reads a single field from an `InternalRow` as a boxed
   * value (or `null` if the field is null). Shared by the exchange (build-side key extraction)
   * and the join (stream-side key extraction) so the dispatch table lives in one place.
   */
  def getValue(dt: DataType, ordinal: Int): InternalRow => Any = {
    // Same set as `RangePredicate.supportedType`. Anything else is rejected at
    // planning; reaching this match is a bug, and `InternalRow.get` would not
    // match the value codegen compares.
    dt match {
      case BooleanType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getBoolean(ordinal)
      case ByteType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getByte(ordinal)
      case ShortType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getShort(ordinal)
      case IntegerType | DateType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getInt(ordinal)
      case LongType | TimestampType | TimestampNTZType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getLong(ordinal)
      case FloatType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getFloat(ordinal)
      case DoubleType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getDouble(ordinal)
      case _: StringType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getUTF8String(ordinal)
      case BinaryType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getBinary(ordinal)
      case t: DecimalType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getDecimal(ordinal, t.precision, t.scale)
      case u: UserDefinedType[_] => getValue(u.sqlType, ordinal)
      case _ =>
        throw SparkException.internalError(
          s"Range join does not support key type ${dt.sql}.")
    }
  }

  /** Turn a projected `(low, high)` row into sweep events. Nulls and `low > high` emit none. */
  def toRangeEvent(
      buildSideKeyValueGetter: List[InternalRow => Any],
      lowHighExtr: Projection,
      cmp: Ordering[Any]): (InternalRow, Int) => Seq[RangeEvent] = {
    (row: InternalRow, index: Int) => {
      val lowHigh: InternalRow = lowHighExtr(row)
      val low = buildSideKeyValueGetter(0)(lowHigh)
      val high = buildSideKeyValueGetter(1)(lowHigh)
      if (low != null && high != null) {
        val result = cmp.compare(low, high)
        if (result == 0) {
          RangeEvent(low, RangeEvent.Point, row, index) :: Nil
        } else if (result < 0) {
          RangeEvent(low, RangeEvent.Start, row, index) ::
            RangeEvent(high, RangeEvent.End, row, index) :: Nil
        } else {
          Nil
        }
      } else {
        Nil
      }
    }
  }
}

/**
 * Sweep-line index of build-side intervals. `overlapping` returns every row whose
 * interval may overlap `[low, high]`. The window is a superset: the low search is
 * exclusive and the high search is inclusive, and the caller drops false positives.
 */
private[execution] object IntervalIndex {

  /** Build an interval index from unsorted events. */
  def build(ordering: Ordering[Any], events: Array[RangeIndex.RangeEvent]): IntervalIndex = {
    val eventComparator = new java.util.Comparator[RangeIndex.RangeEvent] {
      override def compare(a: RangeIndex.RangeEvent, b: RangeIndex.RangeEvent): Int = {
        val keyCmp = ordering.compare(a.key, b.key)
        if (keyCmp != 0) keyCmp else Integer.compare(a.flow, b.flow)
      }
    }
    java.util.Arrays.sort(events, eventComparator)
    buildFromSorted(ordering, events)
  }

  private def buildFromSorted(
      ordering: Ordering[Any],
      events: Array[RangeIndex.RangeEvent]): IntervalIndex = {
    // A leading null key makes the binary search uniform, including an empty index.
    val empty = Array.empty[InternalRow]
    val keys = mutable.Buffer[Any](null)
    val offsets = mutable.Buffer[Int](0)
    val activatedRows = mutable.Buffer.empty[InternalRow]
    val activeNewOffsets = mutable.Buffer.empty[Int]
    val activeRows = mutable.Buffer.empty[Array[InternalRow]]

    // Keyed by the event id so a row leaves the active set in O(1).
    var currentKey: Any = null
    var currentActiveNewOffset: Int = -1
    val currentActiveRows = mutable.LinkedHashMap.empty[Int, InternalRow]

    def writeActiveRows(): Unit = {
      activeNewOffsets += currentActiveNewOffset
      if (currentActiveRows.isEmpty) activeRows += empty
      else activeRows += currentActiveRows.values.toArray
    }

    events.foreach { event =>
      if (currentKey != event.key) {
        writeActiveRows()
        currentKey = event.key
        currentActiveNewOffset = -1
        keys += event.key
        offsets += activatedRows.size
      }
      // Rows newly active at this key start here. Rows before it span the gap below.
      if (event.flow >= RangeIndex.RangeEvent.Point && currentActiveNewOffset == -1) {
        currentActiveNewOffset = currentActiveRows.size
      }
      event.flow match {
        case RangeIndex.RangeEvent.Start =>
          activatedRows += event.row
          currentActiveRows += event.index -> event.row
        case RangeIndex.RangeEvent.Point =>
          activatedRows += event.row
        case RangeIndex.RangeEvent.End =>
          currentActiveRows -= event.index
      }
    }
    writeActiveRows()

    new IntervalIndex(ordering, keys.toArray, offsets.toArray, activeNewOffsets.toArray,
      activeRows.toArray, activatedRows.toArray)
  }
}

private[execution] class IntervalIndex(
    private[this] val ordering: Ordering[Any],
    private[this] val keys: Array[Any],
    private[this] val offsets: Array[Int],
    private[this] val activeNewOffsets: Array[Int],
    private[this] val active: Array[Array[InternalRow]],
    private[this] val activated: Array[InternalRow])
  extends RangeRelation {

  override def sizeInBytes(): Long = activated.map(RangeIndex.rowSize).sum

  private[this] val maxKeyIndex = keys.length - 1

  /**
   * Index of the rightmost key that is lower than `value`, or equal to it when
   * `inclusive` is true. A null key compares smaller than every value.
   */
  @tailrec
  private def closestLowerKey(
      value: Any,
      inclusive: Boolean,
      first: Int = 0,
      last: Int = maxKeyIndex): Int = {
    val mid = first + ((last - first + 1) >>> 1)
    val key = keys(mid)
    val cmp = if (key == null) 1 else ordering.compare(value, key)
    if (cmp == 0) {
      if (inclusive) mid else mid - 1
    } else if (first == last) {
      mid
    } else if (cmp > 0) {
      closestLowerKey(value, inclusive, mid, last)
    } else {
      closestLowerKey(value, inclusive, first, mid - 1)
    }
  }

  /**
   * Rows whose interval may overlap `[low, high]`.
   *
   * `first` is the last key strictly below `low`, so active rows there include
   * intervals that end at `low`. `last` is the last key at or below `high`.
   * Three layouts fall out of that pair:
   *  - `first < last`: active rows at `first`, then points activated on `(first, last]`.
   *  - `first == last`: only the active rows; the probe did not land on a key.
   *  - `first > last`: the probe sits in a gap. Only rows active before `keys(first)`
   *    span it; `activeNewOffsets(first)` is where the new rows begin.
   */
  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (keys.length == 1 || low == null || high == null) return Iterator.empty

    val first = closestLowerKey(low, inclusive = false)
    val last = if (first == maxKeyIndex) maxKeyIndex
      else closestLowerKey(high, inclusive = true, first)

    new Iterator[InternalRow] {
      var activatedAvailable = first < last
      var rowIndex = 0
      var rows: Array[InternalRow] = active(first)
      var rowLength = if (first <= last || activeNewOffsets(first) < 0) rows.length
        else activeNewOffsets(first)

      override final def hasNext: Boolean = {
        var result = rowIndex < rowLength
        if (!result && activatedAvailable) {
          activatedAvailable = false
          rows = activated
          rowIndex = offsets(first + 1)
          rowLength = if (last == maxKeyIndex) activated.length else offsets(last + 1)
          result = rowIndex < rowLength
        }
        result
      }

      override final def next(): InternalRow = {
        val row = rows(rowIndex)
        rowIndex += 1
        row
      }
    }
  }
}

/**
 * Sorted build-side points. `upTo` and `from` both include equals, so `<` and
 * `<=` (and the two greater-than forms) share one broadcast. The join condition
 * drops the bound that does not belong.
 */
private[execution] object PointIndex {
  def build(ordering: Ordering[Any], keyedRows: Array[(Any, InternalRow)]): PointIndex = {
    val present = keyedRows.filter(_._1 != null)
    val comparator = new java.util.Comparator[(Any, InternalRow)] {
      override def compare(a: (Any, InternalRow), b: (Any, InternalRow)): Int =
        ordering.compare(a._1, b._1)
    }
    java.util.Arrays.sort(present, comparator)
    val keys = new Array[Any](present.length)
    val rows = new Array[InternalRow](present.length)
    var i = 0
    while (i < present.length) {
      keys(i) = present(i)._1
      rows(i) = present(i)._2
      i += 1
    }
    new PointIndex(ordering, keys, rows)
  }
}

private[execution] class PointIndex(
    private[this] val ordering: Ordering[Any],
    private[this] val keys: Array[Any],
    private[this] val rows: Array[InternalRow])
  extends RangeRelation {

  override def sizeInBytes(): Long = rows.map(RangeIndex.rowSize).sum

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(0, upperBound(value))

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(lowerBound(value), keys.length)

  private def slice(from: Int, until: Int): Iterator[InternalRow] = new Iterator[InternalRow] {
    private var i = from
    override def hasNext: Boolean = i < until
    override def next(): InternalRow = {
      val row = rows(i)
      i += 1
      row
    }
  }

  /** First index whose key is greater than or equal to `value`. */
  private def lowerBound(value: Any): Int = {
    var lo = 0
    var hi = keys.length
    while (lo < hi) {
      val mid = lo + ((hi - lo) >>> 1)
      if (ordering.compare(keys(mid), value) < 0) lo = mid + 1
      else hi = mid
    }
    lo
  }

  /** First index whose key is greater than `value`. */
  private def upperBound(value: Any): Int = {
    var lo = 0
    var hi = keys.length
    while (lo < hi) {
      val mid = lo + ((hi - lo) >>> 1)
      if (ordering.compare(keys(mid), value) <= 0) lo = mid + 1
      else hi = mid
    }
    lo
  }
}
