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

import scala.collection.immutable.IntMap
import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

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

  /** Sum of [[rowSize]] over `rows`. */
  def rowsSize(rows: Array[InternalRow]): Long = {
    var bytes = 0L
    var i = 0
    while (i < rows.length) { bytes += rowSize(rows(i)); i += 1 }
    bytes
  }

  /**
   * One sweep-line event used to build an [[IntervalIndex]].
   *
   * @param key   the bound value this event occurs at
   * @param flow  [[RangeEvent.Start]], [[RangeEvent.Point]], or [[RangeEvent.End]]
   * @param row   the build-side row that produced this event
   * @param index id of `row`, the active-set key that removes it without a scan
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
    val accessor = InternalRow.getAccessor(dt)
    (input: InternalRow) => accessor(input, ordinal)
  }

  /**
   * First index at or after `value`. `skipEquals` makes the bound strict, so
   * equal keys are skipped. [[IntervalIndex]]'s comparator sorts the leading
   * null key before every value, so this search also covers an empty index.
   *
   * `cmp` is a `java.util.Comparator` rather than a Scala function so the `Int` result
   * of every comparison in this binary search stays unboxed.
   */
  private def searchBound(
      keys: Array[Any],
      value: Any,
      skipEquals: Boolean,
      cmp: java.util.Comparator[Any]): Int = {
    var lo = 0
    var hi = keys.length
    while (lo < hi) {
      val mid = lo + ((hi - lo) >>> 1)
      val c = cmp.compare(keys(mid), value)
      // Strict upper bound also steps past equals. Otherwise equals stay in the upper half.
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }

  /** First index whose key is greater than or equal to `value`. Shared by both indexes. */
  def lowerBound(keys: Array[Any], value: Any, cmp: java.util.Comparator[Any]): Int =
    searchBound(keys, value, skipEquals = false, cmp)

  /** First index whose key is greater than `value`. Walks a whole equal run. Shared. */
  def upperBound(keys: Array[Any], value: Any, cmp: java.util.Comparator[Any]): Int =
    searchBound(keys, value, skipEquals = true, cmp)

  /** Iterator over `rows(from until until)` that does not copy the backing array. */
  def sliceIterator(rows: Array[InternalRow], from: Int, until: Int): Iterator[InternalRow] =
    new Iterator[InternalRow] {
      private var i = from
      override def hasNext: Boolean = i < until
      override def next(): InternalRow = {
        val row = rows(i)
        i += 1
        row
      }
    }

  /**
   * Turn a projected `(low, high)` row into sweep events. A null bound emits none.
   *
   * An inverted interval (`low > high`) is normalized to `(high, low)` rather than dropped.
   * The join condition still accepts a row the predicate accepts, so dropping would lose it:
   * with build `(3, 1)` and stream `(0, 5)`, `a.lo < b.hi AND b.lo < a.hi` holds and the row
   * has to be a candidate. Normalizing only widens the window. `start <= end` also keeps the
   * sweep's invariant, so `IntervalIndex` never sees a reversed pair.
   */
  def toRangeEvents(
      buildSideKeyGetters: List[InternalRow => Any],
      lowHighExtr: Projection,
      cmp: Ordering[Any]): (InternalRow, Int) => Seq[RangeEvent] = {
    (row: InternalRow, index: Int) => {
      val lowHigh: InternalRow = lowHighExtr(row)
      val low = buildSideKeyGetters(0)(lowHigh)
      val high = buildSideKeyGetters(1)(lowHigh)
      if (low != null && high != null) {
        val result = cmp.compare(low, high)
        if (result == 0) {
          RangeEvent(low, RangeEvent.Point, row, index) :: Nil
        } else {
          val (start, end) = if (result < 0) (low, high) else (high, low)
          RangeEvent(start, RangeEvent.Start, row, index) ::
            RangeEvent(end, RangeEvent.End, row, index) :: Nil
        }
      } else {
        Nil
      }
    }
  }
}

/**
 * Sweep-line index of build-side intervals. `overlapping` returns every row whose
 * interval may overlap `[low, high]`. The window is a superset of that predicate,
 * and the caller drops false positives. `first` is the last key strictly below
 * `low`. When `first <= last`, rows still open at `first` are candidates.
 * When `first < last`, a Start at `low` is also a candidate. `last` is the
 * last key at or below `high`.
 *
 * What is broadcast is the sweep: sorted keys, rows in activation order, and one
 * event per bound. Snapshots are built on the first probe and are not serialized,
 * because Java serialization would expand them to O(n^2). [[sizeInBytes]] also
 * charges the paths those snapshots retain on the executor.
 */
private[execution] object IntervalIndex {

  /** Bytes charged per sweep event by [[IntervalIndex.sizeInBytes]]. */
  private[joins] val sweepEventBytes = 8L

  /** Build an interval index from unsorted events. */
  def build(ordering: Ordering[Any], events: Array[RangeIndex.RangeEvent]): IntervalIndex = {
    val eventOrdering =
      Ordering.by[RangeIndex.RangeEvent, Any](_.key)(ordering).orElse(Ordering.by(_.flow))
    java.util.Arrays.sort(events, eventOrdering)
    buildFromSorted(ordering, events)
  }

  private def buildFromSorted(
      ordering: Ordering[Any],
      events: Array[RangeIndex.RangeEvent]): IntervalIndex = {
    // A leading null key makes the binary search uniform, including an empty index.
    // `offsets(i)` and `eventOffsets(i)` start key i; the last slot is one past
    // the end, so key i owns `[a(i), a(i + 1))`. The null key's range is empty.
    val n = events.length
    val keys = mutable.ArrayBuffer[Any](null)
    val offsets = mutable.ArrayBuffer[Int](0)
    val eventOffsets = mutable.ArrayBuffer[Int](0)
    val activatedRows = mutable.ArrayBuffer.empty[InternalRow]
    val eventFlows = new Array[Byte](n)
    val eventIds = new Array[Int](n)

    var currentKey: Any = null
    var i = 0
    while (i < n) {
      val event = events(i)
      // Group by the index ordering. Array equality is identity, and
      // UTF8String equality is binary, so `!=` would split one sweep key.
      if (currentKey == null || ordering.compare(currentKey, event.key) != 0) {
        currentKey = event.key
        keys += event.key
        offsets += activatedRows.size
        eventOffsets += i
      }
      eventFlows(i) = event.flow.toByte
      eventIds(i) = event.index
      // Start and Point join the activation list. End is applied later, when
      // the snapshots remove that id.
      if (event.flow >= RangeIndex.RangeEvent.Point) {
        activatedRows += event.row
      }
      i += 1
    }
    offsets += activatedRows.size
    eventOffsets += n

    new IntervalIndex(ordering, keys.toArray, offsets.toArray, activatedRows.toArray,
      eventOffsets.toArray, eventFlows, eventIds)
  }
}

private[execution] class IntervalIndex(
    private[this] val ordering: Ordering[Any],
    private[this] val keys: Array[Any],
    private[this] val offsets: Array[Int],
    private[this] val activated: Array[InternalRow],
    private[this] val eventOffsets: Array[Int],
    private[this] val eventFlows: Array[Byte],
    private[this] val eventIds: Array[Int])
  extends RangeRelation {

  /**
   * Rows and the sweep that are broadcast, plus 32 bytes for each IntMap node
   * rebuilt on the executor. After each Start or End, that event copies
   * `ceil(log2(max(active set, 2)))` nodes, at least one, using the active-set
   * size after the event. Flow is the change: Start 1, Point 0, End -1.
   * A Point does not copy a node.
   */
  override def sizeInBytes(): Long = {
    def pathNodes(open: Int): Int =
      Integer.SIZE - Integer.numberOfLeadingZeros(math.max(open, 2) - 1)

    var active = 0
    var nodes = 0L
    var i = 0
    while (i < eventFlows.length) {
      val flow = eventFlows(i)
      active += flow
      if (flow != RangeIndex.RangeEvent.Point) nodes += pathNodes(active)
      i += 1
    }
    RangeIndex.rowsSize(activated) +
      eventIds.length * IntervalIndex.sweepEventBytes + nodes * 32
  }

  /** Built on first probe. Transient, so Java serialization does not flatten the maps. */
  @transient private lazy val snapshots = buildSnapshots()

  /**
   * Per key: the set after this key's Ends, then the set after its Starts.
   * Ends sort before Point and Start. A point never enters the map.
   */
  private def buildSnapshots() = {
    val n = keys.length
    val beforeStarts = new Array[IntMap[InternalRow]](n)
    val activeAll = new Array[IntMap[InternalRow]](n)
    var active = IntMap.empty[InternalRow]
    var activatedAt = 0
    var key = 0
    while (key < n) {
      val until = eventOffsets(key + 1)
      var e = eventOffsets(key)
      while (e < until && eventFlows(e) == RangeIndex.RangeEvent.End) {
        active = active.removed(eventIds(e))
        e += 1
      }
      beforeStarts(key) = active
      while (e < until) {
        val flow = eventFlows(e).toInt
        if (flow == RangeIndex.RangeEvent.Start) {
          active = active.updated(eventIds(e), activated(activatedAt))
        } else if (flow != RangeIndex.RangeEvent.Point) {
          throw new IllegalStateException(s"Unknown range event flow $flow")
        }
        activatedAt += 1
        e += 1
      }
      activeAll(key) = active
      key += 1
    }
    (beforeStarts, activeAll)
  }

  /**
   * The leading null key sorts before every real key. Initialized on the first
   * probe, like [[snapshots]]: this lambda is not `Serializable`, and one instance
   * means [[overlapping]] does not allocate a comparator per probe.
   */
  @transient private lazy val keyComparator: java.util.Comparator[Any] =
    (key: Any, value: Any) => if (key == null) -1 else ordering.compare(key, value)

  /**
   * Rows whose interval may overlap `[low, high]`.
   *
   * `first` is the last key strictly below `low`, so rows active there still
   * include intervals that end at `low`. `last` is the last key at or below
   * `high`. `upperBound` steps past a whole equal run; `lowerBound` stops at
   * its first key, and the slice covers the rest of that run. Three layouts:
   *  - `first < last`: `activeAll(first)` (its Starts are still open going into
   *    `(first, last]`), then rows activated on `(first, last]`.
   *  - `first == last`: only `activeAll(first)`. No sweep key lies in the window.
   *  - `first > last`: `low > high` and a key lies strictly between them.
   *    `beforeStarts(first)` is the set after Ends at that key and before its
   *    Starts, so a row that ends or starts there is excluded.
   */
  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (keys.length == 1 || low == null || high == null) return Iterator.empty

    val first = RangeIndex.lowerBound(keys, low, keyComparator) - 1
    val last = RangeIndex.upperBound(keys, high, keyComparator) - 1
    val (beforeStarts, activeAll) = snapshots
    val carried = (if (first <= last) activeAll(first) else beforeStarts(first)).valuesIterator
    // Activations on (first, last]. A start at `first` is returned only from
    // `activeAll`, which `carried` uses when `first <= last`.
    val from = if (first < last) offsets(first + 1) else 0
    val until = if (first < last) offsets(last + 1) else from
    carried ++ RangeIndex.sliceIterator(activated, from, until)
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
    val keyOrdering = Ordering.by[(Any, InternalRow), Any](_._1)(ordering)
    java.util.Arrays.sort(present, keyOrdering)
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

  override def sizeInBytes(): Long = RangeIndex.rowsSize(rows)

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Any): Iterator[InternalRow] =
    if (value == null) {
      Iterator.empty
    } else {
      RangeIndex.sliceIterator(rows, 0, RangeIndex.upperBound(keys, value, ordering))
    }

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Any): Iterator[InternalRow] =
    if (value == null) {
      Iterator.empty
    } else {
      RangeIndex.sliceIterator(rows, RangeIndex.lowerBound(keys, value, ordering), keys.length)
    }
}
