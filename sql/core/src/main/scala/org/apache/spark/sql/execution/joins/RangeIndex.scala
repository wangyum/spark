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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{
  GreaterPartialRangeJoin, LessPartialRangeJoin, RangeEquality, RangeJoin}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._

/**
 * A [[BroadcastMode]] that materializes the build side into a [[RangeIndex]] and broadcasts it.
 *
 * Symmetric with `HashedRelationBroadcastMode`: the planner declares a
 * `BroadcastDistribution(RangeBroadcastMode(...))` requirement on `BroadcastRangeJoinExec`,
 * and the generic `BroadcastExchangeExec` builds and broadcasts the index via
 * `transform`. This avoids a dedicated exchange node and keeps the range join's integration
 * surface to a single new `BroadcastMode`.
 *
 * `transform` only reads the build-side fields of `rangeInfo` (`buildSide`,
 * `normalizedBuildKeys`, `normalizedBuildPlanOutput`, `rangeJoin`), but the mode carries the
 * *full* `RangeInfo` (including the stream-side keys, the residual condition, the equality
 * flags and `buildSideIsPoint`) rather than a build-only projection. This is deliberate:
 * under AQE, `LogicalQueryStageStrategy` reconstructs `BroadcastRangeJoinExec` from a reused
 * broadcast stage's mode (see its `RangeBroadcastJoinStage` case), so the mode is the
 * persistence boundary for all the exec's plan metadata -- not just what `transform`
 * needs to build the index. Trimming the mode to build-only fields would lose the
 * stream-side metadata that the reconstructed exec still requires.
 *
 * @param rangeInfo the full join metadata persisted for AQE reconstruction and
 *                  used by `transform` to build the range index
 */
private[execution] case class RangeBroadcastMode(
    rangeInfo: RangeInfo)
  extends BroadcastMode with Serializable {

  override def transform(rows: Array[InternalRow]): RangeIndex =
    transform(rows.iterator, Some(rows.length))

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): RangeIndex = {
    val buildSide = rangeInfo.buildSide
    val normalizedBuildKeys = rangeInfo.normalizedBuildKeys
    val normalizedBuildPlanOutput = rangeInfo.normalizedBuildPlanOutput
    val rangeJoin = rangeInfo.rangeJoin
    require(normalizedBuildKeys.length == 2,
      "Range join expects exactly two build keys (low, high).")
    val ordering = PhysicalDataType.ordering(normalizedBuildKeys.head.dataType)
    val keyProjection = new InterpretedProjection(normalizedBuildKeys, normalizedBuildPlanOutput)
    val valueGetters = List(
      RangeIndex.getValue(normalizedBuildKeys.head.dataType, 0),
      RangeIndex.getValue(normalizedBuildKeys(1).dataType, 1))
    val eventifier = RangeIndex.toRangeEvent(valueGetters, keyProjection, ordering)
    val events = rows.zipWithIndex.flatMap {
      case (row: InternalRow, idx: Int) => eventifier(row, idx)
      case _ => Nil
    }.toArray
    RangeIndex.build(ordering, events, rangeJoin, buildSide)
  }

  override def canonicalized: RangeBroadcastMode = copy(rangeInfo = rangeInfo.canonicalized())
}

private[execution] object RangeIndex {

  /**
   * One sweep-line event used to build a [[RangeIndex]].
   *
   * @param key   the bound value this event occurs at
   * @param flow  `1` = interval start, `0` = point, `-1` = interval end
   * @param row   the build-side row that produced this event
   * @param index a unique id for `row` so deactivation is O(1)
   */
  case class RangeEvent(key: Any, flow: Int, row: InternalRow, index: Int)

  /**
   * True when the build side is the low bound of a partial-range join (the stream
   * side is the high bound). Shared by `RangeIndex.intersect` (build-side, on the
   * broadcast index) and `BroadcastRangeJoinExec` (stream-side, from `RangeInfo`) so
   * the (rangeJoin, buildSide) -> direction mapping lives in one place.
   */
  def lowRangeOfPartialRangeJoin(rangeJoin: RangeJoin, buildSide: BuildSide): Boolean =
    (rangeJoin == LessPartialRangeJoin && buildSide == BuildLeft) ||
      (rangeJoin == GreaterPartialRangeJoin && buildSide == BuildRight)

  /**
   * True when the build side is the high bound of a partial-range join (the stream
   * side is the low bound).
   */
  def highRangeOfPartialRangeJoin(rangeJoin: RangeJoin, buildSide: BuildSide): Boolean =
    (rangeJoin == LessPartialRangeJoin && buildSide == BuildRight) ||
      (rangeJoin == GreaterPartialRangeJoin && buildSide == BuildLeft)

  /**
   * A type-specialized accessor that reads a single field from an `InternalRow` as a boxed
   * value (or `null` if the field is null). Shared by the exchange (build-side key extraction)
   * and the join (stream-side key extraction) so the dispatch table lives in one place.
   */
  def getValue(dt: DataType, ordinal: Int): InternalRow => Any = {
    // Only orderable atomic types that range predicates can compare. Nested and
    // interval types fall through to `InternalRow.get`; ExtractRangeJoinKeys will
    // not produce those as range keys.
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
      case StringType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getUTF8String(ordinal)
      case BinaryType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getBinary(ordinal)
      case t: DecimalType => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.getDecimal(ordinal, t.precision, t.scale)
      case u: UserDefinedType[_] => getValue(u.sqlType, ordinal)
      case _ => (input: InternalRow) =>
        if (input.isNullAt(ordinal)) null else input.get(ordinal, dt)
    }
  }

  /** Build a range index from an array of unsorted events. */
  def build(
      ordering: Ordering[Any],
      events: Array[RangeEvent],
      rangeJoin: RangeJoin,
      buildSide: BuildSide): RangeIndex = {
    // Sort the events in place by (key, flow). This avoids the array copy and the
    // per-comparison key-tuple allocation that `events.sortBy(e => (e.key, e.flow))`
    // would incur. The keys are `Any` (already boxed), so the comparison still goes
    // through `ordering.compare`, but the sort itself allocates no extra array.
    val eventComparator = new java.util.Comparator[RangeEvent] {
      override def compare(a: RangeEvent, b: RangeEvent): Int = {
        val keyCmp = ordering.compare(a.key, b.key)
        if (keyCmp != 0) keyCmp else Integer.compare(a.flow, b.flow)
      }
    }
    java.util.Arrays.sort(events, eventComparator)
    buildFromSorted(ordering, events, rangeJoin, buildSide)
  }

  /** Build a range index from an array of sorted events. */
  def buildFromSorted(
      ordering: Ordering[Any],
      events: Array[RangeEvent],
      rangeJoin: RangeJoin,
      buildSide: BuildSide): RangeIndex = {
    // Persisted index components. A dummy null value is added to the array. This makes searching
    // easier and it allows us to deal gracefully with unbound keys.
    val empty = Array.empty[(InternalRow, Int)]
    val keys = mutable.Buffer[Any](null)
    val offsets = mutable.Buffer[Int](0)
    val activatedRows = mutable.Buffer.empty[(InternalRow, Int)]
    val activeNewOffsets = mutable.Buffer.empty[Int]
    val activeRows = mutable.Buffer.empty[Array[(InternalRow, Int)]]

    // Current State of the iteration. Keyed by each row's unique index (assigned when the
    // build-side rows were turned into range events) so that a row can be deactivated in O(1)
    // instead of the O(n) linear scan a Buffer removal would require -- important since heavily
    // overlapping build-side intervals can otherwise degrade index construction to O(n^2).
    var currentKey: Any = null
    var currentActiveNewOffset: Int = -1
    val currentActiveRows = mutable.LinkedHashMap.empty[Int, (InternalRow, Int)]

    // Store the currently active rows.
    def writeActiveRows(): Unit = {
      activeNewOffsets += currentActiveNewOffset
      if (currentActiveRows.isEmpty) activeRows += empty
      else activeRows += currentActiveRows.values.toArray
    }

    events.foreach { event =>
      // Check if we have finished processing a key
      if (currentKey != event.key) {
        writeActiveRows()
        currentKey = event.key
        currentActiveNewOffset = -1
        keys += event.key
        offsets += activatedRows.size
      }

      // Store the offset at which we are starting to add rows to the 'active' buffer.
      if (event.flow >= 0 && currentActiveNewOffset == -1) {
        currentActiveNewOffset = currentActiveRows.size
      }

      // Keep track of rows. The stored pair is (row, unique index) so later
      // deactivation is an O(1) map remove by `event.index`.
      val indexedRow = (event.row, event.index)
      event.flow match {
        case 1 =>
          activatedRows += indexedRow
          currentActiveRows += event.index -> indexedRow
        case 0 =>
          activatedRows += indexedRow
        case -1 =>
          currentActiveRows -= event.index
      }
    }

    // Store the final array of activate rows.
    writeActiveRows()

    // Create the index.
    new RangeIndex(ordering, keys.toArray, offsets.toArray, activeNewOffsets.toArray,
      activeRows.toArray, activatedRows.toArray, rangeJoin, buildSide)
  }

  /** Create a function that turns a row into its respective range events. */
  def toRangeEvent(
      buildSideKeyValueGetter: List[InternalRow => Any],
      lowHighExtr: Projection,
      cmp: Ordering[Any]): (InternalRow, Int) => Seq[RangeEvent] = {
    (row: InternalRow, index: Int) => {
      val lowHigh: InternalRow = lowHighExtr(row)
      val low = buildSideKeyValueGetter(0)(lowHigh)
      val high = buildSideKeyValueGetter(1)(lowHigh)
      // Valid points and intervals.
      if (low != null && high != null) {
        val result = cmp.compare(low, high)
        // Point
        if (result == 0) {
          RangeEvent(low, 0, row, index) :: Nil
        }
        // Interval
        else if (result < 0) {
          RangeEvent(low, 1, row, index) :: RangeEvent(high, -1, row, index) :: Nil
        }
        // Reversed Interval (low > high) - Cannot join on this record.
        else {
          Nil
        }
      }
      // Nulls
      else Nil
    }
  }
}

/**
 * A range index is a data structure that can be used to efficiently execute range queries
 * against. A range query has a lower and an upper bound, and the result is an iterator of rows
 * that match the given constraints.
 *
 * @param ordering         used for sorting keys, comparing keys and values, and
 *                         retrieving the rows in a given interval.
 * @param keys             the points at which the set of active/activated rows changes. A key
 *                         marks an event in the composition of the active or activated rows.
 * @param offsets          for each key, the start index into `activated` of the rows activated
 *                         at (or spanning) that key.
 * @param activeNewOffsets for each key, the index into `active` at which the rows that are
 *                         newly active (not carried over from the previous key) begin.
 * @param active           contains the rows that are 'active' between the current key and
 *                         the next key. This is only used for rows that span an interval.
 * @param activated        contains the rows that have been activated at the current key.
 *                         This contains both rows that span an interval and rows that only
 *                         exist at one point.
 * @param rangeJoin        the range-join shape this index was built for (point-in-range or
 *                         partial range); drives the bound direction in `intersect`.
 * @param buildSide        which side was broadcast (built into this index); together with
 *                         `rangeJoin` determines the comparison direction in `intersect`.
 */
private[execution] class RangeIndex(
    private[this] val ordering: Ordering[Any],
    private[this] val keys: Array[Any],
    private[this] val offsets: Array[Int],
    private[this] val activeNewOffsets: Array[Int],
    private[this] val active: Array[Array[(InternalRow, Int)]],
    private[this] val activated: Array[(InternalRow, Int)],
    private[this] val rangeJoin: RangeJoin,
    private[this] val buildSide: BuildSide)
  extends Serializable {

  def sizeInBytes(): Long = {
    // Sum the in-memory size of every activated row. Rows produced by
    // `executeCollectIterator` are normally `UnsafeRow`s, but guard the cast so
    // a non-unsafe row (e.g. from a test fixture) degrades gracefully instead of
    // throwing `ClassCastException` at broadcast time.
    activated.map { case (row, _) =>
      row match {
        case u: UnsafeRow => u.getSizeInBytes.toLong
        case _ => row.numFields * 8L
      }
    }.sum
  }

  private[this] val maxKeyIndex = keys.length - 1

  /**
   * Find the index of the closest key lower than or equal to the value given.
   *
   * This method is tail recursive.
   *
   * @param value     to find the closest lower or equal key index for.
   * @param inclusive when true, a value equal to a key returns that key's index; when false,
   *                 it returns the index just below the matching key (so the active rows at
   *                 that index include rows deactivated at the matching key).
   * @param first     index (inclusive) to start searching at.
   * @param last      index (inclusive) to stop searching at.
   * @return the index of the closest upper bound.
   */
  @tailrec
  final def closestLowerKey(
      value: Any,
      inclusive: Boolean,
      first: Int = 0,
      last: Int = maxKeyIndex): Int = {
    // Determine the mid point.
    val mid = first + ((last - first + 1) >>> 1)

    // Compare the value with the key at the mid point.
    // Note that a value is always larger than NULL.
    val key = keys(mid)
    val cmp = if (key == null) 1
    else ordering.compare(value, key)

    // Value == Key. Keys are unique so we can stop. When the search is exclusive, step
    // below the matching key; otherwise return the key's own index.
    if (cmp == 0) if (inclusive) mid else mid - 1
    // No more elements left to search.
    else if (first == last) mid
    // Value > Key: Search the top half of the key array.
    else if (cmp > 0) closestLowerKey(value, inclusive, mid, last)
    // Value < Key: Search the lower half of the array.
    else closestLowerKey(value, inclusive, first, mid - 1)
  }

  /**
   * Calculate the intersection between the index and a given range.
   *
   * @param low  point of the range. Note that a NULL value is currently interpreted as an unbound
   *             (negative infinite) value.
   * @param high point of the range to intersect with. Note that a NULL value is currently
   *             interpreted as an unbound (infinite) value.
   * @return an iterator containing all the rows that fall within the given range.
   */
  final def intersect(low: Any, high: Any): Iterator[(InternalRow, Int)] = {
    // An index built from an empty build side has a single dummy key (null) and no
    // activated rows, so nothing can match. Short-circuit before indexing into the
    // (length-1) offsets array.
    if (keys.length == 1) return Iterator.empty

    // Find first index by searching for the last key lower than the low value.
    // Always search exclusively (inclusive=false) so that `first` lands below the
    // matching key, and the active (spanning) rows at `first` include rows
    // deactivated at the matching key (whose `hi == low`). The `rangeCheck`
    // post-filter in the caller rejects rows that don't actually match (e.g. rows
    // whose `lo == low` when the low bound is exclusive).
    val first = if (low == null || RangeIndex.lowRangeOfPartialRangeJoin(rangeJoin, buildSide)) 0
    else closestLowerKey(low, inclusive = false)

    // Find last index for the activated-rows scan. Search inclusively (inclusive=true)
    // so that rows whose `lo == high` are returned as candidates. This returns a
    // SUPERSET of the correct answer; the caller is responsible for applying the
    // full join condition (including the range predicates) to filter out false
    // positives such as rows whose `hi` falls short of the query high when the
    // high bound is exclusive.
    val last = if (high == null || first == maxKeyIndex ||
        RangeIndex.highRangeOfPartialRangeJoin(rangeJoin, buildSide)) {
      maxKeyIndex
    }
    else closestLowerKey(high, inclusive = true, first)

    // The iterator below handles three cases for the relationship between `first`
    // and `last` (the inclusive key-range that brackets the query [low, high]):
    //   - first < last: emit the active rows at `first` (all of them, since they
    //     span keys(first) and thus cover low), then the activated (point) rows
    //     in the key range (first, last].
    //   - first == last: `low` did not land exactly on a key, so there are no
    //     point-rows to emit; only the active (spanning) rows at `first` apply.
    //   - first > last: the query range falls in a gap between two keys. Only the
    //     "old" active rows at `first` (those carried over from before keys(first))
    //     span the gap; `activeNewOffsets(first)` marks where the new (non-spanning)
    //     rows begin, so we iterate only up to that offset.
    new Iterator[(InternalRow, Int)] {
      var activatedAvailable = first < last
      var rowIndex = 0
      var rows: Array[(InternalRow, Int)] = active(first)
      var rowLength = if (first <= last || activeNewOffsets(first) < 0) rows.length
      else activeNewOffsets(first)

      override final def hasNext: Boolean = {
        var result = rowIndex < rowLength
        if (!result && activatedAvailable) {
          activatedAvailable = false
          rows = activated
          rowIndex = offsets(first + 1)
          rowLength = if (last == maxKeyIndex) activated.length
          else offsets(last + 1)
          result = rowIndex < rowLength
        }
        result
      }

      override final def next(): (InternalRow, Int) = {
        val row = rows(rowIndex)
        rowIndex += 1
        row
      }
    }
  }

  /**
   * Create a textual representation of the index for debugging purposes.
   *
   * @param maxKeys maximum number of keys shows in the string.
   * @return a textual representation of the index for debugging purposes.
   */
  def toDebugString(maxKeys: Int = Int.MaxValue): String = {
    val builder = new StringBuilder
    builder.append(s"Index[rangeJoin = $rangeJoin, buildSide = $buildSide]")
    val keysShown = math.min(keys.length, maxKeys)
    val keysLeft = keys.length - keysShown
    for (i <- 0 until keysShown) {
      builder.append("\n  +[")
      builder.append(keys(i))
      builder.append("]@")
      builder.append(offsets(i))
      builder.append("\n  | Active: ")
      builder.append(active(i).mkString(","))
      builder.append("\n  | Activated: ")
      val nextOffset = if (i == maxKeyIndex) activated.length
      else offsets(i + 1)
      builder.append(activated.slice(offsets(i), nextOffset).mkString(","))
    }
    if (keysLeft > 0) {
      builder.append("\n  (")
      builder.append(keysLeft)
      builder.append(" keys left)")
    }
    builder.toString
  }

  override def toString: String = toDebugString(10)
}

/**
 * The plan-time metadata a [[BroadcastRangeJoinExec]] needs to build and probe its range
 * index: the build/streamed keys and their (normalized) input schemas, the residual join
 * condition, the bound inclusivity, the range-join shape, and whether the build side is a
 * point (both build keys are the same expression). All expressions are normalized so the
 * `RangeInfo` is structurally canonicalizable for plan equality/reuse. Construct via the
 * `RangeInfo.build` helper rather than the positional constructor.
 */
private[execution] case class RangeInfo(
    buildSide: BuildSide,
    normalizedRestCondition: Option[Expression],
    normalizedAllOutput: Seq[Attribute],
    normalizedBuildKeys: Seq[Expression],
    normalizedBuildPlanOutput: Seq[Attribute],
    normalizedStreamedKeys: Seq[Expression],
    normalizedStreamedPlanOutput: Seq[Attribute],
    equality: RangeEquality,
    rangeJoin: RangeJoin,
    buildSideIsPoint: Boolean) {
  def canonicalized(): RangeInfo = RangeInfo(
    buildSide,
    normalizedRestCondition.map(_.canonicalized),
    normalizedAllOutput.map(_.canonicalized.asInstanceOf[Attribute]),
    normalizedBuildKeys.map(_.canonicalized),
    normalizedBuildPlanOutput.map(_.canonicalized.asInstanceOf[Attribute]),
    normalizedStreamedKeys.map(_.canonicalized),
    normalizedStreamedPlanOutput.map(_.canonicalized.asInstanceOf[Attribute]),
    equality,
    rangeJoin,
    buildSideIsPoint)
}

private[execution] object RangeInfo {
  /**
   * Build a [[RangeInfo]] from the raw (un-normalized) join inputs, performing the
   * key/output normalization that the runtime exec and the broadcast mode both rely on.
   * Centralizing it here keeps `SparkStrategies` free of range-join-specific normalization
   * and gives the tests a single, named constructor instead of a 10-field positional call.
   *
   * @param left            the join's left child
   * @param right           the join's right child
   * @param buildSide        which child is broadcast (built into the range index)
   * @param leftRangeKeys    the left-side range keys from `ExtractRangeJoinKeys`
   * @param rightRangeKeys   the right-side range keys from `ExtractRangeJoinKeys`
   * @param equality         inclusivity of the two bounds
   * @param restCondition    the residual (non-range) join condition, if any
   * @param rangeJoin        the range-join shape (point-in-range / partial)
   */
  def build(
      left: LogicalPlan,
      right: LogicalPlan,
      buildSide: BuildSide,
      leftRangeKeys: Seq[Expression],
      rightRangeKeys: Seq[Expression],
      equality: RangeEquality,
      restCondition: Option[Expression],
      rangeJoin: RangeJoin): RangeInfo = {
    val (buildPlan, streamedPlan) = buildSide match {
      case BuildLeft => (left, right)
      case BuildRight => (right, left)
    }
    val (buildKeys, streamedKeys) = buildSide match {
      case BuildLeft => (leftRangeKeys, rightRangeKeys)
      case BuildRight => (rightRangeKeys, leftRangeKeys)
    }
    val normalizedBuildKeys =
      buildKeys.flatMap(e => QueryPlan.normalizePredicates(e :: Nil, buildPlan.output))
    val normalizedBuildPlanOutput =
      buildPlan.output.map(QueryPlan.normalizeExpressions(_, buildPlan.output))
    val normalizedStreamedKeys =
      streamedKeys.flatMap(e => QueryPlan.normalizePredicates(e :: Nil, streamedPlan.output))
    val normalizedStreamedPlanOutput =
      streamedPlan.output.map(QueryPlan.normalizeExpressions(_, streamedPlan.output))
    val allOutput = left.output ++ right.output
    val normalizedRestCondition =
      restCondition.map(QueryPlan.normalizeExpressions(_, allOutput))
    val normalizedAllOutput = allOutput.map(QueryPlan.normalizeExpressions(_, allOutput))
    RangeInfo(
      buildSide,
      normalizedRestCondition,
      normalizedAllOutput,
      normalizedBuildKeys,
      normalizedBuildPlanOutput,
      normalizedStreamedKeys,
      normalizedStreamedPlanOutput,
      equality,
      rangeJoin,
      normalizedBuildKeys(0).semanticEquals(normalizedBuildKeys(1)))
  }
}
