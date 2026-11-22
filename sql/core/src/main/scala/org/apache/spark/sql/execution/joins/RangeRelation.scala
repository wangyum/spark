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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildSide}
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, PartialRangeJoin, PointInRangeJoin, RangeJoinKind}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator, Utils}
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.DoublePrefixComparator

/**
 * Broadcast payload of a range join. An index only names candidate rows.
 * [[BroadcastRangeJoinExec]] evaluates the original join condition to filter
 * candidates. Keys that map to one order-preserving long are stored raw and
 * compared unboxed ([[LongIntervalIndex]], [[LongPointIndex]]); every other
 * supported type is compared as boxed values ([[IntervalIndex]],
 * [[PointIndex]]). [[RangeRelationBroadcastMode]] picks the family from the
 * build keys' data types.
 *
 *  - The interval indexes answer "which ranges overlap this window?"
 *  - The point indexes answer "which points lie on this side of a bound?"
 */
private[execution] sealed trait RangeRelation extends KnownSizeEstimation with Serializable {
  def keyType: DataType
}

/**
 * Which [[RangeRelation]] a broadcast materializes.
 *
 * A point index has no direction, so one broadcast of a column serves both
 * `<` and `>`. The join records which side holds the lower bound and scans
 * accordingly.
 */
private[execution] sealed abstract class RangeIndexKind {
  def matches(rangeJoinKind: RangeJoinKind): Boolean = RangeIndexKind(rangeJoinKind) == this
}

private[execution] case object IntervalIndexKind extends RangeIndexKind

private[execution] case object PointIndexKind extends RangeIndexKind

private[execution] object RangeIndexKind {
  def apply(rangeJoinKind: RangeJoinKind): RangeIndexKind = rangeJoinKind match {
    case PointInRangeJoin | IntervalOverlapJoin => IntervalIndexKind
    case _: PartialRangeJoin => PointIndexKind
  }
}

/**
 * The boxed-key index family's shared ordering cache, derived from `keyType`
 * on first comparison and kept out of the serialized form, so each executor
 * re-derives its own instance instead of receiving one per broadcast. The
 * long index family compares raw keys and has no ordering to cache.
 */
private[joins] sealed trait BoxedKeyOrdering extends RangeRelation {
  @transient
  @volatile private[this] var _ordering: Ordering[Any] = _

  private[joins] def ordering: Ordering[Any] = {
    var ord = _ordering
    if (ord == null) {
      ord = PhysicalDataType.ordering(keyType)
      _ordering = ord
    }
    ord
  }
}

/**
 * The long index family's shared key conversion, derived from `keyType` on
 * first probe and kept out of the serialized form, so each executor derives
 * its own converter after deserialization. The interpreted probe path calls
 * [[toLong]] once per streamed key; generated probe code keeps its own
 * conversion, emitted at plan time.
 */
private[joins] sealed trait LongKeyConversion extends RangeRelation {
  @transient
  @volatile private[this] var _toLong: Any => Long = _

  /**
   * One evaluated streamed key to its order-preserving long. Within a predicate, the
   * analyzer's comparison type coercion gives both sides one type; across the two
   * predicates an interval-overlap join pairs, `ExtractRangeJoinKeys`' own same-type
   * guards keep the streamed key on the build side's type, so the streamed key type
   * is this index's `keyType`.
   */
  private[joins] def toLong(key: Any): Long = {
    var f = _toLong
    if (f == null) {
      f = RangeIndex.requiredLongKey(keyType).value
      _toLong = f
    }
    f(key)
  }
}

private[joins] object RangeIndex {

  /**
   * Capacity for a build buffer from the driver's row-count hint, capped at 2^20 for any
   * larger hint (well under a hint that large) and equal to the hint itself below that.
   * `BroadcastExchangeExec` only checks the row count against its row-count
   * limit (up to half a billion for this mode) before calling `transform`, and only
   * checks the built relation's byte size afterward, so sizing a buffer directly off an
   * inflated hint can allocate hundreds of megabytes before a single row is read - ahead
   * of the size check that is supposed to catch an over-large broadcast. Starting small
   * and letting the buffer grow from there bounds only the initial allocation; the final
   * size still tracks the actual row count, which is what the post-build size check sees.
   */
  def initialCapacity(sizeHint: Option[Long]): Int =
    sizeHint.map(n => math.min(n, 1L << 20).toInt).getOrElse(64)

  /**
   * Interval builds at or below this size answer an overlap probe with a
   * linear scan over the stored rows instead of walking the tree. Answers
   * stay exact; tiny dimension tables otherwise spend more on walk setup
   * than a nested loop pass over the same handful of rows.
   *
   * Independent of `RangeGroupProbe.Spec.threshold` (also 16 by default): that one gates
   * whether `RangeGroupProbe` builds a per-equi-key-group index for sort-merge join at all,
   * while this one is an internal fast path inside an already-built `overlapping()` call, used
   * by both that path and `BroadcastRangeJoinExec` (whose build side has no row-count gate of
   * its own, so a tiny broadcast table routinely lands here). The two share a default value by
   * coincidence, not because they are the same knob.
   */
  val SMALL_BUILD_OVERLAP_ROWS: Int = 16

  /**
   * One key type's bridge to an order-preserving long: converting an evaluated
   * key value (from either the build or the streamed side), and turning
   * generated Java for a typed key into generated Java for a long. The two
   * agree, so a build key and a probe key of the same type become the same
   * long.
   */
  final case class LongKey(value: Any => Long, code: String => String)

  /**
   * An order-preserving long for a key type that has one, or None when the type
   * must stay on the generic boxed index: variable-length keys (collated string,
   * binary) and decimals wider than `Decimal.MAX_LONG_DIGITS`, whose unscaled
   * value does not fit a long. Floating point maps through
   * `DoublePrefixComparator.computePrefix` XOR `Long.MinValue`, which folds
   * -0.0 into 0.0, canonicalizes NaN, and yields a signed long whose order
   * matches SQL comparison; no long is turned back into a key, and the join
   * condition rechecks every candidate. This is the only place a key type is
   * specialized; the long indexes and the generated probe code both consult it.
   */
  def longKey(dt: DataType): Option[LongKey] = {
    def key(value: Any => Long): LongKey = LongKey(value, v => s"((long) $v)")
    def floatPrefixCode(v: String): String =
      "org.apache.spark.util.collection.unsafe.sort.PrefixComparators" +
        s".DoublePrefixComparator.computePrefix((double) $v) ^ java.lang.Long.MIN_VALUE"
    dt match {
      case _: BooleanType =>
        Some(LongKey(v => if (v.asInstanceOf[Boolean]) 1L else 0L, v => s"($v ? 1L : 0L)"))
      case _: ByteType =>
        Some(key(v => v.asInstanceOf[Byte].toLong))
      case _: ShortType =>
        Some(key(v => v.asInstanceOf[Short].toLong))
      case _: IntegerType | _: DateType =>
        Some(key(v => v.asInstanceOf[Int].toLong))
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        Some(key(v => v.asInstanceOf[Long]))
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        Some(LongKey(v => v.asInstanceOf[Decimal].toUnscaledLong, v => s"$v.toUnscaledLong()"))
      case _: FloatType =>
        Some(LongKey(
          v => DoublePrefixComparator.computePrefix(v.asInstanceOf[Float].toDouble) ^
            Long.MinValue,
          floatPrefixCode))
      case _: DoubleType =>
        Some(LongKey(
          v => DoublePrefixComparator.computePrefix(v.asInstanceOf[Double]) ^ Long.MinValue,
          floatPrefixCode))
      case _ => None
    }
  }

  /**
   * Every key maps to one order-preserving long. [[RangeRelationBroadcastMode]] picks
   * the index family by this test and [[BroadcastRangeJoinExec]] picks its probe
   * code by the same one, so the two cannot drift apart. Within a predicate, the
   * analyzer's comparison type coercion gives both sides one type; across predicates,
   * `ExtractRangeJoinKeys`' own same-type guards keep build and streamed keys aligned,
   * so testing the build keys also settles the streamed keys.
   */
  def canMapKeysToLong(keys: Seq[Expression]): Boolean =
    keys.forall(k => longKey(k.dataType).isDefined)

  /** longKey for a type the caller has routed to the long family. */
  def requiredLongKey(dt: DataType): LongKey = {
    longKey(dt).getOrElse(
      throw new IllegalStateException(s"$dt has no order-preserving long key."))
  }
}

/**
 * Array-backed candidate scan. The index hands rows to the join in array order,
 * without a closure or a tuple hop: the per-pair overhead then matches a bare
 * loop over a broadcast row array. One instance per probe; never serialized.
 */
private[joins] final class RowSlice(
    rows: Array[InternalRow],
    from: Int,
    until: Int) extends Iterator[InternalRow] {
  private var i = from
  def hasNext: Boolean = i < until
  def next(): InternalRow = {
    val r = rows(i)
    i += 1
    r
  }
}

private[joins] object RowSlice {
  /** A shared empty scan: a null probe bound matches nothing. */
  val empty: RowSlice = new RowSlice(new Array[InternalRow](0), 0, 0)
}

// ===== Boxed-key indexes ====================================================
//
// Keys are the boxed values each build key expression evaluates to, compared
// through `BoxedKeyOrdering`. Strings, binary, and decimals wider than 18
// digits stay here: these types have no order-preserving single-long form,
// which is the property that routes a key type to the long indexes below
// (`RangeIndex.longKey`). Floating point does have one (`DoublePrefixComparator`)
// and routes there instead.

/**
 * Augmented interval tree of build-side intervals. Lows and highs sit in
 * parallel arrays sorted by low bound, with a row array in the same order
 * feeding the scan path and `maxHighIdx(mid)` indexing the maximum high
 * bound in the span rooted at mid. Each key and row object is owned once:
 * the tree holds one reference per value plus the single row array. Both
 * Java and Kryo serialization keep these aliases, since the boxed family's
 * key types (string, binary, wide decimal) are reference types Kryo tracks
 * by identity; only a primitive wrapper would deserialize as two objects
 * when shared by `lows` and `highs`, and those types route to the long
 * indexes below instead.
 * Subtrees whose max high is below the probe window, or whose low starts
 * after it, are pruned during search, and a window covering every interval
 * skips the walk entirely.
 */
private[joins] class IntervalIndex private (
    private[this] var _keyType: DataType,
    private[this] var lows: Array[Any],
    private[this] var highs: Array[Any],
    private[this] var maxHighIdx: Array[Int],
    private[this] var rows: Array[InternalRow],
    private[this] var copyRows: Boolean = true)
  extends BoxedKeyOrdering with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null, null, null, true)

  def keyType: DataType = _keyType

  // All-covering window bounds. `lows` is sorted, so the last entry is the
  // largest low: it bounds every low from above, and coreHigh is the minimum
  // high, which bounds every high from below. A window overlaps every stored
  // interval exactly when it holds probeLow <= coreHigh and probeHigh >= coreLow,
  // whether or not the intervals share a common core.
  // Recomputed in `read` too, since `lows`/`highs` are reassigned there.
  private[this] var coreLow: Any = IntervalIndex.coreLow(lows)
  private[this] var coreHigh: Any = IntervalIndex.coreHigh(highs, ordering)

  /**
   * Measured over the retained arrays, not the index object: SizeEstimator
   * would consult `estimatedSize` for a KnownSizeEstimation and recurse.
   * Its identity tracking charges a key shared by `lows` and `highs`, or
   * aliased across indexes, exactly once.
   *
   * When `copyRows = false` (zero-copy indexing), row payloads are referenced
   * directly from the caller without duplication; only the structural arrays
   * and the row reference array container are counted.
   */
  override def estimatedSize: Long = {
    if (copyRows) {
      SizeEstimator.estimate(Array[AnyRef](lows, highs, maxHighIdx, rows))
    } else {
      SizeEstimator.estimate(Array[AnyRef](lows, highs, maxHighIdx)) + (16L + 8L * rows.length)
    }
  }

  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (lows.isEmpty || low == null || high == null) return Iterator.empty
    val ord = ordering
    val (probeLow, probeHigh) = if (ord.lteq(low, high)) (low, high) else (high, low)
    // Tiny builds: a linear scan beats walk setup per probe.
    if (lows.length <= RangeIndex.SMALL_BUILD_OVERLAP_ROWS) {
      return IntervalIndex.linearOverlap(lows, highs, rows, ord, probeLow, probeHigh)
    }
    // A window that covers every interval gets every row the walk would
    // return, without visiting the tree. Returns all rows iff every interval
    // overlaps the probe; the condition still re-checks each candidate.
    if (ord.lteq(probeLow, coreHigh) && ord.gteq(probeHigh, coreLow)) {
      return new RowSlice(rows, 0, rows.length)
    }
    new OverlapIterator(lows, highs, maxHighIdx, rows, ord, probeLow, probeHigh)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = lows.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeObj(lows(i))
      writeObj(highs(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeInt(maxHighIdx(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    lows = new Array[Any](n)
    highs = new Array[Any](n)
    maxHighIdx = new Array[Int](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      lows(i) = readObj()
      highs(i) = readObj()
      i += 1
    }
    i = 0
    while (i < n) {
      maxHighIdx(i) = readInt()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
    coreLow = IntervalIndex.coreLow(lows)
    coreHigh = IntervalIndex.coreHigh(highs, ordering)
  }
}

private[joins] object IntervalIndex {

  // Shared by the primary constructor and `read`, which recomputes these
  // caches after reassigning `lows`/`highs` on deserialization. `lows`/
  // `highs` are null right after the zero-arg constructor runs, before
  // `readExternal`/`read` populate them.
  private[joins] def coreLow(lows: Array[Any]): Any =
    if (lows == null || lows.isEmpty) null else lows(lows.length - 1)

  // `ordering` is by-name: the zero-arg constructor calls this with `highs ==
  // null`, before `keyType` is set, and forcing `ordering` eagerly would
  // throw trying to derive an ordering for a not-yet-known key type.
  private[joins] def coreHigh(highs: Array[Any], ordering: => Ordering[Any]): Any = {
    if (highs == null || highs.isEmpty) {
      null
    } else {
      val ord = ordering
      var min = highs(0)
      var i = 1
      while (i < highs.length) {
        if (ord.lt(highs(i), min)) min = highs(i)
        i += 1
      }
      min
    }
  }

  /** Exact overlap matches via a linear scan; used for tiny builds. */
  private[joins] def linearOverlap(
      lows: Array[Any],
      highs: Array[Any],
      rows: Array[InternalRow],
      ord: Ordering[Any],
      probeLow: Any,
      probeHigh: Any): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var i = 0
      private var nxt: InternalRow = null
      private def advance(): Unit = {
        nxt = null
        while (i < lows.length && nxt == null) {
          if (ord.lteq(lows(i), probeHigh) && ord.gteq(highs(i), probeLow)) {
            nxt = rows(i)
          }
          i += 1
        }
      }
      advance()
      override def hasNext: Boolean = nxt != null
      override def next(): InternalRow = {
        val r = nxt
        advance()
        r
      }
    }
  }

  def apply(
      rows: Iterator[InternalRow],
      buildKeys: Seq[Expression],
      keyType: DataType,
      sizeHint: Option[Long],
      copyRows: Boolean = true): IntervalIndex = {
    val buffer = new ArrayBuffer[(Any, Any, InternalRow)](RangeIndex.initialCapacity(sizeHint))
    val ordering = PhysicalDataType.ordering(keyType)

    if (buildKeys.length == 1) {
      // A point build key is one key used as both ends, so it is copied once
      // and serves as both bounds, matching `LongIntervalIndex.apply`.
      val lowExpr = buildKeys.head
      while (rows.hasNext) {
        val r = rows.next()
        val rawLo = lowExpr.eval(r)
        if (rawLo != null) {
          val key = InternalRow.copyValue(rawLo)
          val rowToStore = if (copyRows) r.copy() else r
          buffer += ((key, key, rowToStore))
        }
      }
    } else {
      val lowExpr = buildKeys.head
      val highExpr = buildKeys(1)
      while (rows.hasNext) {
        val r = rows.next()
        val rawLo = lowExpr.eval(r)
        val rawHi = highExpr.eval(r)
        if (rawLo != null && rawHi != null) {
          val lo = InternalRow.copyValue(rawLo)
          val hi = InternalRow.copyValue(rawHi)
          val (normLo, normHi) = if (ordering.lteq(lo, hi)) (lo, hi) else (hi, lo)
          val rowToStore = if (copyRows) r.copy() else r
          buffer += ((normLo, normHi, rowToStore))
        }
      }
    }

    sortAndBuild(keyType, ordering, buffer.toArray, copyRows)
  }

  private def sortAndBuild(
      keyType: DataType,
      ordering: Ordering[Any],
      valid: Array[(Any, Any, InternalRow)],
      copyRows: Boolean): IntervalIndex = {
    java.util.Arrays.sort(valid, Ordering.by[(Any, Any, InternalRow), Any](_._1)(ordering))
    val length = valid.length
    val lows = new Array[Any](length)
    val highs = new Array[Any](length)
    val rows = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      lows(i) = valid(i)._1
      highs(i) = valid(i)._2
      rows(i) = valid(i)._3
      i += 1
    }

    // `maxHighIdx(mid)` indexes the maximum high bound in the span rooted at
    // mid. Storing an index instead of aliasing the key keeps each key owned,
    // and serialized, exactly once.
    val maxHighIdx = new Array[Int](length)

    def fillMaxHigh(start: Int, end: Int): Int = {
      if (start >= end) return -1
      val mid = (start + end) >>> 1
      var best = mid
      val left = fillMaxHigh(start, mid)
      if (left >= 0 && ordering.gt(highs(left), highs(best))) best = left
      val right = fillMaxHigh(mid + 1, end)
      if (right >= 0 && ordering.gt(highs(right), highs(best))) best = right
      maxHighIdx(mid) = best
      best
    }

    fillMaxHigh(0, length)
    new IntervalIndex(keyType, lows, highs, maxHighIdx, rows, copyRows)
  }
}

private[joins] class OverlapIterator(
    lows: Array[Any],
    highs: Array[Any],
    maxHighIdx: Array[Int],
    rows: Array[InternalRow],
    ord: Ordering[Any],
    probeLow: Any,
    probeHigh: Any) extends Iterator[InternalRow] {

  private[this] val depth = 32 - java.lang.Integer.numberOfLeadingZeros(lows.length)
  private[this] var stackStart = new Array[Int](math.max(8, depth * 3))
  private[this] var stackEnd = new Array[Int](math.max(8, depth * 3))
  private[this] var stackTop = 0
  private[this] var nextRow: InternalRow = null

  push(0, lows.length)

  private[joins] def stackCapacity: Int = stackStart.length
  private[joins] def pendingSpans: Int = stackTop

  private def push(s: Int, e: Int): Unit = {
    if (stackTop >= stackStart.length) {
      stackStart = java.util.Arrays.copyOf(stackStart, stackStart.length * 2)
      stackEnd = java.util.Arrays.copyOf(stackEnd, stackEnd.length * 2)
    }
    stackStart(stackTop) = s
    stackEnd(stackTop) = e
    stackTop += 1
  }

  private def advance(): InternalRow = {
    while (stackTop > 0) {
      stackTop -= 1
      val start = stackStart(stackTop)
      val end = stackEnd(stackTop)
      if (start < end) {
        val mid = (start + end) >>> 1
        if (ord.gteq(highs(maxHighIdx(mid)), probeLow)) {
          if (ord.lteq(lows(mid), probeHigh)) {
            push(mid + 1, end)
          }
          push(mid, -1)
          push(start, mid)
        }
      } else if (end == -1) {
        if (ord.gteq(highs(start), probeLow) && ord.lteq(lows(start), probeHigh)) {
          return rows(start)
        }
      }
    }
    null
  }

  override def hasNext: Boolean = {
    if (nextRow != null) {
      true
    } else {
      nextRow = advance()
      nextRow != null
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException("empty iterator")
    val r = nextRow
    nextRow = null
    r
  }
}

/**
 * Sorted build-side points for single cross-side inequalities (`<`, `<=`, `>`, `>=`).
 * Keys sit beside the rows they name, one owned copy each, in key order.
 */
private[joins] class PointIndex private (
    private[this] var _keyType: DataType,
    private[this] var keys: Array[Any],
    private[this] var rows: Array[InternalRow],
    private[this] var copyRows: Boolean = true)
  extends BoxedKeyOrdering with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null, true)

  def keyType: DataType = _keyType

  private[this] def length: Int = keys.length

  override def estimatedSize: Long = {
    if (copyRows) {
      SizeEstimator.estimate(Array[AnyRef](keys, rows))
    } else {
      SizeEstimator.estimate(keys) + (16L + 8L * rows.length)
    }
  }

  /**
   * Points whose key is below `value`, or at it when `strict` is false. For a
   * strict partial-range predicate the original condition rejects keys equal
   * to the probe value, so the scan can skip them: a build heavy in
   * duplicated keys would only hand the join rows that all fail.
   */
  def upTo(value: Any, strict: Boolean): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(0, searchBound(value, skipEquals = !strict))

  /** Points whose key is above `value`, or at it when `strict` is false. */
  def from(value: Any, strict: Boolean): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(searchBound(value, skipEquals = strict), length)

  private def searchBound(value: Any, skipEquals: Boolean): Int = {
    val ord = ordering
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = ord.compare(keys(mid), value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }

  private def slice(from: Int, until: Int): Iterator[InternalRow] = new RowSlice(rows, from, until)

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = keys.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeObj(keys(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    keys = new Array[Any](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      keys(i) = readObj()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
  }
}

private[joins] object PointIndex {

  def apply(
      rows: Iterator[InternalRow],
      buildKeys: Seq[Expression],
      keyType: DataType,
      sizeHint: Option[Long],
      copyRows: Boolean = true): PointIndex = {
    val buffer = new ArrayBuffer[(Any, InternalRow)](RangeIndex.initialCapacity(sizeHint))
    val keyExpr = buildKeys.head
    val ordering = PhysicalDataType.ordering(keyType)

    while (rows.hasNext) {
      val r = rows.next()
      val rawKey = keyExpr.eval(r)
      if (rawKey != null) {
        val key = InternalRow.copyValue(rawKey)
        val rowToStore = if (copyRows) r.copy() else r
        buffer += ((key, rowToStore))
      }
    }

    sortAndBuild(keyType, ordering, buffer.toArray, copyRows)
  }

  private def sortAndBuild(
      keyType: DataType,
      ordering: Ordering[Any],
      valid: Array[(Any, InternalRow)],
      copyRows: Boolean): PointIndex = {
    java.util.Arrays.sort(valid, Ordering.by[(Any, InternalRow), Any](_._1)(ordering))
    val length = valid.length
    val keys = new Array[Any](length)
    val rows = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      keys(i) = valid(i)._1
      rows(i) = valid(i)._2
      i += 1
    }
    new PointIndex(keyType, keys, rows, copyRows)
  }
}

// ===== Long-specialized indexes for keys that map to one long ==============
//
// Indexes for the key types `RangeIndex.longKey` maps to one order-preserving
// long, stored as raw longs converted at the build boundary. The key comparison
// itself touches no Spark types once built: the tree walk and the binary searches
// compare raw longs, and generated probe code passes long keys without boxing.
// The row payload still does - `rows: Array[InternalRow]` and the serialized
// `_keyType: DataType` are retained alongside the long arrays. The routed
// property is an order-preserving long encoding (`RangeIndex.longKey`), not
// fixed width: floating point routes here through `DoublePrefixComparator`
// and compact decimals as their unscaled value, while variable-length keys
// (string, binary) and decimals too wide for a long stay boxed.
// `RangeRelationBroadcastMode` picks between the two families once, from the
// build keys' data types.

/**
 * Augmented interval tree of build-side intervals, on raw keys. Lows and highs sit in
 * parallel arrays sorted by low bound, with a row array in the same order feeding the
 * scan path and `maxHighs(mid)` holding the maximum high bound in the span rooted at
 * mid - the value itself, unlike [[IntervalIndex.maxHighIdx]], which stores an index
 * into `highs` because boxed values there are shared references worth not duplicating.
 * `highs` here is already an unboxed `Array[Long]`, so storing the max value directly
 * costs 4 bytes/row more than an Int index would (a Long vs. an Int), in exchange for
 * comparing it without the extra `highs(idx)` indirection on every node a probe visits.
 */
private[joins] class LongIntervalIndex private (
    private[this] var _keyType: DataType,
    private[this] var lows: Array[Long],
    private[this] var highs: Array[Long],
    private[this] var maxHighs: Array[Long],
    private[this] var rows: Array[InternalRow],
    private[this] var copyRows: Boolean = true)
  extends LongKeyConversion with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null, null, null, true)

  def keyType: DataType = _keyType

  // All-covering window bounds on raw keys: `lows` is sorted, so the last entry
  // is the largest low, bounding every low from above, and coreHigh is the
  // minimum high, bounding every high from below. A window overlaps every
  // stored interval exactly when it holds probeLow <= coreHigh and
  // probeHigh >= coreLow, whether or not a common core exists.
  // Recomputed in `read` too, since `lows`/`highs` are reassigned there.
  private[this] var coreLow: Long = LongIntervalIndex.coreLow(lows)
  private[this] var coreHigh: Long = LongIntervalIndex.coreHigh(highs)

  override def estimatedSize: Long = {
    if (copyRows) {
      SizeEstimator.estimate(Array[AnyRef](lows, highs, maxHighs, rows))
    } else {
      SizeEstimator.estimate(lows) + SizeEstimator.estimate(highs) +
        SizeEstimator.estimate(maxHighs) + (16L + 8L * rows.length)
    }
  }

  def overlapping(low: Long, high: Long): Iterator[InternalRow] = {
    if (lows.isEmpty) return Iterator.empty
    var probeLow = low
    var probeHigh = high
    if (probeLow > probeHigh) {
      val t = probeLow
      probeLow = probeHigh
      probeHigh = t
    }
    // Tiny builds: a linear scan beats walk setup per probe.
    if (lows.length <= RangeIndex.SMALL_BUILD_OVERLAP_ROWS) {
      return LongIntervalIndex.linearOverlap(lows, highs, rows, probeLow, probeHigh)
    }
    // A window that covers every interval gets every row the walk would
    // return, without visiting the tree. Returns all rows iff every interval
    // overlaps the probe; the condition still re-checks each candidate.
    if (probeLow <= coreHigh && probeHigh >= coreLow) {
      return new RowSlice(rows, 0, rows.length)
    }
    new LongOverlapIterator(lows, highs, rows, maxHighs, probeLow, probeHigh)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, out.writeLong,
      (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt, out.writeLong,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeLong: Long => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = lows.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeLong(lows(i))
      writeLong(highs(i))
      writeLong(maxHighs(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readLong(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => in.readLong(),
      () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readLong: () => Long,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    lows = new Array[Long](n)
    highs = new Array[Long](n)
    maxHighs = new Array[Long](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      lows(i) = readLong()
      highs(i) = readLong()
      maxHighs(i) = readLong()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
    coreLow = LongIntervalIndex.coreLow(lows)
    coreHigh = LongIntervalIndex.coreHigh(highs)
  }
}

private[joins] object LongIntervalIndex {

  // Shared by the primary constructor and `read`, which recomputes these
  // caches after reassigning `lows`/`highs` on deserialization. `lows`/
  // `highs` are null right after the zero-arg constructor runs, before
  // `readExternal`/`read` populate them.
  private[joins] def coreLow(lows: Array[Long]): Long =
    if (lows == null || lows.isEmpty) {
      Long.MinValue
    } else {
      lows(lows.length - 1)
    }

  private[joins] def coreHigh(highs: Array[Long]): Long = {
    if (highs == null || highs.isEmpty) {
      Long.MaxValue
    } else {
      var min = Long.MaxValue
      var i = 0
      while (i < highs.length) {
        if (highs(i) < min) min = highs(i)
        i += 1
      }
      min
    }
  }

  /** Exact overlap matches via a linear scan; used for tiny builds. */
  private[joins] def linearOverlap(
      lows: Array[Long],
      highs: Array[Long],
      rows: Array[InternalRow],
      probeLow: Long,
      probeHigh: Long): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var i = 0
      private var nxt: InternalRow = null
      private def advance(): Unit = {
        nxt = null
        while (i < lows.length && nxt == null) {
          if (lows(i) <= probeHigh && highs(i) >= probeLow) {
            nxt = rows(i)
          }
          i += 1
        }
      }
      advance()
      override def hasNext: Boolean = nxt != null
      override def next(): InternalRow = {
        val r = nxt
        advance()
        r
      }
    }
  }

  /**
   * Sort staging for `apply`: `Tuple3` is never specialized, so staging rows as
   * `(Long, Long, InternalRow)` would box both converted keys beside each
   * tuple. The holder keeps them raw from the build boundary on, and the sort
   * comparator reads the primitive fields without boxing. `Arrays.sort` with a
   * comparator is stable, so rows with equal lows keep input order, matching
   * the tuple staging it replaces.
   */
  private final class Staged(val lo: Long, val hi: Long, val row: InternalRow)

  def apply(
      rows: Iterator[InternalRow],
      buildKeys: Seq[Expression],
      keyType: DataType,
      sizeHint: Option[Long],
      copyRows: Boolean = true): LongIntervalIndex = {
    val toLongs: Seq[Any => Long] = buildKeys.map(k => RangeIndex.requiredLongKey(k.dataType).value)
    val valid = new ArrayBuffer[LongIntervalIndex.Staged](RangeIndex.initialCapacity(sizeHint))
    val lowExpr = buildKeys.head
    val highExpr = if (buildKeys.length == 1) null else buildKeys(1)

    while (rows.hasNext) {
      val r = rows.next()
      val rawLo = lowExpr.eval(r)
      val rawHi = if (buildKeys.length == 1) rawLo else highExpr.eval(r)
      if (rawLo != null && rawHi != null) {
        // A point build key is one key used as both ends, so it is read once.
        var lo = toLongs.head(rawLo)
        var hi = if (buildKeys.length == 1) lo else toLongs(1)(rawHi)
        if (lo > hi) {
          val t = lo
          lo = hi
          hi = t
        }
        val rowToStore = if (copyRows) r.copy() else r
        valid += new LongIntervalIndex.Staged(lo, hi, rowToStore)
      }
    }

    val staged = valid.toArray
    java.util.Arrays.sort(staged, (a: Staged, b: Staged) => java.lang.Long.compare(a.lo, b.lo))
    val length = staged.length
    val lows = new Array[Long](length)
    val highs = new Array[Long](length)
    val rowArr = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      lows(i) = staged(i).lo
      highs(i) = staged(i).hi
      rowArr(i) = staged(i).row
      i += 1
    }

    val maxHighs = new Array[Long](length)

    def fillMaxHigh(start: Int, end: Int): Long = {
      if (start >= end) return Long.MinValue
      val mid = (start + end) >>> 1
      var max = highs(mid)
      val left = fillMaxHigh(start, mid)
      val right = fillMaxHigh(mid + 1, end)
      if (left > max) max = left
      if (right > max) max = right
      maxHighs(mid) = max
      max
    }

    fillMaxHigh(0, length)
    new LongIntervalIndex(keyType, lows, highs, maxHighs, rowArr, copyRows)
  }
}

private[joins] class LongOverlapIterator(
    lows: Array[Long],
    highs: Array[Long],
    rows: Array[InternalRow],
    maxHighs: Array[Long],
    probeLow: Long,
    probeHigh: Long) extends Iterator[InternalRow] {

  private[this] val depth = 32 - java.lang.Integer.numberOfLeadingZeros(lows.length)
  private[this] var stackStart = new Array[Int](math.max(8, depth * 3))
  private[this] var stackEnd = new Array[Int](math.max(8, depth * 3))
  private[this] var stackTop = 0
  private[this] var nextRow: InternalRow = null

  push(0, lows.length)

  private[joins] def stackCapacity: Int = stackStart.length
  private[joins] def pendingSpans: Int = stackTop

  private def push(s: Int, e: Int): Unit = {
    if (stackTop >= stackStart.length) {
      stackStart = java.util.Arrays.copyOf(stackStart, stackStart.length * 2)
      stackEnd = java.util.Arrays.copyOf(stackEnd, stackEnd.length * 2)
    }
    stackStart(stackTop) = s
    stackEnd(stackTop) = e
    stackTop += 1
  }

  private def advance(): InternalRow = {
    while (stackTop > 0) {
      stackTop -= 1
      val start = stackStart(stackTop)
      val end = stackEnd(stackTop)
      if (start < end) {
        val mid = (start + end) >>> 1
        if (maxHighs(mid) >= probeLow) {
          if (lows(mid) <= probeHigh) {
            push(mid + 1, end)
          }
          push(mid, -1)
          push(start, mid)
        }
      } else if (end == -1) {
        if (highs(start) >= probeLow && lows(start) <= probeHigh) {
          return rows(start)
        }
      }
    }
    null
  }

  override def hasNext: Boolean = {
    if (nextRow != null) {
      true
    } else {
      nextRow = advance()
      nextRow != null
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException("empty iterator")
    val r = nextRow
    nextRow = null
    r
  }
}

private[joins] class LongPointIndex private (
    private[this] var _keyType: DataType,
    private[this] var keys: Array[Long],
    private[this] var rows: Array[InternalRow],
    private[this] var copyRows: Boolean = true)
  extends LongKeyConversion with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null, true)

  def keyType: DataType = _keyType

  private[this] def length: Int = keys.length

  override def estimatedSize: Long = {
    if (copyRows) {
      SizeEstimator.estimate(Array[AnyRef](keys, rows))
    } else {
      SizeEstimator.estimate(keys) + (16L + 8L * rows.length)
    }
  }

  /**
   * Points whose key is below `value`, or at it when `strict` is false. For a
   * strict partial-range predicate the original condition rejects keys equal
   * to the probe value, so the scan can skip them; see [[PointIndex.upTo]].
   */
  def upTo(value: Long, strict: Boolean): Iterator[InternalRow] =
    new RowSlice(rows, 0, searchBound(value, skipEquals = !strict))

  /** Points whose key is above `value`, or at it when `strict` is false. */
  def from(value: Long, strict: Boolean): Iterator[InternalRow] =
    new RowSlice(rows, searchBound(value, skipEquals = strict), length)

  // java.util.Arrays.binarySearch returns an arbitrary hit; a bound scan needs
  // the first or last position, so lower and upper bounds are hand-rolled.
  private def searchBound(value: Long, skipEquals: Boolean): Int = {
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = java.lang.Long.compare(keys(mid), value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, out.writeLong,
      (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt, out.writeLong,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeLong: Long => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = keys.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeLong(keys(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readLong(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => in.readLong(),
      () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readLong: () => Long,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    keys = new Array[Long](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      keys(i) = readLong()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
  }
}

private[joins] object LongPointIndex {

  /**
   * Sort staging for `apply`: `Tuple2` is only specialized when both type
   * arguments are primitive, so staging rows as `(Long, InternalRow)` boxes
   * every converted key. The holder keeps it raw from the build boundary on,
   * and the sort comparator reads the primitive field without boxing.
   * `Arrays.sort` with a comparator is stable, so rows with equal keys keep
   * input order, matching the tuple staging it replaces.
   */
  private final class Staged(val key: Long, val row: InternalRow)

  def apply(
      rows: Iterator[InternalRow],
      buildKeys: Seq[Expression],
      keyType: DataType,
      sizeHint: Option[Long],
      copyRows: Boolean = true): LongPointIndex = {
    require(buildKeys.length == 1, "A long point index expects one build key.")
    val toLong = RangeIndex.requiredLongKey(buildKeys.head.dataType).value
    val valid = new ArrayBuffer[LongPointIndex.Staged](RangeIndex.initialCapacity(sizeHint))
    val keyExpr = buildKeys.head

    while (rows.hasNext) {
      val r = rows.next()
      val rawKey = keyExpr.eval(r)
      if (rawKey != null) {
        val rowToStore = if (copyRows) r.copy() else r
        valid += new LongPointIndex.Staged(toLong(rawKey), rowToStore)
      }
    }

    val staged = valid.toArray
    java.util.Arrays.sort(staged, (a: Staged, b: Staged) => java.lang.Long.compare(a.key, b.key))
    val length = staged.length
    val keys = new Array[Long](length)
    val rowArr = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      keys(i) = staged(i).key
      rowArr(i) = staged(i).row
      i += 1
    }
    new LongPointIndex(keyType, keys, rowArr, copyRows)
  }
}

/**
 * How a join probes the [[RangeRelation]] its build side materializes, paired 1:1 with the
 * [[RangeIndexKind]] that relation was built as (`indexKind` projects it out, so a caller
 * holding a `RangeProbeKind` never separately tracks or re-derives which index family it
 * probes). A point-in-range or interval-overlap join always probes with `overlapping`; a
 * partial range (single inequality) probes [[PointIndex]] / [[LongPointIndex]]'s `upTo`
 * (when `isUpTo` is true) or `from` (otherwise) method, with the predicate's strictness
 * traveling beside it as `strict`. Replaces an `Option[(Boolean, Boolean)]` that forced
 * every caller to `.get` it back out inside a [[PointIndexKind]] branch: an interval probe
 * can never reach that branch, but the Option's type didn't say so - this type does, since
 * only [[RangeProbeKind.Point]] carries a scan direction at all.
 */
private[joins] sealed trait RangeProbeKind {
  def indexKind: RangeIndexKind
}

private[joins] object RangeProbeKind {
  case object Interval extends RangeProbeKind {
    override def indexKind: RangeIndexKind = IntervalIndexKind
  }

  /**
   * `isUpTo` selects which of [[PointIndex]] / [[LongPointIndex]]'s two scan methods probes
   * this join's partial-range predicate: true for `upTo` (this side holds the lower bound),
   * false for `from`. Generated probe code can't reference a Scala method value directly, so
   * [[RangeIndexProbeCodegen.genCandidateExpr]] splices the method name into the Java source
   * it emits from this same flag by hand; a mismatch there would only surface as a Janino
   * compile failure at query-run time, not a compile-time error here. RangeJoinSuite's codegen
   * point-scan tests cover the long point-index methods (int keys); RangeJoinSQLSuite's
   * wide-decimal and string partial-range queries are what catch a stale boxed `PointIndex`
   * method name, since DOUBLE keys build a `LongPointIndex` instead.
   */
  final case class Point(isUpTo: Boolean, strict: Boolean) extends RangeProbeKind {
    override def indexKind: RangeIndexKind = PointIndexKind
  }

  def forJoin(kind: RangeJoinKind, buildSide: BuildSide): RangeProbeKind = kind match {
    case partial: PartialRangeJoin =>
      // Lower bound build side scans `upTo`, upper bound scans `from`.
      val isUpTo = partial.leftIsLower == (buildSide == BuildLeft)
      Point(isUpTo, partial.strict)
    case _ => Interval
  }
}

/**
 * A [[BroadcastMode]] that materializes the build side into a [[RangeRelation]].
 *
 * Symmetric with `HashedRelationBroadcastMode`. `buildKeys` are bound to the
 * build row, so the index does not depend on attribute ids. Which join input is
 * broadcast, and the original join condition, live on [[BroadcastRangeJoinExec]].
 * Under AQE, `LogicalQueryStageStrategy` checks that this mode's bound keys are
 * the keys `ExtractRangeJoinKeys` finds.
 *
 * Bound keys are stored with nullability forced on. AQE can rewrite a stage's
 * nullability, and [[org.apache.spark.sql.catalyst.plans.physical.BroadcastPartitioning]]
 * decides reuse with `==`. Forcing it here keeps that check aligned with the
 * strategy, which compares modes the same way. The index already treats a null
 * key as non-matching.
 */
private[execution] case class RangeRelationBroadcastMode private (
    buildKeys: Seq[Expression],
    indexKind: RangeIndexKind)
  extends BroadcastMode with Serializable {

  override def transform(rows: Array[InternalRow]): RangeRelation =
    transform(rows.iterator, Some(rows.length), copyRows = true)

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): RangeRelation = transform(rows, sizeHint, copyRows = true)

  def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long],
      copyRows: Boolean): RangeRelation = {
    val longKeys = RangeIndex.canMapKeysToLong(buildKeys)
    indexKind match {
      case IntervalIndexKind =>
        require(buildKeys.nonEmpty && buildKeys.length <= 2,
          "An interval index expects (low, high), or one point used as both ends.")
        if (longKeys) {
          LongIntervalIndex(rows, buildKeys, buildKeys.head.dataType, sizeHint, copyRows)
        } else {
          IntervalIndex(rows, buildKeys, buildKeys.head.dataType, sizeHint, copyRows)
        }

      case PointIndexKind =>
        require(buildKeys.length == 1, "A point index expects one build key.")
        if (longKeys) {
          LongPointIndex(rows, buildKeys, buildKeys.head.dataType, sizeHint, copyRows)
        } else {
          PointIndex(rows, buildKeys, buildKeys.head.dataType, sizeHint, copyRows)
        }
    }
  }

  override lazy val canonicalized: RangeRelationBroadcastMode =
    RangeRelationBroadcastMode(buildKeys.map(_.canonicalized), indexKind)

  def copy(
      buildKeys: Seq[Expression] = buildKeys,
      indexKind: RangeIndexKind = indexKind): RangeRelationBroadcastMode =
    RangeRelationBroadcastMode(buildKeys, indexKind)
}

private[execution] object RangeRelationBroadcastMode {

  def apply(buildKeys: Seq[Expression], indexKind: RangeIndexKind): RangeRelationBroadcastMode = {
    val normalizedKeys = buildKeys.map { key =>
      key.transform {
        case b: BoundReference => b.copy(nullable = true)
      }
    }
    new RangeRelationBroadcastMode(normalizedKeys, indexKind)
  }
}

/**
 * Shared range-index probe dispatch for broadcast and per-key-group sort-merge joins.
 */
private[joins] object RangeIndexProbe {

  /**
   * Create candidate probe function for a materialized range relation and bound probe keys.
   * `relation`'s concrete class and `probeKind` both derive from the same join's
   * `RangeJoinKind` (see `RangeProbeKind.forJoin` and the `RangeRelationBroadcastMode` that
   * built `relation`), so exactly one of the four cases below ever matches for a given call;
   * the catch-all exists only because `relation`'s concrete type isn't visible to the compiler
   * here; it is not a reachable outcome.
   */
  def makeCandidateFn(
      relation: RangeRelation,
      boundStreamKeys: Seq[Expression],
      probeKind: RangeProbeKind): InternalRow => Iterator[InternalRow] = {
    val isOneKey = boundStreamKeys.length == 1
    val lowExpr = boundStreamKeys.head
    val highExpr = if (isOneKey) null else boundStreamKeys(1)

    (relation, probeKind) match {
      case (longs: LongIntervalIndex, RangeProbeKind.Interval) => row =>
        val low = lowExpr.eval(row)
        if (low == null) {
          Iterator.empty
        } else {
          val high = if (isOneKey) low else highExpr.eval(row)
          if (high == null) {
            Iterator.empty
          } else {
            longs.overlapping(longs.toLong(low), longs.toLong(high))
          }
        }
      case (points: LongPointIndex, RangeProbeKind.Point(isUpTo, strict)) =>
        val scan: Long => Iterator[InternalRow] =
          if (isUpTo) points.upTo(_, strict) else points.from(_, strict)
        row =>
          val low = lowExpr.eval(row)
          if (low == null) Iterator.empty else scan(points.toLong(low))
      case (intervals: IntervalIndex, RangeProbeKind.Interval) => row =>
        val low = lowExpr.eval(row)
        if (low == null) {
          Iterator.empty
        } else {
          val high = if (isOneKey) low else highExpr.eval(row)
          intervals.overlapping(low, high)
        }
      case (points: PointIndex, RangeProbeKind.Point(isUpTo, strict)) =>
        val scan: Any => Iterator[InternalRow] =
          if (isUpTo) points.upTo(_, strict) else points.from(_, strict)
        row =>
          val low = lowExpr.eval(row)
          if (low == null) Iterator.empty else scan(low)
      case (other, kind) =>
        throw SparkException.internalError(
          s"RangeRelation ${other.getClass.getSimpleName} does not match probe kind $kind; " +
          "both must derive from the same join's RangeJoinKind.")
    }
  }
}

/**
 * Shared whole-stage codegen for probing a [[RangeRelation]], used by both
 * [[BroadcastRangeJoinExec]] (one relation per plan) and [[SortMergeJoinExec]]'s per-equi-key-
 * group range index (one relation per group, type fixed for the life of the plan). Both callers
 * know the concrete index class and key family statically at codegen time
 * ([[RangeIndexKind]] plus whether the build keys map to one order-preserving long, see
 * [[RangeIndex.longKey]]), so generated code can cast directly to it and call its scan method,
 * with probe keys evaluated into primitive `long` locals for the long-keyed families and boxed
 * `Object` locals otherwise - mirroring [[RangeIndexProbe.makeCandidateFn]]'s interpreted
 * dispatch, but without a closure or per-row boxing on the long-keyed path.
 */
private[joins] object RangeIndexProbeCodegen {

  /** Generated probe-key evaluation: setup code plus one local variable name per key. */
  final case class ProbeKeys(code: String, keyVars: Seq[String], nullVars: Seq[String])

  /**
   * Evaluate `boundKeys` (already bound to the producing side's output, with `ctx.currentVars`
   * set to `inputVars` matching that output) into generated locals. `usesLongKeys` selects
   * primitive `long` locals, converted via [[RangeIndex.longKey]]'s generated code and null-
   * tracked in `nullVars` (a primitive `long` has no null to carry through), or boxed `Object`
   * locals whose null case is encoded as a Java `null` the index method itself treats as
   * "no candidates" - so `nullVars` is empty for the boxed family.
   */
  def genProbeKeys(
      ctx: CodegenContext,
      inputVars: Seq[ExprCode],
      boundKeys: Seq[Expression],
      usesLongKeys: Boolean): ProbeKeys = {
    ctx.currentVars = inputVars
    if (usesLongKeys) {
      val keys = boundKeys.map { bound =>
        val ev = bound.genCode(ctx)
        val toLong = RangeIndex.requiredLongKey(bound.dataType)
        val name = ctx.freshName("probeKey")
        val code =
          s"""
             |${ev.code}
             |long $name = ${ev.isNull} ? 0L : ${toLong.code(ev.value)};
           """.stripMargin
        (code, name, ev.isNull.toString)
      }
      ProbeKeys(keys.map(_._1).mkString("\n"), keys.map(_._2), keys.map(_._3))
    } else {
      val keys = boundKeys.map { bound =>
        val ev = bound.genCode(ctx)
        val name = ctx.freshName("probeKey")
        val javaType = CodeGenerator.javaType(bound.dataType)
        val boxed = CodeGenerator.boxedType(bound.dataType)
        val valueExpr = if (javaType == boxed) {
          s"${ev.value}"
        } else {
          s"$boxed.valueOf(${ev.value})"
        }
        val code =
          s"""
             |${ev.code}
             |Object $name = ${ev.isNull} ? null : $valueExpr;
           """.stripMargin
        (code, name)
      }
      ProbeKeys(keys.map(_._1).mkString("\n"), keys.map(_._2), Nil)
    }
  }

  /**
   * The concrete [[RangeRelation]] class `indexKind`/`usesLongKeys` selects -- the single
   * source both `genCandidateExpr` (for the cast it splices into generated code) and
   * [[BroadcastRangeJoinExec.prepareBroadcast]] (for its mutable field's declared type)
   * consult, so the two cannot name different classes for the same relation.
   */
  def indexClassName(indexKind: RangeIndexKind, usesLongKeys: Boolean): String = indexKind match {
    case PointIndexKind =>
      (if (usesLongKeys) classOf[LongPointIndex] else classOf[PointIndex]).getName
    case IntervalIndexKind =>
      (if (usesLongKeys) classOf[LongIntervalIndex] else classOf[IntervalIndex]).getName
  }

  /**
   * The Java expression that probes `relationTerm` - already known at codegen time to be an
   * instance of the concrete class `probeKind.indexKind`/`usesLongKeys` selects - and yields a
   * `scala.collection.Iterator`. Returns only the expression, not a declaration: callers decide
   * whether to declare a new variable or assign into one already declared (e.g. inside an
   * `if`/`else` branch). Unlike the interpreted `RangeIndexProbe.makeCandidateFn`, this match
   * is exhaustive with no unreachable case: codegen never inspects `relationTerm`'s actual class,
   * so there is nothing here that could disagree with `probeKind`.
   */
  def genCandidateExpr(
      ctx: CodegenContext,
      relationTerm: String,
      probeKind: RangeProbeKind,
      usesLongKeys: Boolean,
      keys: ProbeKeys): String = {
    val className = indexClassName(probeKind.indexKind, usesLongKeys)
    val call = probeKind match {
      case RangeProbeKind.Point(isUpTo, strict) =>
        val methodName = if (isUpTo) "upTo" else "from"
        s"(($className) $relationTerm).$methodName(${keys.keyVars.head}, $strict)"
      case RangeProbeKind.Interval =>
        // A point probe has one key. Reuse it as both ends so it is evaluated once.
        val high = if (keys.keyVars.length == 1) keys.keyVars.head else keys.keyVars(1)
        s"(($className) $relationTerm).overlapping(${keys.keyVars.head}, $high)"
    }
    if (keys.nullVars.isEmpty) {
      call
    } else {
      val iterCls = classOf[Iterator[InternalRow]].getName
      val emptyScan = s"${classOf[RowSlice].getName}$$.MODULE$$.empty()"
      s"${keys.nullVars.mkString(" || ")} ? ($iterCls) $emptyScan : $call"
    }
  }
}
