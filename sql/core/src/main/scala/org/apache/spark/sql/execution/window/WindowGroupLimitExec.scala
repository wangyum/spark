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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet,
  BindReferences, DenseRank, Expression, Rank, RowNumber, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode,
  GenerateOrdering, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution,
  Distribution, Partitioning}
import org.apache.spark.sql.catalyst.util.SQLOrderingUtil
import org.apache.spark.sql.execution.{BlockingOperatorWithCodegen, CodegenSupport,
  ExternalAppendOnlyUnsafeRowArray, SparkPlan, UnaryExecNode, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal,
  DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, TimestampNTZType,
  TimestampType}
import org.apache.spark.unsafe.Platform

sealed trait WindowGroupLimitMode

case object Partial extends WindowGroupLimitMode

case object Final extends WindowGroupLimitMode

/**
 * Where a [[HashGroupLimitAccumulator]] routes every row once it has permanently fallen back from
 * hash accumulation (see the concrete accumulators' class docs for the possible triggers): an
 * [[UnsafeExternalRowSorter]] under `mode == Partial` (its output still needs per-key filtering
 * before the shuffle), or an [[ExternalAppendOnlyUnsafeRowArray]] under `mode == Final` (output is
 * unfiltered -- see [[HashRowNumberGroupLimitAccumulator.buildFallbackArray]] for why). Unifying
 * both behind one `insertRow` lets every post-fallback call site -- `consumeRow`, `switchTo*`, and
 * `WindowGroupLimitExec.doConsume`'s generated code -- dispatch through a single check regardless
 * of `mode`, instead of maintaining two parallel nullable fields and re-deriving the same
 * `mode == Final` branch at every one of them. Must stay public (not `private[window]`): generated
 * code lives outside this package and calls `insertRow` directly.
 */
sealed trait FallbackDestination {
  def insertRow(row: UnsafeRow): Unit
}

/** [[FallbackDestination]] for `mode == Partial`. */
case class SorterFallbackDestination(sorter: UnsafeExternalRowSorter)
  extends FallbackDestination {
  override def insertRow(row: UnsafeRow): Unit = sorter.insertRow(row)
}

/** [[FallbackDestination]] for `mode == Final`. */
case class ArrayFallbackDestination(array: ExternalAppendOnlyUnsafeRowArray)
  extends FallbackDestination {
  override def insertRow(row: UnsafeRow): Unit = array.add(row)
}

/**
 * Common interface over [[HashRowNumberGroupLimitAccumulator]] and
 * [[HashRankGroupLimitAccumulator]] so [[WindowGroupLimitExec]]'s whole-stage codegen (`doProduce`
 * / `doConsume`) can drive either one through a single generated-code variable, regardless of
 * which `rankLikeFunction` this particular plan node has. Must stay public (not `private[window]`)
 * since generated code lives outside this package.
 */
trait HashGroupLimitAccumulator extends Logging {
  /**
   * Consumes one row, computing `key`/`orderKey` itself via the concrete accumulator's own
   * groupingProjection/orderKeyProjection. Used by the interpreted path
   * ([[HashGroupLimitEvaluatorFactory]]'s shared `createEvaluator()`), which has only a full
   * `row` to work with, no already-materialized column registers to project from -- unlike
   * `doConsume`'s generated code, which calls the four-argument overload below directly (see its
   * doc). Checks [[fallbackDestinationOrNull]] itself, since that check is otherwise the caller's
   * responsibility (the four-argument overload skips it, relying on `doConsume`'s generated code
   * to have already special-cased it).
   */
  def consumeRow(row: UnsafeRow): Unit

  /**
   * Consumes one row, given `key`/`orderKey`/`keyHash` already computed by the caller.
   * `doConsume`'s generated code computes these directly from the row's already-materialized
   * column registers (via `partitionSpec`/`orderSpec`'s expressions bound to `child.output` and
   * run through `GenerateUnsafeProjection`), rather than via this accumulator's own
   * groupingProjection/orderKeyProjection, which would otherwise redundantly re-extract those
   * same columns from `row` after `row` itself was just built from those very registers.
   * The single-argument `consumeRow(row)` above delegates here after computing
   * `key`/`orderKey`/`keyHash` itself. Callers must have already checked
   * [[fallbackDestinationOrNull]] themselves; this skips that check (both `consumeRow(row)` and
   * `doConsume`'s generated code already special-case it).
   */
  def consumeRow(row: UnsafeRow, key: UnsafeRow, keyHash: Int, orderKey: UnsafeRow): Unit

  /**
   * Like the four-argument overload, but for the common case of a single numeric order column:
   * `doConsume`'s generated code (see `isSingleOrderCol`) extracts that column as a primitive
   * `Long` to avoid building an [[UnsafeRow]] wrapper just to pass one value. Deliberately left
   * abstract rather than given a default that re-wraps `orderKeyVal`/`orderKeyIsNull` into an
   * [[UnsafeRow]] and delegates to the four-argument overload: both concrete implementations
   * already need their own allocation-free comparison/storage path for this single-column case,
   * so a "generic" default here would only ever be reached by a future implementation that forgot
   * to write one -- silently reintroducing the per-row allocation this overload exists to avoid,
   * with no compiler signal. Forcing an explicit implementation surfaces that at compile time.
   */
  def consumeRow(
      row: UnsafeRow,
      key: UnsafeRow,
      keyHash: Int,
      orderKeyVal: Long,
      orderKeyIsNull: Boolean): Unit

  def finishAccumulator(): Unit
  def nextResultRow(resultRow: UnsafeRow): Boolean

  def finish(): Iterator[InternalRow] = {
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
   * Non-null once this accumulator has permanently fallen back to [[FallbackDestination]] output
   * (see the concrete implementations' class docs for the possible triggers). Exposed so
   * `doConsume`'s generated code can, once a task enters permanent fallback, insert directly into
   * this destination -- the same direct dispatch [[org.apache.spark.sql.execution.SortExec]]'s own
   * `doConsume` uses for its sorter -- instead of paying `consumeRow`'s extra virtual dispatch
   * (through this trait) plus its own internal null check on every one of the, possibly many
   * million, remaining rows.
   */
  def fallbackDestinationOrNull: FallbackDestination = fallbackDestination

  // Backs fallbackDestinationOrNull; mutated only by switchToFallback/switchToFallbackProactively
  // below. A plain concrete field on the trait (not abstract) -- each mixing-in instance gets its
  // own copy, same as a private var declared directly in a class.
  //
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
  protected var fallbackDestination: FallbackDestination = null

  // --- Shared fallback control flow -----------------------------------------------------------
  // The following members back both concrete accumulators' identical fallback bookkeeping (see
  // switchToFallback/switchToFallbackProactively/maybeBailOutForPoorEfficiency below): homed here
  // for the same reason as compareSlotSingle above -- two files previously carried byte-for-byte
  // identical copies (one literally annotated "Mirrors HashRowNumberGroupLimitAccumulator.X" in
  // its own doc comment), which is exactly the kind of duplication that let the signed-zero bug
  // ship unfixed in one of its two copies.
  protected def childOutput: Seq[Attribute]
  protected def mode: WindowGroupLimitMode
  protected def minCompaction: Double

  // Two distinct SQLMetrics rather than one combined "fallback happened" counter: the two
  // triggers call for opposite remediation (capacity/skew investigation for a genuine OOM, versus
  // tuning minCompaction/maxLimit down for a benign, workload-shape-driven proactive
  // bailout), so collapsing them into one count would hide which situation actually occurred from
  // anyone reading the SQL UI rather than grepping executor logs for the two already-distinct
  // logInfo messages below.
  protected def numTasksFallBackedOOM: SQLMetric
  protected def numTasksFallBackedPoorCompaction: SQLMetric

  /**
   * Builds the `Final`-mode fallback destination: an unsorted, append-only, spill-capable row
   * array. Builds the `Partial`-mode fallback destination otherwise: the same (spill-capable)
   * sorter [[org.apache.spark.sql.execution.SortExec]] itself uses. See the concrete
   * implementations' own `buildFallbackArray`/`buildFallbackSorter` docs for why `Final` never
   * needs a sorter, and why each is seeded with every row already known to be kept.
   */
  protected def buildFallbackArray(): ExternalAppendOnlyUnsafeRowArray
  protected def buildFallbackSorter(): UnsafeExternalRowSorter

  /**
   * Reached when the hash map or the row arena could no longer acquire memory. Dumps every row
   * already known to be kept, plus `pendingRow` (the row that just failed to be
   * inserted/updated), into the fallback destination (a sorter for `Partial`, an unsorted array
   * for `Final`; see `fallbackDestination`'s doc). Every row consumed after this point goes
   * straight there instead of the hash map.
   */
  protected def switchToFallback(pendingRow: UnsafeRow): Unit = {
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
    numTasksFallBackedOOM += 1
  }

  /**
   * Reached when [[maybeBailOutForPoorEfficiency]] decides hash accumulation isn't paying off.
   * Unlike [[switchToFallback]], there is no extra pending row to insert: the row that triggered
   * this check was already fully processed into the concrete accumulator's own row storage.
   */
  protected def switchToFallbackProactively(): Unit = {
    fallbackDestination = if (mode == Final) {
      ArrayFallbackDestination(buildFallbackArray())
    } else {
      SorterFallbackDestination(buildFallbackSorter())
    }
    numTasksFallBackedPoorCompaction += 1
  }

  /**
   * Checks whether hash accumulation is still paying off, proactively falling back rather than
   * waiting for the hash map to genuinely run out of memory (which, for high-cardinality keys, may
   * happen far later than it should, if at all): rows-processed-per-distinct-key has fallen below
   * `minCompaction` -- duplicate keys aren't being pruned enough to offset the hash map's
   * per-row overhead. `rowsConsumed`/`numKeys` are passed in rather than read from an abstract
   * member: both are already locally available at every call site (inside `consumeRow`, right
   * after incrementing `rowsConsumed`), so there is no need to expose the underlying mutable
   * counter or the hash map itself just for this one periodic check.
   */
  protected def maybeBailOutForPoorEfficiency(rowsConsumed: Long, numKeys: Int): Unit = {
    if (numKeys > 0 && rowsConsumed.toDouble / numKeys < minCompaction) {
      val ratio = rowsConsumed.toDouble / numKeys
      logInfo(s"Hash-based window group limit falling back to sort-based: compaction ratio " +
        s"$ratio after $rowsConsumed rows is below the minimum $minCompaction.")
      switchToFallbackProactively()
    }
  }

  // --- Shared single-order-column fast path -------------------------------------------------
  // The following members back both concrete accumulators' single-column comparison logic (see
  // HashGroupLimitAccumulator.singleColTypeTag for the type-eligibility table these rely on).
  // Homed here, as one definition both classes inherit, rather than duplicated verbatim in each:
  // a prior duplicate of compareSlotSingle independently shipped the same signed-zero/Double-
  // Float comparison bug in both files, which a single shared definition makes structurally
  // impossible to re-diverge.
  protected def singleColTypeTag: Int
  protected def isSingleColAscending: Boolean
  protected def isSingleColNullsFirst: Boolean
  protected def isSingleColNullable: Boolean
  protected def bitSetWidthInBytes: Int
  protected def slotOrderOrdinal(slot: Int, col: Int): Int

  /**
   * Compares a single-column order-key value (`kVal`/`kNull`) against slot `slot`'s stored value,
   * reading directly from `buffer`'s raw memory rather than unpacking either side into an
   * [[UnsafeRow]]. Double/Float cases go through [[SQLOrderingUtil]] to match Spark's actual SQL
   * float ordering (`-0.0 == 0.0`, `NaN` greatest) -- unlike raw `java.lang.Double`/
   * `Float.compare`, which ranks `0.0` strictly above `-0.0`, that mismatch previously let two
   * "equal" order-key rows of `0.0`/`-0.0` occupy two distinct slots instead of one, which is
   * silently wrong for `DENSE_RANK()` (whose slot weight counts distinct slots, not rows).
   */
  protected def compareSlotSingle(
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
        SQLOrderingUtil.compareDoubles(
          java.lang.Double.longBitsToDouble(kVal), java.lang.Double.longBitsToDouble(sVal))
      case 4 =>
        SQLOrderingUtil.compareFloats(
          java.lang.Float.intBitsToFloat(kVal.toInt), java.lang.Float.intBitsToFloat(sVal.toInt))
      case 5 => java.lang.Short.compare(kVal.toShort, sVal.toShort)
      case 6 => java.lang.Byte.compare(kVal.toByte, sVal.toByte)
      case 7 => java.lang.Boolean.compare(kVal != 0, sVal != 0)
      case 8 => java.lang.Long.compare(kVal, sVal)
      case _ => 0
    }
    if (isSingleColAscending) cmp else -cmp
  }

  protected def compareSlotSingle(kVal: Long, kNull: Boolean, buffer: UnsafeRow, slot: Int): Int =
    compareSlotSingle(kVal, kNull, buffer.getBaseObject, buffer.getBaseOffset, slot)
}

/**
 * Constants shared by [[HashRowNumberGroupLimitAccumulator]] and [[HashRankGroupLimitAccumulator]]
 * -- homed here, on the trait both extend, rather than on either accumulator's own companion
 * object, so neither implementation appears to privately own knowledge the other equally depends
 * on.
 */
object HashGroupLimitAccumulator {
  // Matches UnsafeHashedRelation/LongToUnsafeRowMap's own choice of initial page size for a
  // BytesToBytesMap-backed structure expected to often hold little data (see HashedRelation.scala).
  private[window] val INITIAL_PAGE_SIZE_BYTES: Long = 1L << 20

  // How many output rows `nextResultRow` accumulates locally before flushing to the numOutputRows
  // SQLMetric (a LongAccumulator), so a task emitting many rows pays one accumulator update per
  // batch instead of one per row.
  private[window] val OUTPUT_ROWS_METRIC_BATCH_SIZE: Int = 1024

  /**
   * Maps a single order-key column's data type to a nonzero tag if the single-column fast path
   * (see [[HashGroupLimitAccumulator.compareSlotSingle]]) supports it, 0 otherwise. The sole
   * source of truth for this eligibility: both concrete accumulators' own `singleColTypeTag`
   * field and [[WindowGroupLimitExec]]'s `isSingleOrderCol` codegen check all defer to this one
   * function, so a type added here is automatically in lock-step everywhere it matters, instead
   * of three independently-maintained copies of the same type list risking silent drift. Every
   * case here is read back with raw `getLong`/`Platform.getLong` in [[compareSlotSingle]], so any
   * type added must also satisfy `UnsafeRow.isFixedLength` -- see [[WindowGroupLimitExec
   * .isHashEligible]]'s doc for why the broader `UnsafeRow.isMutable` is not sufficient.
   */
  private[window] def singleColTypeTag(dataType: DataType): Int = dataType match {
    case LongType | TimestampType | TimestampNTZType => 1
    case IntegerType | DateType => 2
    case DoubleType => 3
    case FloatType => 4
    case ShortType => 5
    case ByteType => 6
    case BooleanType => 7
    case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS => 8
    case _ => 0
  }
}

/**
 * Common interface over [[HashRowNumberGroupLimitEvaluatorFactory]] and
 * [[HashRankGroupLimitEvaluatorFactory]], mirroring [[HashGroupLimitAccumulator]]'s purpose for
 * the factory side: lets [[WindowGroupLimitExec]] hold one statically-typed
 * `hashEvaluatorFactory` regardless of `rankLikeFunction`, both for the interpreted `doExecute()`
 * path (via the inherited [[PartitionEvaluatorFactory]]) and for codegen's `createAccumulator()`
 * call in `doProduce`.
 */
trait HashGroupLimitEvaluatorFactory extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  /**
   * Builds a fresh, per-task accumulator. Used both by [[createEvaluator]] below (the interpreted
   * `doExecute()` path) and directly by [[WindowGroupLimitExec]]'s whole-stage codegen, which
   * drives the accumulator with generated code instead of going through [[createEvaluator]]'s
   * pull-based [[PartitionEvaluator]].
   */
  def createAccumulator(): HashGroupLimitAccumulator

  /**
   * Pulls rows from the single child input, feeding each to a fresh [[createAccumulator]] via its
   * single-argument `consumeRow(row)`, then drains the accumulator's result. Shared by both
   * concrete factories -- the loop only ever touches [[HashGroupLimitAccumulator]]'s common
   * interface, so there is nothing for a `RowNumber`- vs `Rank`/`DenseRank`-specific override to
   * add.
   */
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new HashGroupLimitPartitionEvaluator

  private class HashGroupLimitPartitionEvaluator
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

/**
 * This operator is designed to filter out unnecessary rows before WindowExec
 * for top-k computation.
 * @param partitionSpec Should be the same as [[WindowExec#partitionSpec]].
 * @param orderSpec Should be the same as [[WindowExec#orderSpec]].
 * @param rankLikeFunction The function to compute row rank, should be RowNumber/Rank/DenseRank.
 * @param limit The limit for rank value.
 * @param mode The mode describes [[WindowGroupLimitExec]] before or after shuffle.
 * @param child The child spark plan.
 */
case class WindowGroupLimitExec(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends UnaryExecNode with BlockingOperatorWithCodegen {

  override def output: Seq[Attribute] = child.output

  override def requiredChildDistribution: Seq[Distribution] = mode match {
    case Partial => super.requiredChildDistribution
    case Final =>
      if (partitionSpec.isEmpty) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(partitionSpec) :: Nil
      }
  }

  /**
   * True when this operator can skip requiring sorted input and instead keep, per partition key,
   * only the rows that can possibly be part of the final answer in a hash map
   * ([[HashRowNumberGroupLimitAccumulator]] for `RowNumber`; [[HashRankGroupLimitAccumulator]]
   * for `Rank`/`DenseRank`, which must keep every row tied at a qualifying order-key value, not
   * just `limit` rows). This is a static, plan-time decision, not a runtime fallback: it requires
   * `limit` to be within `windowGroupLimitHashPushdownMaxLimit` (each key's fixed-width per-key
   * state grows with `limit`), and every order-key expression's type to be one `UnsafeRow` stores
   * directly in a fixed-width 8-byte word (`UnsafeRow.isFixedLength`, not the broader
   * `UnsafeRow.isMutable`: both accumulators' `extractSlotInto`/`writeSlotFrom` copy an order-key
   * field between rows with raw `getLong`/`setLong`, which is only safe for a type whose on-the-
   * wire representation *is* that 8-byte value -- a wide `DecimalType` (precision above
   * `Decimal.MAX_LONG_DIGITS`), `CalendarIntervalType`, or a nanosecond-precision timestamp is
   * `isMutable` but stores a `(cursor, length)` pointer into the row's own variable-length region
   * instead, which `isFixedLength` correctly excludes).
   */
  lazy val isHashEligible: Boolean =
    conf.windowGroupLimitHashPushdownEnabled &&
      (rankLikeFunction.isInstanceOf[RowNumber] ||
        rankLikeFunction.isInstanceOf[Rank] ||
        rankLikeFunction.isInstanceOf[DenseRank]) &&
      limit >= 1 &&
      limit <= conf.windowGroupLimitHashPushdownMaxLimit &&
      orderSpec.forall(order => UnsafeRow.isFixedLength(order.child.dataType))

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    if (isHashEligible) {
      Seq(Nil)
    } else {
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    }

  override def outputOrdering: Seq[SortOrder] =
    if (isHashEligible) Nil else child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  // isHashEligible is a derived lazy val, not a constructor field, so it would otherwise never
  // appear in EXPLAIN output -- leaving no static way to tell which path a given instance takes
  // without running the query and inspecting its SQLMetrics. Mirrors ShuffledJoin's identical
  // "(skew=true)" nodeName suffix for the same kind of derived boolean plan characteristic.
  override def nodeName: String =
    if (isHashEligible) super.nodeName + "(hash)" else super.nodeName

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  ) ++ {
    // Only the hash path can report these -- the sort-based path has nothing to fall back
    // from -- so the rest don't show a constant/misleading 0 in the SQL UI for ineligible plans
    // (see HashAggregateExec.metrics for the same "only when applicable" approach).
    if (isHashEligible) {
      Map(
        "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
        "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
        "numTasksFallBackedOOM" -> SQLMetrics.createMetric(
          sparkContext, "number of sort fallback tasks (out of memory)"),
        "numTasksFallBackedPoorCompaction" -> SQLMetrics.createMetric(
          sparkContext, "number of sort fallback tasks (poor compaction ratio)"))
    } else {
      Map.empty
    }
  }

  // Only ever constructed when isHashEligible is true: shared by the interpreted doExecute()
  // path below and by doProduce()'s codegen path (see supportCodegen), as a single source of
  // truth for how a task's accumulator gets built. Deliberately captures only
  // partitionSpec/orderSpec/rankLikeFunction/child.output/the metrics -- not `this` or `child` --
  // so codegen's addReferenceObj does not drag the child plan into every task's closure.
  @transient private lazy val hashEvaluatorFactory: HashGroupLimitEvaluatorFactory =
    rankLikeFunction match {
      case _: RowNumber =>
        new HashRowNumberGroupLimitEvaluatorFactory(
          partitionSpec,
          orderSpec,
          child.output,
          longMetric("numOutputRows"),
          longMetric("peakMemory"),
          longMetric("spillSize"),
          longMetric("numTasksFallBackedOOM"),
          longMetric("numTasksFallBackedPoorCompaction"),
          conf.windowGroupLimitHashPushdownMinCompaction,
          conf.windowGroupLimitHashPushdownMinRows,
          conf.enableRadixSort,
          limit,
          mode)
      case _ =>
        new HashRankGroupLimitEvaluatorFactory(
          partitionSpec,
          orderSpec,
          rankLikeFunction,
          child.output,
          longMetric("numOutputRows"),
          longMetric("peakMemory"),
          longMetric("spillSize"),
          longMetric("numTasksFallBackedOOM"),
          longMetric("numTasksFallBackedPoorCompaction"),
          conf.windowGroupLimitHashPushdownMinCompaction,
          conf.windowGroupLimitHashPushdownMinRows,
          conf.enableRadixSort,
          limit,
          mode)
    }

  protected override def doExecute(): RDD[InternalRow] = {
    val evaluatorFactory =
      if (isHashEligible) {
        hashEvaluatorFactory
      } else {
        new WindowGroupLimitEvaluatorFactory(
          partitionSpec,
          orderSpec,
          rankLikeFunction,
          limit,
          child.output,
          longMetric("numOutputRows"))
      }

    if (conf.usePartitionEvaluator) {
      child.execute().mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      child.execute().mapPartitionsWithIndexInternal { (index, rowIterator) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(index, rowIterator)
      }
    }
  }

  // Only the hash path is codegen'd; the sort-based path keeps acting as a pipeline breaker
  // exactly as it does today.
  override def supportCodegen: Boolean = isHashEligible

  // All of child.output: row's own materialization already needs every column (output =
  // child.output, so the full row must survive to be re-emitted later), and doConsume's generated
  // code now also independently projects partitionSpec/orderSpec straight off `input`. Declaring
  // the full set here is what makes WholeStageCodegenExec.consume's evaluateRequiredVariables
  // force-evaluate each of those input columns exactly once and clear their ExprCode.code --
  // without it, row's construction, the key projection, and the orderKey projection would each
  // independently re-emit (and re-declare) the same underlying input variable wherever they
  // overlap, which javac/Janino rejects as a local variable redefinition.
  override def usedInputs: AttributeSet = AttributeSet(child.output)

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()

  // Set by doProduce, read by doConsume -- both are only ever invoked while generating code for
  // the same WholeStageCodegenExec, so sharing this var across the two calls is safe.
  private var accumulatorTerm: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToConsume = ctx.addMutableState(
      "boolean", "needToConsumeGroupLimit", v => s"$v = true;")
    val factoryTerm = ctx.addReferenceObj(
      "groupLimitAccumulatorFactory",
      hashEvaluatorFactory,
      classOf[HashGroupLimitEvaluatorFactory].getName)
    accumulatorTerm = ctx.addMutableState(
      classOf[HashGroupLimitAccumulator].getName,
      "groupLimitAccumulator",
      v => s"$v = $factoryTerm.createAccumulator();",
      forceInline = true)
    val consumeGroupLimitInput = ctx.freshName("consumeGroupLimitInput")
    val consumeInputFuncName = ctx.addNewFunction(consumeGroupLimitInput,
      s"""
         | private void $consumeGroupLimitInput(int partitionIndex) throws java.io.IOException {
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
       """.stripMargin)

    val outputRow = ctx.freshName("groupLimitRow")
    ctx.INPUT_ROW = outputRow
    s"""
       | if ($needToConsume) {
       |   $consumeInputFuncName(partitionIndex);
       |   $accumulatorTerm.finishAccumulator();
       |   $needToConsume = false;
       | }
       | org.apache.spark.sql.catalyst.expressions.UnsafeRow $outputRow =
       |   new org.apache.spark.sql.catalyst.expressions.UnsafeRow(${child.output.length});
       | while ($limitNotReachedCond $accumulatorTerm.nextResultRow($outputRow)) {
       |   ${consume(ctx, null, outputRow)}
       |   if (shouldStop()) return;
       | }
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // Checks fallbackDestinationOrNull on every row rather than caching it in a mutable state once
    // seen non-null: consumeRow() itself needs the identical check (the interpreted path drives it
    // directly, without going through doConsume at all), so there is no simpler, still-correct
    // alternative -- but once non-null it stays non-null for the rest of the task (see
    // HashGroupLimitAccumulator's doc), so the JIT's branch predictor settles on the fallback
    // branch immediately, leaving destTerm.insertRow as a direct dispatch on a concrete class,
    // exactly like SortExec.doConsume's own insertRow call -- unlike routing every such row through
    // consumeRow's own virtual dispatch (through this trait) *plus* its internal null check. One
    // check covers both `mode`s: FallbackDestination hides whether it's backed by the Partial-mode
    // sorter or the Final-mode array (see FallbackDestination's doc), so there is nothing left for
    // `mode` to select between here.
    val destTerm = ctx.freshName("fallbackDestination")
    val destClass = classOf[FallbackDestination].getName

    // Projects partitionSpec/orderSpec directly off `input`'s already-materialized column
    // registers (ctx.currentVars, set by the framework before doConsume runs -- see
    // WholeStageCodegenExec.consume) via GenerateUnsafeProjection, the same mechanism
    // HashAggregateExec.doConsumeWithKeys uses for its own grouping key. This independently
    // (re-)evaluates partitionSpec's/orderSpec's expressions straight from those registers rather
    // than going through HashGroupLimitAccumulator's own groupingProjection/orderKeyProjection,
    // which would otherwise redundantly unpack `row` below -- itself unavoidably needed, since a
    // kept/candidate row's full bytes must still be stored in rowArena -- just to re-extract the
    // handful of columns partitionSpec/orderSpec actually need. Placed inside the non-fallback
    // branch below: once a task has permanently fallen back, FallbackDestination.insertRow needs
    // neither. Relies on usedInputs (above) declaring the full child.output so the framework
    // force-evaluates and clears each input column's code exactly once -- otherwise row's own
    // construction and these two projections would each independently re-declare the same
    // underlying input variable wherever they overlap, which Janino rejects outright.
    val boundPartitionSpec = BindReferences.bindReferences[Expression](partitionSpec, child.output)
    val boundOrderKeyExprs =
      BindReferences.bindReferences[Expression](orderSpec.map(_.child), child.output)
    val keyCode = GenerateUnsafeProjection.createCode(ctx, boundPartitionSpec)
    val keyHashTerm = ctx.freshName("keyHash")

    val isSingleOrderCol = boundOrderKeyExprs.length == 1 &&
      HashGroupLimitAccumulator.singleColTypeTag(boundOrderKeyExprs.head.dataType) != 0

    val (orderKeySetup, orderKeyArgs) = if (isSingleOrderCol) {
      val orderExpr = boundOrderKeyExprs.head
      val orderEv = orderExpr.genCode(ctx)
      val orderValTerm = ctx.freshName("orderVal")
      val orderNullTerm = ctx.freshName("orderNull")
      val toLongCode = orderExpr.dataType match {
        case LongType | TimestampType | TimestampNTZType =>
          s"${orderEv.value}"
        case IntegerType | DateType | ShortType | ByteType =>
          s"(long) ${orderEv.value}"
        case BooleanType =>
          s"${orderEv.value} ? 1L : 0L"
        case FloatType =>
          s"(long) java.lang.Float.floatToIntBits(${orderEv.value})"
        case DoubleType =>
          s"java.lang.Double.doubleToLongBits(${orderEv.value})"
        case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
          s"${orderEv.value}.toUnscaledLong()"
        case _ => ""
      }
      val setup =
        s"""
           |${orderEv.code}
           |long $orderValTerm = ${orderEv.isNull} ? 0L : $toLongCode;
           |boolean $orderNullTerm = ${orderEv.isNull};
         """.stripMargin
      (setup, s"$orderValTerm, $orderNullTerm")
    } else {
      val orderKeyCode = GenerateUnsafeProjection.createCode(ctx, boundOrderKeyExprs)
      (orderKeyCode.code.toString, s"(UnsafeRow) ${orderKeyCode.value}")
    }

    s"""
       |$destClass $destTerm = $accumulatorTerm.fallbackDestinationOrNull();
       |if ($destTerm != null) {
       |  ${row.code}
       |  $destTerm.insertRow((UnsafeRow) ${row.value});
       |} else {
       |  ${keyCode.code}
       |  int $keyHashTerm = ${keyCode.value}.hashCode();
       |  $orderKeySetup
       |  ${row.code}
       |  $accumulatorTerm.consumeRow((UnsafeRow) ${row.value}, (UnsafeRow) ${keyCode.value},
       |    $keyHashTerm, $orderKeyArgs);
       |}
     """.stripMargin
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WindowGroupLimitExec =
    copy(child = newChild)
}

abstract class BaseLimitIterator extends Iterator[InternalRow] {
  def numOutputRows: SQLMetric

  def input: Iterator[InternalRow]

  def limit: Int

  var rank = 0

  var nextRow: UnsafeRow = null

  // Increase the rank value.
  def increaseRank(): Unit

  override def hasNext: Boolean = rank < limit && input.hasNext

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    nextRow = input.next().asInstanceOf[UnsafeRow]
    increaseRank()
    numOutputRows += 1
    nextRow
  }

  def reset(): Unit
}

case class SimpleLimitIterator(
    input: Iterator[InternalRow],
    limit: Int,
    numOutputRows: SQLMetric) extends BaseLimitIterator {

  override def increaseRank(): Unit = {
    rank += 1
  }

  override def reset(): Unit = {
    rank = 0
  }
}

trait OrderSpecProvider {
  def output: Seq[Attribute]
  def orderSpec: Seq[SortOrder]

  val ordering = GenerateOrdering.generate(orderSpec, output)
  var currentRankRow: UnsafeRow = null
}

case class RankLimitIterator(
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int,
    numOutputRows: SQLMetric) extends BaseLimitIterator with OrderSpecProvider {

  var count = 0

  override def increaseRank(): Unit = {
    if (count == 0) {
      currentRankRow = nextRow.copy()
    } else {
      if (ordering.compare(currentRankRow, nextRow) != 0) {
        rank = count
        currentRankRow = nextRow.copy()
      }
    }
    count += 1
  }

  override def reset(): Unit = {
    rank = 0
    count = 0
    currentRankRow = null
  }
}

case class DenseRankLimitIterator(
    output: Seq[Attribute],
    input: Iterator[InternalRow],
    orderSpec: Seq[SortOrder],
    limit: Int,
    numOutputRows: SQLMetric) extends BaseLimitIterator with OrderSpecProvider {

  override def increaseRank(): Unit = {
    if (currentRankRow == null) {
      currentRankRow = nextRow.copy()
    } else {
      if (ordering.compare(currentRankRow, nextRow) != 0) {
        rank += 1
        currentRankRow = nextRow.copy()
      }
    }
  }

  override def reset(): Unit = {
    rank = 0
    currentRankRow = null
  }
}

class GroupedLimitIterator(
    input: Iterator[InternalRow],
    output: Seq[Attribute],
    partitionSpec: Seq[Expression],
    createLimitIterator: Iterator[InternalRow] => BaseLimitIterator)
  extends Iterator[InternalRow] {

  val grouping = UnsafeProjection.create(partitionSpec, output)

  // Manage the stream and the grouping.
  var nextRow: UnsafeRow = null
  var nextGroup: UnsafeRow = null
  var nextRowAvailable: Boolean = false
  protected[this] def fetchNextRow(): Unit = {
    nextRowAvailable = input.hasNext
    if (nextRowAvailable) {
      nextRow = input.next().asInstanceOf[UnsafeRow]
      nextGroup = grouping(nextRow)
    }
  }
  fetchNextRow()

  var groupIterator: GroupIterator = _
  var limitIterator: BaseLimitIterator = _
  if (nextRowAvailable) {
    groupIterator = new GroupIterator()
    limitIterator = createLimitIterator(groupIterator)
  }

  override final def hasNext: Boolean = nextRowAvailable && {
    if (!limitIterator.hasNext) {
      // if `limitIterator.hasNext` is false, we should jump to the next group if present
      groupIterator.skipRemainingRows()
      limitIterator.reset()
    }
    limitIterator.hasNext
  }

  override final def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    limitIterator.next()
  }

  class GroupIterator() extends Iterator[InternalRow] {
    // Before we start to fetch new input rows, make a copy of nextGroup.
    var currentGroup = nextGroup.copy()

    def hasNext: Boolean = nextRowAvailable && nextGroup == currentGroup

    def next(): InternalRow = {
      if (!hasNext) throw new NoSuchElementException
      val currentRow = nextRow.copy()
      fetchNextRow()
      currentRow
    }

    def skipRemainingRows(): Unit = {
      // Skip all the remaining rows in this group
      while (hasNext) {
        fetchNextRow()
      }

      // Switch to next group
      if (nextRowAvailable) currentGroup = nextGroup.copy()
    }
  }
}
