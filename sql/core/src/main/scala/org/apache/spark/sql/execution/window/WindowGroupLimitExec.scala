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

import org.apache.spark.PartitionEvaluatorFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, DenseRank,
  Expression, Rank, RowNumber, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode,
  GenerateOrdering}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution,
  Distribution, Partitioning}
import org.apache.spark.sql.execution.{BlockingOperatorWithCodegen, CodegenSupport,
  ExternalAppendOnlyUnsafeRowArray, SparkPlan, UnaryExecNode, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

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
trait FallbackDestination {
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
trait HashGroupLimitAccumulator {
  def consumeRow(row: UnsafeRow): Unit
  def finish(): Iterator[InternalRow]

  /**
   * Non-null once this accumulator has permanently fallen back to [[FallbackDestination]] output
   * (see the concrete implementations' class docs for the possible triggers). Exposed so
   * `doConsume`'s generated code can, once a task enters permanent fallback, insert directly into
   * this destination -- the same direct dispatch [[org.apache.spark.sql.execution.SortExec]]'s own
   * `doConsume` uses for its sorter -- instead of paying `consumeRow`'s extra virtual dispatch
   * (through this trait) plus its own internal null check on every one of the, possibly many
   * million, remaining rows.
   */
  def fallbackDestinationOrNull: FallbackDestination
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

  // Below this, the periodic compaction check always runs at the fixed
  // minRowsBetweenCompactionChecks interval, unchanged from before ramp-up existed -- several
  // low-level tests hand-tune small minRowsBetweenCompactionChecks values (down to single digits)
  // to land the check's first-ever evaluation on an exact row, including one that must coincide
  // with a specific genuine-OOM row found by trial and error; a value this far below any of those
  // is guaranteed not to disturb them. At or above it (in particular, the real, much larger
  // default of 100000), ramp-up's smaller first checks start paying for themselves: see
  // HashRowNumberGroupLimitAccumulator.consumeRow's doc.
  private[window] val COMPACTION_CHECK_RAMP_UP_THRESHOLD: Long = 1000L
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
  def createAccumulator(): HashGroupLimitAccumulator
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
   * state grows with `limit`), and every order-key expression's type to be one `UnsafeRow` can
   * mutate in place (the hash map's per-key state is fixed-width; see
   * [[HashRowNumberGroupLimitAccumulator]] for why).
   */
  lazy val isHashEligible: Boolean =
    conf.windowGroupLimitHashPushdownEnabled &&
      (rankLikeFunction.isInstanceOf[RowNumber] ||
        rankLikeFunction.isInstanceOf[Rank] ||
        rankLikeFunction.isInstanceOf[DenseRank]) &&
      limit >= 1 &&
      limit <= conf.windowGroupLimitHashPushdownMaxLimit &&
      orderSpec.forall(order => UnsafeRow.isMutable(order.child.dataType))

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    if (isHashEligible) {
      Seq(Nil)
    } else {
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    }

  override def outputOrdering: Seq[SortOrder] =
    if (isHashEligible) Nil else child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

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
        "numTasksFallBacked" ->
          SQLMetrics.createMetric(sparkContext, "number of sort fallback tasks"))
    } else {
      Map.empty
    }
  }

  // Only ever constructed when isHashEligible is true: shared by the interpreted doExecute()
  // path below and by doProduce()'s codegen path (see supportCodegen), as a single source of
  // truth for how a task's accumulator gets built. Deliberately captures only
  // partitionSpec/orderSpec/rankLikeFunction/child.output/the metrics -- not `this` or `child` --
  // so codegen's addReferenceObj does not drag the child plan into every task's closure.
  @transient private[sql] lazy val hashEvaluatorFactory: HashGroupLimitEvaluatorFactory =
    rankLikeFunction match {
      case _: RowNumber =>
        new HashRowNumberGroupLimitEvaluatorFactory(
          partitionSpec,
          orderSpec,
          child.output,
          longMetric("numOutputRows"),
          longMetric("peakMemory"),
          longMetric("spillSize"),
          longMetric("numTasksFallBacked"),
          conf.windowGroupLimitHashPushdownMinCompactionRatio,
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
          longMetric("numTasksFallBacked"),
          conf.windowGroupLimitHashPushdownMinCompactionRatio,
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

  override def usedInputs: AttributeSet = AttributeSet.empty

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
    val resultIterTerm = ctx.addMutableState(
      "scala.collection.Iterator<InternalRow>", "groupLimitResultIter", forceInline = true)

    val consumeGroupLimitInput = ctx.freshName("consumeGroupLimitInput")
    val consumeInputFuncName = ctx.addNewFunction(consumeGroupLimitInput,
      s"""
         | private void $consumeGroupLimitInput(int partitionIndex) throws java.io.IOException {
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
       """.stripMargin)

    val outputRow = ctx.freshName("groupLimitRow")
    s"""
       | if ($needToConsume) {
       |   $consumeInputFuncName(partitionIndex);
       |   $resultIterTerm = $accumulatorTerm.finish();
       |   $needToConsume = false;
       | }
       | while ($limitNotReachedCond $resultIterTerm.hasNext()) {
       |   UnsafeRow $outputRow = (UnsafeRow) $resultIterTerm.next();
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
    s"""
       |${row.code}
       |$destClass $destTerm = $accumulatorTerm.fallbackDestinationOrNull();
       |if ($destTerm != null) {
       |  $destTerm.insertRow((UnsafeRow) ${row.value});
       |} else {
       |  $accumulatorTerm.consumeRow((UnsafeRow) ${row.value});
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
