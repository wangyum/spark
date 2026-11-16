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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Expression, RowNumber, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateOrdering}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{BlockingOperatorWithCodegen, CodegenSupport, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

sealed trait WindowGroupLimitMode

case object Partial extends WindowGroupLimitMode

case object Final extends WindowGroupLimitMode

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
   * only the single best row seen so far in a hash map
   * ([[HashRowNumberGroupLimitAccumulator]]). This is a static, plan-time decision, not a
   * runtime fallback: it is only safe when at most one row per key can ever be the final answer
   * (`RowNumber` with `limit == 1`; `Rank`/`DenseRank` must keep every row tied for the best order
   * key, which is not a bounded accumulation), and only when every order-key expression's type is
   * one `UnsafeRow` can mutate in place (the hash map's per-key state is fixed-width; see
   * [[HashRowNumberGroupLimitAccumulator]] for why).
   */
  lazy val isHashEligible: Boolean =
    conf.windowGroupLimitHashPushdownEnabled &&
      rankLikeFunction.isInstanceOf[RowNumber] &&
      limit == 1 &&
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
  // truth for how a task's HashRowNumberGroupLimitAccumulator gets built. Deliberately captures
  // only partitionSpec/orderSpec/child.output/the metrics -- not `this` or `child` -- so
  // codegen's addReferenceObj does not drag the child plan into every task's closure.
  @transient private[sql] lazy val hashEvaluatorFactory: HashRowNumberGroupLimitEvaluatorFactory =
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
      conf.enableRadixSort)

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

  // Only the hash path (Section 4.3/4.4 of the design) is codegen'd; the sort-based path keeps
  // acting as a pipeline breaker exactly as it does today.
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
    val factoryTerm = ctx.addReferenceObj("groupLimitAccumulatorFactory", hashEvaluatorFactory)
    accumulatorTerm = ctx.addMutableState(
      classOf[HashRowNumberGroupLimitAccumulator].getName,
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
    s"""
       |${row.code}
       |$accumulatorTerm.consumeRow((UnsafeRow) ${row.value});
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
