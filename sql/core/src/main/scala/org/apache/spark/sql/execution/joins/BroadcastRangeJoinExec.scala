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

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.RangeJoinKind
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs a range join on two tables. Supported types are inner, left outer, right outer,
 * left semi, and left anti. The preserved side is streamed: unmatched build rows are not
 * emitted, because the broadcast is copied to every partition. See `ExtractRangeJoinKeys`
 * for the exact set of join-condition shapes that are recognized as a range join.
 *
 * The build side is an index. Point-in-range and interval overlap both build an
 * [[IntervalIndex]] and probe it with `overlapping`. A single inequality builds a
 * [[PointIndex]] and scans the side of the bound the build side holds. Keys that
 * map to one order-preserving long instead build [[LongIntervalIndex]] or
 * [[LongPointIndex]], so their probes compare raw longs without boxing. Either
 * probe returns a superset. A candidate is kept only when the original join
 * condition is true, so inclusivity stays on the predicate. Heavy overlap can
 * make one probe return almost every build row.
 */
case class BroadcastRangeJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    rangeJoinKind: RangeJoinKind)
  extends JoinCodegenSupport {

  if (condition.isEmpty) {
    throw SparkException.internalError("BroadcastRangeJoinExec requires a join condition.")
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  /** BuildRight means the right relation is the broadcast relation. */
  private val (streamed, broadcast, streamedKeys, buildKeys) = buildSide match {
    case BuildRight => (left, right, leftKeys, rightKeys)
    case BuildLeft => (right, left, rightKeys, leftKeys)
  }

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType $buildSide ($opId)".trim
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = RangeRelationBroadcastMode(
      BindReferences.bindReferences(buildKeys, broadcast.output), probeKind.indexKind)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = streamed.outputOrdering

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case LeftSemi | LeftAnti =>
        left.output
      case t =>
        throw SparkException.internalError(s"BroadcastRangeJoin should not take $t as the JoinType")
    }
  }

  // How this join probes its build side's materialized RangeRelation. See `RangeProbeKind`.
  private val probeKind: RangeProbeKind = RangeProbeKind.forJoin(rangeJoinKind, buildSide)

  private def perPartition(
      relation: Broadcast[RangeRelation])(
      f: BroadcastRangeJoinExec.ProbeContext => Iterator[InternalRow]): RDD[InternalRow] = {
    val boundStreamKeys = BindReferences.bindReferences(streamedKeys, streamed.output)
    val cond = condition.get
    val schema = left.output ++ right.output
    val numBuildFields = broadcast.output.length
    val kind = probeKind
    val side = buildSide
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val candidateFn = RangeRelationProbe.makeCandidateFn(
        relation.value, boundStreamKeys, kind)
      f(new BroadcastRangeJoinExec.ProbeContext(
        streamedIter,
        Predicate.create(cond, schema).eval _,
        new JoinedRow,
        new GenericInternalRow(numBuildFields),
        candidateFn,
        side))
    }
  }

  private def innerJoin(relation: Broadcast[RangeRelation]): RDD[InternalRow] = {
    perPartition(relation) { p =>
      p.stream.flatMap { row =>
        p.candidates(row).map(p.join(row, _)).filter(p.condition)
      }
    }
  }

  /**
   * Emit each candidate the condition accepts. When none were, emit the stream
   * row once with the build side padded by nulls.
   *
   * The loop test checks `streamRow` before `stream.hasNext`. A whole-stage
   * stream reuses one `UnsafeRow`, and `hasNext` fills that buffer. It must
   * not run while the current row is still being joined.
   */
  private def outerJoin(relation: Broadcast[RangeRelation]): RDD[InternalRow] = {
    perPartition(relation) { p =>
      new Iterator[InternalRow] {
        private var streamRow: InternalRow = null
        private var matches: Iterator[InternalRow] = Iterator.empty
        private var foundMatch: Boolean = false
        private var resultRow: InternalRow = null

        private def findNext(): Boolean = {
          while (streamRow != null || p.stream.hasNext) {
            if (streamRow == null) {
              streamRow = p.stream.next()
              foundMatch = false
              matches = p.candidates(streamRow)
            }
            while (matches.hasNext) {
              val joined = p.join(streamRow, matches.next())
              if (p.condition(joined)) {
                foundMatch = true
                resultRow = joined
                return true
              }
            }
            if (!foundMatch) {
              resultRow = p.join(streamRow, p.nullBuild)
              streamRow = null
              return true
            }
            streamRow = null
          }
          false
        }

        override def hasNext: Boolean = resultRow != null || findNext()

        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /**
   * The implementation for LeftSemi and LeftAnti joins.
   */
  private def leftExistenceJoin(
      relation: Broadcast[RangeRelation],
      exists: Boolean): RDD[InternalRow] = {
    perPartition(relation) { p =>
      p.stream.filter { row =>
        val matched = p.candidates(row).exists { buildRow =>
          p.condition(p.join(row, buildRow))
        }
        matched == exists
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    if (!BroadcastRangeJoinExec.supports(joinType, buildSide)) {
      throw SparkException.internalError(
        s"BroadcastRangeJoin does not support $joinType with $buildSide")
    }
    val broadcastedRelation = broadcast.executeBroadcast[RangeRelation]()
    val resultRdd = joinType match {
      case _: InnerLike => innerJoin(broadcastedRelation)
      case LeftOuter | RightOuter => outerJoin(broadcastedRelation)
      case LeftSemi => leftExistenceJoin(broadcastedRelation, exists = true)
      case LeftAnti => leftExistenceJoin(broadcastedRelation, exists = false)
      case t =>
        throw SparkException.internalError(
          s"BroadcastRangeJoin should not take $t as the JoinType")
    }
    val (resultOutput, resultInput) = joinType match {
      case LeftSemi | LeftAnti =>
        (output, output)
      case _ =>
        (output, (left.output ++ right.output).map(_.withNullability(true)))
    }
    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = UnsafeProjection.create(resultOutput, resultInput)
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  override def supportCodegen: Boolean = {
    BroadcastRangeJoinExec.supports(joinType, buildSide)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamed.asInstanceOf[CodegenSupport].inputRDDs()
  }

  private def multipleOutputForOneInput: Boolean = joinType match {
    case _: InnerLike | LeftOuter | RightOuter => true
    case _ => false
  }

  override def needCopyResult: Boolean = {
    streamed.asInstanceOf[CodegenSupport].needCopyResult || multipleOutputForOneInput
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamed.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case _: InnerLike => codegenInner(ctx, input)
      case LeftOuter | RightOuter => codegenOuter(ctx, input)
      case LeftSemi => codegenLeftExistence(ctx, input, exists = true)
      case LeftAnti => codegenLeftExistence(ctx, input, exists = false)
      case t =>
        throw SparkException.internalError(
          s"BroadcastRangeJoin code-gen should not take $t as the JoinType")
    }
  }

  private def prepareBroadcast(ctx: CodegenContext): String = {
    val broadcastRelation = broadcast.executeBroadcast[RangeRelation]()
    val broadcastTerm = ctx.addReferenceObj("broadcast", broadcastRelation)
    val clsName = RangeIndexProbeCodegen.indexClassName(probeKind.indexKind, usesLongKeys)
    ctx.addMutableState(clsName, "rangeRelation",
      v => s"$v = ($clsName) $broadcastTerm.value();", forceInline = true)
  }

  /**
   * Keys that map to one order-preserving long (`RangeIndex.longKey`) route
   * to the long indexes: build and probe keys then cross as raw longs, with
   * no boxing on either side, and every other supported key stays boxed.
   * One test picks both the `RangeRelationBroadcastMode` index family and
   * the generated probe code, so they cannot disagree.
   */
  private[this] lazy val usesLongKeys: Boolean = RangeIndex.canMapKeysToLong(buildKeys)

  /**
   * Shared init for every doConsume path: broadcasts the relation, evaluates the
   * streamed probe keys, and opens the candidate iterator over it. The index kind and
   * scan direction are fixed when the plan is built, so `RangeIndexProbeCodegen` generates
   * code that calls the concrete index method directly; see its documentation for how keys
   * are evaluated and how a null key becomes an empty scan.
   */
  private def prepareProbe(ctx: CodegenContext, input: Seq[ExprCode]): (String, String, String) = {
    val relationTerm = prepareBroadcast(ctx)
    val boundStreamedKeys = BindReferences.bindReferences(streamedKeys, streamed.output)
    val keys = RangeIndexProbeCodegen.genProbeKeys(ctx, input, boundStreamedKeys, usesLongKeys)
    val candidateExpr = RangeIndexProbeCodegen.genCandidateExpr(
      ctx, relationTerm, probeKind, usesLongKeys, keys)
    val matches = ctx.freshName("matches")
    val iterCls = classOf[Iterator[InternalRow]].getName
    val iteratorCode = s"$iterCls $matches = $candidateExpr;"
    (keys.code, iteratorCode, matches)
  }

  private def orderResultVars(
      streamVars: Seq[ExprCode],
      buildVars: Seq[ExprCode]): Seq[ExprCode] = {
    buildSide match {
      case BuildLeft => buildVars ++ streamVars
      case BuildRight => streamVars ++ buildVars
    }
  }

  private def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (keyCode, iteratorCode, matches) = prepareProbe(ctx, input)
    val (buildRow, checkCondition, buildVars) = getJoinCondition(ctx, input, streamed, broadcast)
    val resultVars = orderResultVars(input, buildVars)
    val numOutput = metricTerm(ctx, "numOutputRows")

    s"""
       |$keyCode
       |$iteratorCode
       |while ($matches.hasNext()) {
       |  InternalRow $buildRow = (InternalRow) $matches.next();
       |  $checkCondition {
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
     """.stripMargin
  }

  private def codegenOuter(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (keyCode, iteratorCode, matches) = prepareProbe(ctx, input)
    val buildRow = ctx.freshName("buildRow")
    val (_, checkCondition, _) = getJoinCondition(ctx, input, streamed, broadcast, Some(buildRow))
    val buildVars = genOneSideJoinVars(ctx, buildRow, broadcast, setDefaultValue = true)
    val resultVars = orderResultVars(input, buildVars)
    val shouldOutputRow = ctx.freshName("shouldOutputRow")
    val foundMatch = ctx.freshName("foundMatch")
    val numOutput = metricTerm(ctx, "numOutputRows")

    // One consume site, inside the loop. The last iteration pads the build side
    // when no candidate satisfied the original condition.
    s"""
       |$keyCode
       |$iteratorCode
       |boolean $foundMatch = false;
       |while ($matches.hasNext() || !$foundMatch) {
       |  InternalRow $buildRow = $matches.hasNext() ?
       |    (InternalRow) $matches.next() : null;
       |  boolean $shouldOutputRow = false;
       |  if ($buildRow != null) {
       |    $checkCondition {
       |      $shouldOutputRow = true;
       |      $foundMatch = true;
       |    }
       |  } else {
       |    $shouldOutputRow = true;
       |    $foundMatch = true;
       |  }
       |  if ($shouldOutputRow) {
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
     """.stripMargin
  }

  private def codegenLeftExistence(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      exists: Boolean): String = {
    val (keyCode, iteratorCode, matches) = prepareProbe(ctx, input)
    val (buildRow, checkCondition, _) =
      getJoinCondition(ctx, input, streamed, broadcast)
    val foundMatch = ctx.freshName("foundMatch")
    val numOutput = metricTerm(ctx, "numOutputRows")

    s"""
       |$keyCode
       |$iteratorCode
       |boolean $foundMatch = false;
       |while (!$foundMatch && $matches.hasNext()) {
       |  InternalRow $buildRow = (InternalRow) $matches.next();
       |  $checkCondition {
       |    $foundMatch = true;
       |  }
       |}
       |if ($foundMatch == $exists) {
       |  $numOutput.add(1);
       |  ${consume(ctx, input)}
       |}
     """.stripMargin
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)
}

object BroadcastRangeJoinExec {

  /**
   * Stream-preserving combinations. Left outer, left semi, and left anti broadcast the
   * right side. Right outer broadcasts the left side. Inner may broadcast either side.
   */
  def supports(joinType: JoinType, buildSide: BuildSide): Boolean = (joinType, buildSide) match {
    case (_: InnerLike, _) => true
    case (RightOuter, BuildLeft) => true
    case (LeftOuter | LeftSemi | LeftAnti, BuildRight) => true
    case _ => false
  }

  /**
   * Broadcast relation, bound probe keys, and the original condition for one
   * streamed partition. The joined row is reused; the caller copies it. Nested here
   * (not inside [[BroadcastRangeJoinExec]]) so constructing it does not implicitly
   * capture the enclosing plan node through a synthetic outer reference.
   */
  private class ProbeContext(
      val stream: Iterator[InternalRow],
      val condition: InternalRow => Boolean,
      joinedRow: JoinedRow,
      val nullBuild: InternalRow,
      candidateFn: InternalRow => Iterator[InternalRow],
      buildSide: BuildSide) {

    def candidates(row: InternalRow): Iterator[InternalRow] = candidateFn(row)

    def join(streamRow: InternalRow, buildRow: InternalRow): InternalRow = buildSide match {
      case BuildRight => joinedRow(streamRow, buildRow)
      case BuildLeft => joinedRow(buildRow, streamRow)
    }
  }
}
