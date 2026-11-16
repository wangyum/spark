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

import java.io.Serializable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray

/**
 * Per equi-key group range probe: reuse broadcast range join materialization at key-group
 * granularity.
 */
private[joins] object RangeGroupProbe {

  final class Spec(
      val mode: RangeRelationBroadcastMode,
      val boundStreamKeys: Seq[Expression],
      val pointScan: (PointScan, Boolean),
      val threshold: Int) extends Serializable

  final class Handle private[RangeGroupProbe] (
      val relation: Option[RangeRelation],
      val matches: ExternalAppendOnlyUnsafeRowArray,
      val candidateFn: Option[InternalRow => Iterator[InternalRow]])

  def build(matches: ExternalAppendOnlyUnsafeRowArray, spec: Spec): Handle = {
    val n = matches.length
    // A spilled group already proved too large to hold safely in memory; indexing it would
    // re-materialize the whole group a second time as untracked JVM heap, defeating the spill.
    if (n <= spec.threshold || matches.isSpillBacked) {
      new Handle(None, matches, None)
    } else {
      val relation = spec.mode.transform(matches.generateIterator(), Some(n))
      val candidateFn = RangeRelationProbe.makeCandidateFn(
        relation, spec.boundStreamKeys, spec.pointScan)
      new Handle(Some(relation), matches, Some(candidateFn))
    }
  }

  def candidates(handle: Handle, streamedRow: InternalRow): Iterator[InternalRow] = {
    handle.candidateFn match {
      case Some(probe) => probe(streamedRow)
      case None => handle.matches.generateIterator()
    }
  }
}