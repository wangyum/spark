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

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray

private[joins] class RangeIndexMemoryConsumer(taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) {
  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
}

private[joins] object RangeGroupProbe {

  final class Spec(
      val mode: RangeRelationBroadcastMode,
      val boundStreamKeys: Seq[Expression],
      val pointScan: (PointScan, Boolean),
      val threshold: Int,
      val maxRows: Int,
      val maxBytes: Long) extends Serializable

  final class Handle private[RangeGroupProbe] (
      val relation: RangeRelation,
      val allocatedBytes: Long,
      val candidateFn: InternalRow => Iterator[InternalRow]) {

    def candidates(streamedRow: InternalRow): Iterator[InternalRow] = candidateFn(streamedRow)
  }

  def build(
      matches: ExternalAppendOnlyUnsafeRowArray,
      spec: Spec,
      taskMemoryManager: TaskMemoryManager,
      consumer: RangeIndexMemoryConsumer): Handle = {
    val n = matches.length
    if (n <= spec.threshold || n > spec.maxRows || matches.isSpillBacked) {
      return null
    }

    val bytesPerRow = spec.mode.indexKind match {
      case IntervalIndexKind => 64L
      case PointIndexKind => 32L
    }
    val requiredBytes = n * bytesPerRow
    if (requiredBytes > spec.maxBytes) {
      return null
    }

    val acquired = if (taskMemoryManager != null && consumer != null) {
      taskMemoryManager.acquireExecutionMemory(requiredBytes, consumer)
    } else {
      requiredBytes
    }

    if (acquired < requiredBytes) {
      if (taskMemoryManager != null && consumer != null && acquired > 0) {
        taskMemoryManager.releaseExecutionMemory(acquired, consumer)
      }
      return null
    }

    val relation = spec.mode.transform(matches.generateIterator(), Some(n))
    val candidateFn = RangeRelationProbe.makeCandidateFn(
      relation, spec.boundStreamKeys, spec.pointScan)
    new Handle(relation, acquired, candidateFn)
  }

  def release(
      handle: Handle,
      taskMemoryManager: TaskMemoryManager,
      consumer: RangeIndexMemoryConsumer): Unit = {
    if (handle != null && taskMemoryManager != null &&
        consumer != null && handle.allocatedBytes > 0) {
      taskMemoryManager.releaseExecutionMemory(handle.allocatedBytes, consumer)
    }
  }

  def candidates(
      handle: Handle,
      matches: ExternalAppendOnlyUnsafeRowArray,
      streamedRow: InternalRow): Iterator[InternalRow] = {
    if (handle != null) {
      handle.candidates(streamedRow)
    } else {
      matches.generateIterator()
    }
  }
}
