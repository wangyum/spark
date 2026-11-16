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
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeRow}
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.types.DataType

/**
 * Zero-copy flat point index for a single equi-key buffered group in SortMergeJoin.
 * Holds sorted scalar keys and references to buffered rows directly without duplicating UnsafeRows.
 */
private[joins] final class CompactLongPointIndex(
    private val keys: Array[Long],
    private val rows: Array[UnsafeRow],
    val length: Int) {

  def getRow(i: Int): UnsafeRow = rows(i)

  def upTo(value: Long, strict: Boolean): Iterator[InternalRow] = {
    val end = searchBound(value, skipEquals = !strict)
    new RowSlice(rows.asInstanceOf[Array[InternalRow]], 0, end)
  }

  def from(value: Long, strict: Boolean): Iterator[InternalRow] = {
    val start = searchBound(value, skipEquals = strict)
    new RowSlice(rows.asInstanceOf[Array[InternalRow]], start, length)
  }

  def between(low: Long, high: Long): Iterator[InternalRow] = {
    val start = searchBound(low, skipEquals = false)
    val end = searchBound(high, skipEquals = true)
    if (start < end) {
      new RowSlice(rows.asInstanceOf[Array[InternalRow]], start, end)
    } else {
      Iterator.empty
    }
  }

  def searchBound(value: Long, skipEquals: Boolean): Int = {
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = java.lang.Long.compare(keys(mid), value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }
}

private[joins] object CompactLongPointIndex {

  def sort3Way(keys: Array[Long], rows: Array[UnsafeRow], lo: Int, hi: Int): Unit = {
    if (hi <= lo) return
    if (hi - lo < 16) {
      var i = lo + 1
      while (i <= hi) {
        val key = keys(i)
        val row = rows(i)
        var j = i - 1
        while (j >= lo && keys(j) > key) {
          keys(j + 1) = keys(j)
          rows(j + 1) = rows(j)
          j -= 1
        }
        keys(j + 1) = key
        rows(j + 1) = row
        i += 1
      }
      return
    }

    val mid = (lo + hi) >>> 1
    swap(keys, rows, lo, mid)
    val pivot = keys(lo)

    var lt = lo
    var gt = hi
    var i = lo + 1
    while (i <= gt) {
      val cmp = java.lang.Long.compare(keys(i), pivot)
      if (cmp < 0) {
        swap(keys, rows, lt, i)
        lt += 1
        i += 1
      } else if (cmp > 0) {
        swap(keys, rows, i, gt)
        gt -= 1
      } else {
        i += 1
      }
    }
    sort3Way(keys, rows, lo, lt - 1)
    sort3Way(keys, rows, gt + 1, hi)
  }

  private def swap(keys: Array[Long], rows: Array[UnsafeRow], i: Int, j: Int): Unit = {
    val tempK = keys(i)
    keys(i) = keys(j)
    keys(j) = tempK

    val tempR = rows(i)
    rows(i) = rows(j)
    rows(j) = tempR
  }
}

private[joins] class RangeIndexMemoryConsumer(taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) {
  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
}

private[joins] sealed abstract class CompactPointScan
private[joins] object CompactPointScan {
  case object UpTo extends CompactPointScan
  case object From extends CompactPointScan
  case object Between extends CompactPointScan
}

private[joins] object RangeGroupProbe {

  final class Spec(
      val boundBufferKey: Expression,
      val boundStreamKeys: Seq[Expression],
      val keyType: DataType,
      val pointScan: CompactPointScan,
      val strict: Boolean,
      val threshold: Int,
      val maxRows: Int,
      val maxBytes: Long) extends Serializable {

    @transient lazy val toLong: Any => Long = RangeIndex.requiredLongKey(keyType).value
  }

  final class Handle private[RangeGroupProbe] (
      val index: CompactLongPointIndex,
      val allocatedBytes: Long,
      val candidateFn: InternalRow => Iterator[InternalRow]) {

    def candidates(streamedRow: InternalRow): Iterator[InternalRow] = candidateFn(streamedRow)
  }

  def build(
      matches: ExternalAppendOnlyUnsafeRowArray,
      spec: Spec,
      taskMemoryManager: TaskMemoryManager,
      consumer: MemoryConsumer): Handle = {
    val n = matches.length
    if (n <= spec.threshold || n > spec.maxRows || matches.isSpillBacked) {
      return null
    }

    val requiredBytes = n * 16L
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

    val length = n.toInt
    val keys = new Array[Long](length)
    val rows = new Array[UnsafeRow](length)
    val iter = matches.generateIterator()
    var i = 0
    val toLong = spec.toLong
    val keyExpr = spec.boundBufferKey

    while (iter.hasNext && i < length) {
      val r = iter.next()
      val raw = keyExpr.eval(r)
      if (raw != null) {
        keys(i) = toLong(raw)
        rows(i) = r
        i += 1
      }
    }
    val validCount = i
    if (validCount > 1) {
      CompactLongPointIndex.sort3Way(keys, rows, 0, validCount - 1)
    }
    val index = new CompactLongPointIndex(keys, rows, validCount)

    val candidateFn: InternalRow => Iterator[InternalRow] = spec.pointScan match {
      case CompactPointScan.UpTo =>
        val probeExpr = spec.boundStreamKeys.head
        row =>
          val raw = probeExpr.eval(row)
          if (raw == null) Iterator.empty else index.upTo(toLong(raw), spec.strict)

      case CompactPointScan.From =>
        val probeExpr = spec.boundStreamKeys.head
        row =>
          val raw = probeExpr.eval(row)
          if (raw == null) Iterator.empty else index.from(toLong(raw), spec.strict)

      case CompactPointScan.Between =>
        val loExpr = spec.boundStreamKeys(0)
        val hiExpr = spec.boundStreamKeys(1)
        row =>
          val rawLo = loExpr.eval(row)
          val rawHi = hiExpr.eval(row)
          if (rawLo == null || rawHi == null) Iterator.empty
          else index.between(toLong(rawLo), toLong(rawHi))
    }

    new Handle(index, acquired, candidateFn)
  }

  def release(
      handle: Handle,
      taskMemoryManager: TaskMemoryManager,
      consumer: MemoryConsumer): Unit = {
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
