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

import org.apache.spark.TaskContext
import org.apache.spark.memory.{MemoryConsumer, SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock

/**
 * Append-only storage for full [[UnsafeRow]]s in Tungsten pages tracked by [[TaskMemoryManager]],
 * so retaining a row beyond the lifetime of the (reused, mutable) row object its source iterator
 * handed out doesn't require a plain JVM-heap `UnsafeRow.copy()` -- unlike a heap copy, this
 * memory is accounted for by Spark's execution-memory bookkeeping and triggers the same
 * backpressure/OOM signal (`SparkOutOfMemoryError`) as every other Tungsten-managed structure.
 *
 * Mirrors [[org.apache.spark.sql.execution.joins.HashedRelation.LongToUnsafeRowMap]]'s row-storage
 * mechanics exactly (single growable page, page-relative offset+size packed into one `Long`,
 * doubling growth that copies existing bytes into a bigger page and frees the old one) minus the
 * key-hashing half of that class, which this doesn't need: the caller already has its own index
 * (here, [[HashRowNumberGroupLimitAccumulator]]'s
 * `org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap`) and only needs a place to put
 * the row bytes themselves plus a stable pointer back to them.
 *
 * Entries are never removed or updated in place -- appending a replacement for a row whose address
 * is no longer referenced simply leaves the old bytes as dead space in the page until the whole
 * arena is freed. This mirrors exactly how the previous JVM-heap `ArrayBuffer[UnsafeRow]` this
 * replaces already behaved (a replaced row's old copy became heap garbage); the difference is that
 * garbage here is accounted Tungsten memory, not unaccounted JVM heap.
 *
 * `linked` fixes, for this instance's lifetime, whether entries carry an extra trailing 8-byte
 * "next" pointer ([[appendLinked]]/[[getNext]]) for threading a singly-linked candidate list per
 * key -- used by [[HashRankGroupLimitAccumulator]] to chain rows tied at the same order-key value.
 * [[HashRowNumberGroupLimitAccumulator]] has no use for that link (at most one row survives per
 * heap slot), so it uses the plain [[append]]/[[getRow]] pair instead and never pays the extra 8
 * bytes per row. Mixing the two modes on one instance is a programming error, not a data-dependent
 * condition, so [[append]]/[[appendLinked]]/[[getNext]]/[[getRowAndNext]] fail fast (`require`) if
 * called on an instance constructed for the other mode.
 */
private[window] final class UnsafeRowArena(taskMemoryManager: TaskMemoryManager, linked: Boolean)
  extends MemoryConsumer(
    taskMemoryManager,
    UnsafeRowArena.INITIAL_PAGE_SIZE_BYTES,
    taskMemoryManager.getTungstenMemoryMode) {

  private val pageHeaderBytes = TaskMemoryManager.growingPageHeaderBytes(
    taskMemoryManager.pageSizeBytes())
  private var page: MemoryBlock =
    allocatePage(UnsafeRowArena.INITIAL_PAGE_SIZE_BYTES - pageHeaderBytes)
  private var pageBaseObj: AnyRef = page.getBaseObject
  private var pageBaseOff: Long = page.getBaseOffset
  private var cursor: Long = pageBaseOff
  private var pageEnd: Long = pageBaseOff + page.size()

  // Mirrors UnsafeFixedWidthAggregationMap's own defensive registration: guarantees the arena's
  // pages are freed at task end even if a caller (e.g. a downstream LIMIT) never fully drains the
  // iterator that references them.
  TaskContext.get().addTaskCompletionListener[Unit](_ => free())

  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  /**
   * Appends `row`'s bytes and returns a stable address usable with [[getRow]] for the remaining
   * lifetime of this arena, or [[UnsafeRowArena.NO_NEXT]] if `row` could not be appended -- either
   * because no more execution memory is available, because growing the current page further would
   * exceed `TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES` (see [[grow]]), or because `row` itself is
   * too large for this arena's address packing to represent (see [[UnsafeRowArena.SIZE_BITS]]).
   * Callers that want to degrade gracefully (as [[HashRowNumberGroupLimitAccumulator]] does) check
   * for this sentinel return rather than catching an exception, avoiding exception-handling
   * overhead on this per-row path. Requires `linked = false` (`require`, see the class doc).
   */
  def append(row: UnsafeRow): Long = {
    require(!linked, "append() is not supported on an arena constructed for linked mode.")
    val sizeInBytes = row.getSizeInBytes
    if (sizeInBytes >= (1 << UnsafeRowArena.SIZE_BITS)) {
      return UnsafeRowArena.NO_NEXT
    }
    if (cursor + sizeInBytes > pageEnd) {
      try {
        grow(sizeInBytes)
      } catch {
        case _: SparkOutOfMemoryError =>
          return UnsafeRowArena.NO_NEXT
      }
    }
    val offset = cursor
    Platform.copyMemory(
      row.getBaseObject, row.getBaseOffset, pageBaseObj, cursor, sizeInBytes)
    cursor += sizeInBytes
    UnsafeRowArena.toAddress(offset - pageBaseOff, sizeInBytes)
  }

  /**
   * Like [[append]] (including its [[UnsafeRowArena.NO_NEXT]] failure-sentinel contract), but also
   * stores `next` as a trailing 8-byte link readable via [[getNext]] or [[getRowAndNext]],
   * threading this entry into a singly-linked list of rows tied at the same order-key value (see
   * the class doc). Requires `linked = true` (`require`, see the class doc).
   */
  def appendLinked(row: UnsafeRow, next: Long): Long = {
    require(linked, "appendLinked() is not supported on an arena constructed for non-linked mode.")
    val sizeInBytes = row.getSizeInBytes
    if (sizeInBytes >= (1 << UnsafeRowArena.SIZE_BITS)) {
      return UnsafeRowArena.NO_NEXT
    }
    val totalSize = sizeInBytes + java.lang.Long.BYTES
    if (cursor + totalSize > pageEnd) {
      try {
        grow(totalSize)
      } catch {
        case _: SparkOutOfMemoryError =>
          return UnsafeRowArena.NO_NEXT
      }
    }
    val offset = cursor
    Platform.copyMemory(
      row.getBaseObject, row.getBaseOffset, pageBaseObj, cursor, sizeInBytes)
    cursor += sizeInBytes
    Platform.putLong(pageBaseObj, cursor, next)
    cursor += java.lang.Long.BYTES
    UnsafeRowArena.toAddress(offset - pageBaseOff, sizeInBytes)
  }

  /** Points `resultRow` (reused across calls, like other Tungsten row iterators) at `address`. */
  def getRow(address: Long, resultRow: UnsafeRow): UnsafeRow = {
    resultRow.pointTo(
      pageBaseObj,
      pageBaseOff + UnsafeRowArena.toOffset(address),
      UnsafeRowArena.toSize(address))
    resultRow
  }

  /** The `next` link stored alongside `address` by [[appendLinked]]. Requires `linked = true`. */
  def getNext(address: Long): Long = {
    require(linked, "getNext() is not supported on an arena constructed for non-linked mode.")
    val rowEnd =
      pageBaseOff + UnsafeRowArena.toOffset(address) + UnsafeRowArena.toSize(address)
    Platform.getLong(pageBaseObj, rowEnd)
  }

  /**
   * Points `resultRow` at `address` and returns the trailing `next` link stored alongside it,
   * combining [[getRow]] and [[getNext]] in a single address decode. Requires `linked = true`.
   */
  def getRowAndNext(address: Long, resultRow: UnsafeRow): Long = {
    require(linked, "getRowAndNext() is not supported on an arena constructed for non-linked mode.")
    val base = pageBaseOff + UnsafeRowArena.toOffset(address)
    val size = UnsafeRowArena.toSize(address)
    resultRow.pointTo(pageBaseObj, base, size)
    Platform.getLong(pageBaseObj, base + size)
  }

  /** Current bytes held (not a high-water mark; callers combine with their own peak tracking). */
  def bytesUsed: Long = getUsed

  /**
   * Grows the current page to fit `inputRowSize` more bytes, doubling (at minimum) like
   * [[org.apache.spark.sql.execution.joins.HashedRelation.LongToUnsafeRowMap.grow]] -- only the
   * doubling strategy is shared, not the cap below: that sibling instead caps at a lower, fixed
   * 8 GB (`1 << 30` words) and throws a plain, non-degradable `SparkUnsupportedOperationException`
   * via `QueryExecutionErrors.cannotBuildHashedRelationLargerThan8GError`, since a hash join has no
   * fallback for an oversized build side. This arena caps at `TaskMemoryManager
   * .MAXIMUM_PAGE_SIZE_BYTES` (~17.18 GB) instead, because that is the limit
   * `TaskMemoryManager.allocatePage` itself enforces unconditionally -- i.e. independent of
   * whether memory is actually under pressure -- throwing the uncatchable, unaccounted
   * `TooLargePageException` rather than `SparkOutOfMemoryError` if asked for more. Checking the
   * cap here first lets a single task's accumulated state exceed that limit degrade through the
   * normal `SparkOutOfMemoryError` fallback path instead of crashing the task.
   */
  private def grow(inputRowSize: Int): Unit = {
    val usedBytes = cursor - pageBaseOff
    val neededNumWords = (usedBytes + inputRowSize + 7) / 8
    if (neededNumWords > page.size() / 8) {
      val maxNumWords = TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES / 8
      val newNumWords = UnsafeRowArena.computeNewPageWords(
        neededNumWords, page.size(), pageHeaderBytes, maxNumWords).getOrElse {
        // No bytes were ever requested from/granted by the allocator -- this is a pre-check that
        // fires before allocatePage would even be attempted -- so receivedBytes is 0, matching
        // WindowSegmentTree's use of this same error for its own failed-acquisition case.
        // scalastyle:off throwerror
        throw QueryExecutionErrors.cannotAcquireMemoryForWindowExecutionError(
          neededNumWords * 8L, 0L, taskMemoryManager.getMemoryConsumptionBreakdown())
        // scalastyle:on throwerror
      }
      val newPageSizeBytes = newNumWords * 8L
      val newPage = allocatePage(newPageSizeBytes)
      Platform.copyMemory(
        pageBaseObj, pageBaseOff, newPage.getBaseObject, newPage.getBaseOffset,
        usedBytes)
      freePage(page)
      page = newPage
      pageBaseObj = page.getBaseObject
      pageBaseOff = page.getBaseOffset
      cursor = pageBaseOff + usedBytes
      pageEnd = pageBaseOff + page.size()
    }
  }

  /** Idempotent; safe to call even though the `TaskCompletionListener` above will also call it. */
  def free(): Unit = {
    if (page != null) {
      freePage(page)
      page = null
      pageBaseObj = null
      pageBaseOff = 0L
      pageEnd = 0L
    }
  }
}

private[window] object UnsafeRowArena {
  /** Sentinel terminating a linked candidate list built via [[UnsafeRowArena.appendLinked]]. */
  val NO_NEXT: Long = -1L

  private val INITIAL_PAGE_SIZE_BYTES: Long = 1L << 16 // 64 KiB

  // Matches LongToUnsafeRowMap's split: 28 bits of size (up to ~256 MB per row, far beyond any
  // realistic single row) leaves 36 bits of page-relative offset (up to 64 GB of cumulative
  // storage), ample since the arena is a single page whose total size is independently capped by
  // grow()'s own check against TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES (~17.18 GB, see grow's
  // doc) -- a bound that holds regardless of how many rows a given key accumulates, including the
  // K-slot (ROW_NUMBER) and unbounded-per-key linked (RANK/DENSE_RANK) usages of this class.
  private val SIZE_BITS = 28
  private val SIZE_MASK = 0xfffffff

  private def toAddress(offset: Long, size: Int): Long = (offset << SIZE_BITS) | size
  private def toOffset(address: Long): Long = address >>> SIZE_BITS
  private def toSize(address: Long): Int = (address & SIZE_MASK).toInt

  /**
   * The page-growth arithmetic [[UnsafeRowArena.grow]] uses, pulled out as a pure function (no
   * page allocation) so it can be unit-tested against the `maxNumWords` boundary directly instead
   * of needing to actually grow a page to the real ~17.18 GB `TaskMemoryManager
   * .MAXIMUM_PAGE_SIZE_BYTES` cap. Returns `None` if `neededNumWords` itself exceeds `maxNumWords`
   * (irrecoverable -- not even a page of exactly the needed size would fit), or `Some(newNumWords)`
   * otherwise: the doubling target clamped to `maxNumWords` before being combined with
   * `neededNumWords` via `max`, so a request that genuinely fits under the cap is never rejected
   * just because doubling the current page would overshoot it (matches `LongToUnsafeRowMap
   * .grow`'s identical clamp).
   */
  private[window] def computeNewPageWords(
      neededNumWords: Long,
      currentPageSizeBytes: Long,
      pageHeaderBytes: Long,
      maxNumWords: Long): Option[Long] = {
    if (neededNumWords > maxNumWords) {
      None
    } else {
      val doubledNumWords = ((currentPageSizeBytes + pageHeaderBytes) * 2 - pageHeaderBytes) / 8
      Some(math.max(neededNumWords, math.min(doubledNumWords, maxNumWords)))
    }
  }
}
