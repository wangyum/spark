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
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
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
 * condition, so [[append]]/[[appendLinked]]/[[getNext]] fail fast (`require`) if called on an
 * instance constructed for the other mode.
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
  private var cursor: Long = page.getBaseOffset

  // Mirrors UnsafeFixedWidthAggregationMap's own defensive registration: guarantees the arena's
  // pages are freed at task end even if a caller (e.g. a downstream LIMIT) never fully drains the
  // iterator that references them.
  TaskContext.get().addTaskCompletionListener[Unit](_ => free())

  override def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  /**
   * Appends `row`'s bytes and returns a stable address usable with [[getRow]] for the remaining
   * lifetime of this arena. Throws `org.apache.spark.memory.SparkOutOfMemoryError` -- either
   * because no more execution memory is available, or because growing the current page further
   * would exceed `TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES` (see [[grow]]) -- or, if `row` itself
   * is too large for this arena's address packing to represent (see [[UnsafeRowArena.SIZE_BITS]]),
   * `SparkUnsupportedOperationException` from
   * [[org.apache.spark.sql.errors.QueryExecutionErrors.rowLargerThan256MUnsupportedError]], the
   * same error [[org.apache.spark.sql.execution.joins.HashedRelation.LongToUnsafeRowMap.append]]
   * raises for the identical bound. Callers that want to degrade gracefully (as
   * [[HashRowNumberGroupLimitAccumulator]] does) must catch both themselves.
   */
  def append(row: UnsafeRow): Long = {
    require(!linked, "append() is not supported on an arena constructed for linked mode; use" +
      " appendLinked() instead.")
    appendInternal(row, next = None)
  }

  /**
   * Like [[append]], but also stores `next` as a trailing 8-byte link readable via [[getNext]],
   * threading this entry into a singly-linked list of rows tied at the same order-key value (see
   * the class doc). Only valid on an arena constructed with `linked = true`.
   */
  def appendLinked(row: UnsafeRow, next: Long): Long = {
    require(linked, "appendLinked() is not supported on an arena constructed for non-linked" +
      " mode; use append() instead.")
    appendInternal(row, next = Some(next))
  }

  private def appendInternal(row: UnsafeRow, next: Option[Long]): Long = {
    val sizeInBytes = row.getSizeInBytes
    if (sizeInBytes >= (1 << UnsafeRowArena.SIZE_BITS)) {
      throw QueryExecutionErrors.rowLargerThan256MUnsupportedError()
    }
    val linkBytes = if (next.isDefined) java.lang.Long.BYTES else 0
    grow(sizeInBytes + linkBytes)
    val offset = cursor
    Platform.copyMemory(
      row.getBaseObject, row.getBaseOffset, page.getBaseObject, cursor, sizeInBytes)
    cursor += sizeInBytes
    next.foreach { nextAddress =>
      Platform.putLong(page.getBaseObject, cursor, nextAddress)
      cursor += linkBytes
    }
    UnsafeRowArena.toAddress(offset - page.getBaseOffset, sizeInBytes)
  }

  /** Points `resultRow` (reused across calls, like other Tungsten row iterators) at `address`. */
  def getRow(address: Long, resultRow: UnsafeRow): UnsafeRow = {
    resultRow.pointTo(
      page.getBaseObject,
      page.getBaseOffset + UnsafeRowArena.toOffset(address),
      UnsafeRowArena.toSize(address))
    resultRow
  }

  /** The `next` link stored alongside `address` by [[appendLinked]]. Requires `linked = true`. */
  def getNext(address: Long): Long = {
    require(linked, "getNext() is not supported on an arena constructed for non-linked mode.")
    val rowEnd =
      page.getBaseOffset + UnsafeRowArena.toOffset(address) + UnsafeRowArena.toSize(address)
    Platform.getLong(page.getBaseObject, rowEnd)
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
    val usedBytes = cursor - page.getBaseOffset
    val neededNumWords = (usedBytes + inputRowSize + 7) / 8
    if (neededNumWords > page.size() / 8) {
      val doubledNumWords = ((page.size() + pageHeaderBytes) * 2 - pageHeaderBytes) / 8
      val newNumWords = math.max(neededNumWords, doubledNumWords)
      val newPageSizeBytes = newNumWords * 8L
      if (newPageSizeBytes > TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES) {
        // No bytes were ever requested from/granted by the allocator -- this is a pre-check that
        // fires before allocatePage would even be attempted -- so receivedBytes is 0, matching
        // WindowSegmentTree's use of this same error for its own failed-acquisition case.
        // scalastyle:off throwerror
        throw QueryExecutionErrors.cannotAcquireMemoryForWindowAggregateError(
          newPageSizeBytes, 0L, taskMemoryManager.getMemoryConsumptionBreakdown())
        // scalastyle:on throwerror
      }
      val newPage = allocatePage(newPageSizeBytes)
      Platform.copyMemory(
        page.getBaseObject, page.getBaseOffset, newPage.getBaseObject, newPage.getBaseOffset,
        usedBytes)
      freePage(page)
      page = newPage
      cursor = page.getBaseOffset + usedBytes
    }
  }

  /** Idempotent; safe to call even though the `TaskCompletionListener` above will also call it. */
  def free(): Unit = {
    if (page != null) {
      freePage(page)
      page = null
    }
  }
}

private[window] object UnsafeRowArena {
  /** Sentinel terminating a linked candidate list built via [[UnsafeRowArena.appendLinked]]. */
  val NO_NEXT: Long = -1L

  private val INITIAL_PAGE_SIZE_BYTES: Long = 1L << 16 // 64 KiB

  // Matches LongToUnsafeRowMap's split: 28 bits of size (up to ~256 MB per row, far beyond any
  // realistic single row) leaves 36 bits of page-relative offset (up to 64 GB of cumulative
  // storage), ample since this arena only ever holds one row per distinct key at a time.
  private val SIZE_BITS = 28
  private val SIZE_MASK = 0xfffffff

  private def toAddress(offset: Long, size: Int): Long = (offset << SIZE_BITS) | size
  private def toOffset(address: Long): Long = address >>> SIZE_BITS
  private def toSize(address: Long): Int = (address & SIZE_MASK).toInt
}
