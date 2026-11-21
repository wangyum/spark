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

package org.apache.spark.sql.execution

import java.util
import java.util.ConcurrentModificationException

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.collection.unsafe.sort.{UnsafeSorterIterator, UnsafeSorterSpillReader}

class ExternalAppendOnlyUnsafeRowArraySuite extends SparkFunSuite with LocalSparkContext {
  private val random = new java.util.Random()

  private def withExternalArray(inMemoryThreshold: Int, spillThreshold: Int)
                               (f: ExternalAppendOnlyUnsafeRowArray => Unit): Unit = {
    sc = new SparkContext("local", "test", new SparkConf(false))

    val taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)

    try {
      val array = new ExternalAppendOnlyUnsafeRowArray(
        taskContext.taskMemoryManager(),
        SparkEnv.get.blockManager,
        SparkEnv.get.serializerManager,
        taskContext,
        1024,
        SparkEnv.get.memoryManager.pageSizeBytes,
        inMemoryThreshold,
        Long.MaxValue,
        spillThreshold,
        Long.MaxValue)
      try f(array) finally {
        array.clear()
      }
    } finally {
      TaskContext.unset()
    }
  }

  private def insertRow(array: ExternalAppendOnlyUnsafeRowArray): Long = {
    val valueInserted = random.nextLong()

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](64), 16)
    row.setLong(0, valueInserted)
    array.add(row)
    valueInserted
  }

  private def checkIfValueExists(iterator: Iterator[UnsafeRow], expectedValue: Long): Unit = {
    assert(iterator.hasNext)
    val actualRow = iterator.next()
    assert(actualRow.getLong(0) == expectedValue)
    assert(actualRow.getSizeInBytes == 16)
  }

  private def validateData(
      array: ExternalAppendOnlyUnsafeRowArray,
      expectedValues: ArrayBuffer[Long]): Iterator[UnsafeRow] = {
    val iterator = array.generateIterator()
    for (value <- expectedValues) {
      checkIfValueExists(iterator, value)
    }

    assert(!iterator.hasNext)
    iterator
  }

  private def populateRows(
      array: ExternalAppendOnlyUnsafeRowArray,
      numRowsToBePopulated: Int): ArrayBuffer[Long] = {
    val populatedValues = new ArrayBuffer[Long]
    populateRows(array, numRowsToBePopulated, populatedValues)
  }

  private def populateRows(
      array: ExternalAppendOnlyUnsafeRowArray,
      numRowsToBePopulated: Int,
      populatedValues: ArrayBuffer[Long]): ArrayBuffer[Long] = {
    for (_ <- 0 until numRowsToBePopulated) {
      populatedValues.append(insertRow(array))
    }
    populatedValues
  }

  private def getNumBytesSpilled: Long = {
    TaskContext.get().taskMetrics().memoryBytesSpilled
  }

  private def assertNoSpill(): Unit = {
    assert(getNumBytesSpilled == 0)
  }

  private def assertSpill(): Unit = {
    assert(getNumBytesSpilled > 0)
  }

  test("length is tracked as a Long") {
    // `length` must return `Long` so a single partition can hold more than Int.MaxValue rows
    // without the counter overflowing (window execution relies on this to detect oversized
    // partitions). Assert the type directly: a plain value check would pass even if `length`
    // reverted to `Int`, because small values widen implicitly.
    assert(classOf[ExternalAppendOnlyUnsafeRowArray].getMethod("length").getReturnType ===
      java.lang.Long.TYPE)
  }

  test("in-memory rows are defensively copied, independent of the caller's row object") {
    // A consumer of the in-memory (non-spill-backed) iterator - e.g. a future optimization that
    // skips its own extra UnsafeRow.copy() because it trusts this array to have already handed
    // it an independent row - depends on add() never aliasing the caller's own row object. If
    // add() ever stopped defensively copying, such a consumer would silently retain whatever
    // mutable row the caller happens to still be writing into afterward.
    withExternalArray(inMemoryThreshold = 10, spillThreshold = 100) { array =>
      val row = new UnsafeRow(1)
      row.pointTo(new Array[Byte](64), 16)
      row.setLong(0, 111L)
      array.add(row)

      // Mutate the caller's row object after add() returns.
      row.setLong(0, 999L)

      val iterator = array.generateIterator()
      assert(iterator.hasNext)
      assert(iterator.next().getLong(0) == 111L,
        "a row added to the in-memory buffer must be an independent copy, unaffected by later " +
          "mutation of the caller's original row object")
    }
  }

  test("in-memory rows are distinct objects, never a single buffer reused across next() calls") {
    // Same motivation as above: a consumer that wants to hold onto every row from a fully-
    // drained in-memory iterator - instead of defensively re-copying each one, which is only
    // required under the spill-backed contract (see `isSpillBacked`'s scaladoc) - depends on
    // each row being independently owned, not an alias of one shared, mutated-in-place buffer.
    withExternalArray(inMemoryThreshold = 10, spillThreshold = 100) { array =>
      val expectedValues = populateRows(array, 5)
      assert(!array.isSpillBacked, "test setup must keep this array in-memory")

      // Materialize every row before inspecting any of them: if the iterator ever re-pointed a
      // single shared UnsafeRow on each next() call, every entry below would alias the same
      // object and all read back as the last row's value.
      val rows = array.generateIterator().toList
      assert(rows.length == expectedValues.length)
      rows.zip(expectedValues).foreach { case (row, expected) =>
        assert(row.getLong(0) == expected,
          "each row read from a fully-drained in-memory iterator must still report the value " +
            "it had when added, not a later row's value from a shared buffer")
      }
      for (i <- rows.indices; j <- rows.indices if i != j) {
        assert(rows(i) ne rows(j),
          "every row from the in-memory iterator must be a distinct object, not the same " +
            "buffer re-pointed across next() calls")
      }
    }
  }

  test("bufferSize tracks added rows and resets on clear(), independent of a later " +
    "batch's row width") {
    // RangeGroupProbe.perRowKeyBytes divides bufferSize by the *current* group's row count to
    // get a boxed key's average row width, trusting that clear() (called by
    // SortMergeJoinExec.resetBufferedGroup between equi-key groups sharing one buffered array)
    // has reset this to exactly the new group's own bytes, with no carryover from a prior,
    // differently-sized group. Exercise that exact clear()-then-reuse pattern with rows of two
    // different widths, so a regression in either reset site (clear() or add()'s spill-switch
    // branch) would surface as a wrong total here instead of silently matching by coincidence.
    withExternalArray(inMemoryThreshold = 10, spillThreshold = 100) { array =>
      assert(array.bufferSize == 0, "a fresh array must report zero buffered bytes")

      val narrowRows = 5
      populateRows(array, narrowRows)
      assert(array.bufferSize == narrowRows * 16L,
        "bufferSize must equal the sum of each added row's own UnsafeRow.getSizeInBytes")

      array.clear()
      assert(array.bufferSize == 0, "clear() must reset the buffered byte count to zero")

      // A second, wider-row batch: if the reset above were ever skipped, this would report the
      // narrow batch's stale bytes still added on top, not just the wide batch's own.
      val wideRowBytes = 24
      val wideRows = 3
      (0 until wideRows).foreach { i =>
        val row = new UnsafeRow(2)
        row.pointTo(new Array[Byte](64), wideRowBytes)
        row.setLong(0, i.toLong)
        row.setLong(1, i.toLong)
        array.add(row)
      }
      assert(array.bufferSize == wideRows * wideRowBytes.toLong,
        "bufferSize after clear()-and-reuse must reflect only the new batch's rows, with no " +
          "carryover from the earlier, differently-sized batch")
    }
  }

  test("insert rows less than the inMemoryThreshold") {
    val (inMemoryThreshold, spillThreshold) = (100, 50)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      assert(array.isEmpty)

      val expectedValues = populateRows(array, 1)
      assert(!array.isEmpty)
      assert(array.length == 1)

      val iterator1 = validateData(array, expectedValues)

      // Add more rows (but not too many to trigger switch to [[UnsafeExternalSorter]])
      // Verify that NO spill has happened
      populateRows(array, inMemoryThreshold - 1, expectedValues)
      assert(array.length == inMemoryThreshold)
      assertNoSpill()

      val iterator2 = validateData(array, expectedValues)

      assert(!iterator1.hasNext)
      assert(!iterator2.hasNext)
    }
  }

  test("insert rows more than the inMemoryThreshold but less than spillThreshold") {
    val (inMemoryThreshold, spillThreshold) = (10, 50)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      assert(array.isEmpty)
      val expectedValues = populateRows(array, inMemoryThreshold - 1)
      assert(array.length == (inMemoryThreshold - 1))
      val iterator1 = validateData(array, expectedValues)
      assertNoSpill()

      // Add more rows to trigger switch to [[UnsafeExternalSorter]] but not too many to cause a
      // spill to happen. Verify that NO spill has happened
      populateRows(array, spillThreshold - expectedValues.length - 1, expectedValues)
      assert(array.length == spillThreshold - 1)
      assertNoSpill()

      val iterator2 = validateData(array, expectedValues)
      assert(!iterator2.hasNext)

      assert(!iterator1.hasNext)
      intercept[ConcurrentModificationException](iterator1.next())
      checkIteratorClosedWhenThrowConcurrentModificationException(iterator1)
    }
  }

  test("insert rows enough to force spill") {
    val (inMemoryThreshold, spillThreshold) = (20, 10)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      assert(array.isEmpty)
      val expectedValues = populateRows(array, inMemoryThreshold - 1)
      assert(array.length == (inMemoryThreshold - 1))
      val iterator1 = validateData(array, expectedValues)
      assertNoSpill()

      // Add more rows to trigger switch to [[UnsafeExternalSorter]] and cause a spill to happen.
      // Verify that spill has happened
      populateRows(array, 2, expectedValues)
      assert(array.length == inMemoryThreshold + 1)
      assertSpill()

      val iterator2 = validateData(array, expectedValues)
      assert(!iterator2.hasNext)

      assert(!iterator1.hasNext)
      intercept[ConcurrentModificationException](iterator1.next())
      checkIteratorClosedWhenThrowConcurrentModificationException(iterator1)
    }
  }

  test("iterator on an empty array should be empty") {
    withExternalArray(inMemoryThreshold = 4, spillThreshold = 10) { array =>
      val iterator = array.generateIterator()
      assert(array.isEmpty)
      assert(array.length == 0)
      assert(!iterator.hasNext)
    }
  }

  test("generate iterator with negative start index") {
    withExternalArray(inMemoryThreshold = 100, spillThreshold = 56) { array =>
      val exception =
        intercept[ArrayIndexOutOfBoundsException](array.generateIterator(startIndex = -10))

      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array")
      )
    }
  }

  test("generate iterator with start index exceeding array's size (without spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      populateRows(array, spillThreshold / 2)

      val exception =
        intercept[ArrayIndexOutOfBoundsException](
          array.generateIterator(startIndex = spillThreshold * 10))
      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array"))
    }
  }

  test("generate iterator with start index exceeding array's size (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      populateRows(array, spillThreshold * 2)

      val exception =
        intercept[ArrayIndexOutOfBoundsException](
          array.generateIterator(startIndex = spillThreshold * 10))

      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array"))
    }
  }

  test("generate iterator with custom start index (without spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      val expectedValues = populateRows(array, inMemoryThreshold)
      val startIndex = inMemoryThreshold / 2
      val iterator = array.generateIterator(startIndex = startIndex)
      for (i <- startIndex until expectedValues.length) {
        checkIfValueExists(iterator, expectedValues(i))
      }
    }
  }

  test("generate iterator with custom start index (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      val expectedValues = populateRows(array, spillThreshold * 10)
      val startIndex = spillThreshold * 2
      val iterator = array.generateIterator(startIndex = startIndex)
      for (i <- startIndex until expectedValues.length) {
        checkIfValueExists(iterator, expectedValues(i))
      }
    }
  }

  test("test iterator invalidation (without spill)") {
    withExternalArray(inMemoryThreshold = 10, spillThreshold = 100) { array =>
      // insert 2 rows, iterate until the first row
      populateRows(array, 2)

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      populateRows(array, 1)
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
      checkIteratorClosedWhenThrowConcurrentModificationException(iterator)

      // Clearing the array should also invalidate any old iterators
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
      checkIteratorClosedWhenThrowConcurrentModificationException(iterator)
    }
  }

  test("test iterator invalidation (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (2, 10)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      // Populate enough rows so that spill happens
      populateRows(array, spillThreshold * 2)
      assertSpill()

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      populateRows(array, 1)
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
      checkIteratorClosedWhenThrowConcurrentModificationException(iterator)

      // Clearing the array should also invalidate any old iterators
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
      checkIteratorClosedWhenThrowConcurrentModificationException(iterator)
    }
  }

  test("clear on an empty the array") {
    withExternalArray(inMemoryThreshold = 2, spillThreshold = 3) { array =>
      val iterator = array.generateIterator()
      assert(!iterator.hasNext)

      // multiple clear'ing should not have an side-effect
      array.clear()
      array.clear()
      array.clear()
      assert(array.isEmpty)
      assert(array.length == 0)

      // Clearing an empty array should also invalidate any old iterators
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
      checkIteratorClosedWhenThrowConcurrentModificationException(iterator)
    }
  }

  test("clear array (without spill)") {
    val (inMemoryThreshold, spillThreshold) = (10, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      // Populate rows ... but not enough to trigger spill
      populateRows(array, inMemoryThreshold / 2)
      assertNoSpill()

      // Clear the array
      array.clear()
      assert(array.isEmpty)

      // Re-populate few rows so that there is no spill
      // Verify the data. Verify that there was no spill
      val expectedValues = populateRows(array, inMemoryThreshold / 2)
      validateData(array, expectedValues)
      assertNoSpill()

      // Populate more rows .. enough to not trigger a spill.
      // Verify the data. Verify that there was no spill
      populateRows(array, inMemoryThreshold / 2, expectedValues)
      validateData(array, expectedValues)
      assertNoSpill()
    }
  }

  test("clear array (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (10, 20)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      // Populate enough rows to trigger spill
      populateRows(array, spillThreshold * 2)
      val bytesSpilled = getNumBytesSpilled
      assert(bytesSpilled > 0)

      // Clear the array
      array.clear()
      assert(array.isEmpty)

      // Re-populate the array ... but NOT upto the point that there is spill.
      // Verify data. Verify that there was NO "extra" spill
      val expectedValues = populateRows(array, spillThreshold / 2)
      validateData(array, expectedValues)
      assert(getNumBytesSpilled == bytesSpilled)

      // Populate more rows to trigger spill
      // Verify the data. Verify that there was "extra" spill
      populateRows(array, spillThreshold * 2, expectedValues)
      validateData(array, expectedValues)
      assert(getNumBytesSpilled > bytesSpilled)
    }
  }

  private def checkIteratorClosedWhenThrowConcurrentModificationException(
      iterator: Iterator[UnsafeRow]): Unit = {
    def getFieldValue(obj: Any, fieldName: String): Any = {
      val field = obj.getClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      field.get(obj)
    }
    def checkUnsafeSorterSpillReaderClosed(
        unsafeSorterIterator: UnsafeSorterIterator): Unit = unsafeSorterIterator match {
      case reader: UnsafeSorterSpillReader =>
        // If UnsafeSorterSpillReader is not closed, `in` and `din` are not null
        assert(getFieldValue(reader, "in") == null)
        assert(getFieldValue(reader, "din") == null)
      case _ => // do noting
    }
    // Only check `SpillableArrayIterator` because `InMemoryBufferIterator` not open the file handle
    if (iterator.getClass.getSimpleName.equals("SpillableArrayIterator")) {
      val chainedIterator = getFieldValue(iterator, "iterator")
      val current = getFieldValue(chainedIterator, "current")
      assert(current.isInstanceOf[UnsafeSorterIterator])
      checkUnsafeSorterSpillReaderClosed(current.asInstanceOf[UnsafeSorterIterator])
      val iterators = getFieldValue(chainedIterator, "iterators")
      iterators.asInstanceOf[util.Queue[UnsafeSorterIterator]].asScala
        .foreach(checkUnsafeSorterSpillReaderClosed)
    }
  }
}
