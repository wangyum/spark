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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, Lag, Literal, MutableProjection, NamedExpression, NthValue, OffsetWindowFunction, SortOrder, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Tests for the window partition-size guard. Most frames track the per-row index and window
 * bounds in 32-bit `Int`s, so a partition larger than `Int.MaxValue` rows is rejected with a clear
 * error instead of silently producing wrong results, unless every frame declares
 * `supportsLargePartition` (LEAD/LAG and unbounded NTH_VALUE). See
 * `WindowEvaluatorFactoryBase.checkPartitionSizeLimit`.
 *
 * All frames here are real frames, so the tests read each frame's actual
 * `supportsLargePartition` rather than a value a stub was told to return.
 */
class WindowFunctionFrameSuite extends SparkFunSuite {

  /** Minimal factory exposing the protected partition-size guard for testing. */
  private class TestFactory extends WindowEvaluatorFactoryBase {
    override def windowExpression: Seq[NamedExpression] = Nil
    override def partitionSpec: Seq[Expression] = Nil
    override def orderSpec: Seq[SortOrder] = Nil
    override def childOutput: Seq[Attribute] = Nil
    override def spillSize: SQLMetric = null

    def check(numRows: Long, frames: WindowFunctionFrame*): Unit =
      checkPartitionSizeLimit(numRows, frames.toArray)
  }

  private val maxRows = Int.MaxValue.toLong
  private val attr = AttributeReference("v", IntegerType, nullable = true)()
  private val newProjection =
    (expressions: Seq[Expression], schema: Seq[Attribute]) =>
      MutableProjection.create(expressions, schema)

  private def target = new SpecificInternalRow(Seq(IntegerType))

  /** LAG(v, 1): a frameless offset frame. */
  private def lagFrame: WindowFunctionFrame =
    new FrameLessOffsetWindowFunctionFrame(
      target,
      ordinal = 0,
      Array[OffsetWindowFunction](Lag(attr, Literal(1), Literal(null, IntegerType), false)),
      Seq[Attribute](attr),
      newProjection,
      offset = -1)

  /** NTH_VALUE(v, 1) over the whole partition. */
  private def unboundedNthValueFrame: WindowFunctionFrame =
    new UnboundedOffsetWindowFunctionFrame(
      target,
      ordinal = 0,
      Array[OffsetWindowFunction](NthValue(attr, Literal(1), ignoreNulls = false)),
      Seq[Attribute](attr),
      newProjection,
      offset = 1)

  /** NTH_VALUE(v, 1) over UNBOUNDED PRECEDING AND CURRENT ROW; its `write` reads the row index. */
  private def unboundedPrecedingNthValueFrame: WindowFunctionFrame =
    new UnboundedPrecedingOffsetWindowFunctionFrame(
      target,
      ordinal = 0,
      Array[OffsetWindowFunction](NthValue(attr, Literal(1), ignoreNulls = false)),
      Seq[Attribute](attr),
      newProjection,
      offset = 1)

  /** An aggregate over the whole partition; inherits the default `supportsLargePartition`. */
  private def unboundedAggregateFrame: WindowFunctionFrame =
    new UnboundedWindowFunctionFrame(target, processor = null)

  test("frames declare whether they support partitions larger than Int.MaxValue rows") {
    assert(lagFrame.supportsLargePartition)
    assert(unboundedNthValueFrame.supportsLargePartition)
    assert(!unboundedPrecedingNthValueFrame.supportsLargePartition)
    // Fails if the `WindowFunctionFrame` default ever becomes `true`.
    assert(!unboundedAggregateFrame.supportsLargePartition)
  }

  test("partitions up to Int.MaxValue rows are allowed for every frame") {
    val factory = new TestFactory
    factory.check(0L, unboundedAggregateFrame)
    factory.check(maxRows, unboundedAggregateFrame, unboundedPrecedingNthValueFrame)
  }

  test("larger partitions are allowed when every frame supports them") {
    val factory = new TestFactory
    factory.check(maxRows + 1, lagFrame)
    factory.check(Long.MaxValue, lagFrame, unboundedNthValueFrame)
  }

  test("larger partitions are rejected with a clear error if any frame does not support them") {
    val factory = new TestFactory
    val e = intercept[SparkException] {
      factory.check(maxRows + 1, unboundedAggregateFrame)
    }
    assert(e.getCondition == "WINDOW_FUNCTION_PARTITION_SIZE_EXCEEDS_LIMIT")
    assert(e.getMessageParameters.get("numRows") == (maxRows + 1).toString)

    // A single unsupported frame alongside supported ones is enough to reject.
    intercept[SparkException] {
      factory.check(maxRows + 1, lagFrame, unboundedPrecedingNthValueFrame)
    }
  }

  /** A LAG(v, 1) frame whose cursor a test can move, to reach positions it cannot iterate to. */
  private class SeekableLagFrame(result: SpecificInternalRow)
    extends FrameLessOffsetWindowFunctionFrame(
      result,
      ordinal = 0,
      Array[OffsetWindowFunction](Lag(attr, Literal(1), Literal(null, IntegerType), false)),
      Seq[Attribute](attr),
      newProjection,
      offset = -1) {

    def startCursorAt(index: Int): Unit = {
      inputIndex = index
    }
  }

  test("LAG keeps returning real rows when its cursor crosses Int.MaxValue") {
    // `OffsetWindowFunctionFrameBase.inputIndex` is a `Long`. As an `Int` it would wrap negative
    // after Int.MaxValue rows, `inputIndex >= 0` would turn false, and every later row would
    // silently get the default value (NULL) instead of the lagged value. A partition that large
    // cannot be built in a unit test, so use a small array that reports a length above
    // Int.MaxValue and start the cursor just below the boundary.
    val values = Seq(10, 20, 30, 40, 50)
    val toUnsafeRow = UnsafeProjection.create(Array[DataType](IntegerType))
    val rows = new ExternalAppendOnlyUnsafeRowArray(
      null, null, null, null, 1024, 1L, 100, Long.MaxValue, 100, Long.MaxValue) {
      override def length: Long = maxRows + 10
    }
    values.foreach(v => rows.add(toUnsafeRow(new GenericInternalRow(Array[Any](v)))))

    val result = target
    val frame = new SeekableLagFrame(result)
    frame.prepare(rows)
    frame.startCursorAt(Int.MaxValue - 1)

    // The cursor is at Int.MaxValue - 1, Int.MaxValue, Int.MaxValue + 1 and Int.MaxValue + 2 for
    // these four writes; the last two are past the boundary.
    values.take(4).zipWithIndex.foreach { case (expected, i) =>
      frame.write(i, InternalRow.empty)
      assert(!result.isNullAt(0), s"write $i returned the default value")
      assert(result.getInt(0) == expected, s"write $i")
    }
  }

  test("SPARK-61231: SlidingWindowFunctionFrame reuses rows from an unspilled buffer") {
    // While `input` hasn't spilled, its iterator hands out the distinct `UnsafeRow`s it already
    // owns (see `ExternalAppendOnlyUnsafeRowArray.isSpillBacked`), so the frame's own buffer can
    // hold the very same references instead of copying each row again.
    val toUnsafeRow = UnsafeProjection.create(Array[DataType](IntegerType))
    val rows = new ExternalAppendOnlyUnsafeRowArray(
      null, null, null, null, 1024, 1L, 100, Long.MaxValue, 100, Long.MaxValue)
    val values = Seq(10, 20, 30, 40, 50)
    values.foreach(v => rows.add(toUnsafeRow(new GenericInternalRow(Array[Any](v)))))
    assert(!rows.isSpillBacked)
    val owned = rows.generateIterator().toArray

    // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING.
    val frame = new SlidingWindowFunctionFrame(
      target, processor = null, lbound = RowBoundOrdering(-1), ubound = RowBoundOrdering(1))
    frame.prepare(rows)
    values.indices.foreach(i => frame.write(i, InternalRow.empty))

    assert(frame.buffer.size() > 0)
    val bufferedRows = new Array[InternalRow](frame.buffer.size())
    frame.buffer.toArray(bufferedRows).foreach { row =>
      assert(owned.exists(_.asInstanceOf[AnyRef] eq row.asInstanceOf[AnyRef]),
        "the buffered row is a fresh copy instead of the array's own row instance")
    }
  }

  test("SPARK-61231: SlidingWindowFunctionFrame still copies rows once input has spilled") {
    // Once `input` spills, its iterator rebinds a single reusable `UnsafeRow` on every `next()`,
    // so the frame must keep copying to hold on to a row past the current iteration step.
    val toUnsafeRow = UnsafeProjection.create(Array[DataType](IntegerType))
    // `numRowsInMemoryBufferThreshold = 0` forces every `add` straight into the spillable path.
    val rows = new ExternalAppendOnlyUnsafeRowArray(
      null, null, null, null, 1024, 1L, 0, Long.MaxValue, 100, Long.MaxValue) {
      // Avoid touching a real UnsafeExternalSorter (needs a live TaskMemoryManager/BlockManager);
      // this fake spillable store is just a plain in-memory holder that reuses one row object,
      // reproducing the spill-backed iterator's "reused `UnsafeRow`" sharing behavior.
      private val stored = new scala.collection.mutable.ArrayBuffer[Array[Byte]]()
      override def isSpillBacked: Boolean = true
      override def add(unsafeRow: UnsafeRow): Unit = {
        // `copy()` first: `getBytes()` alone can return the row's own live backing array
        // (see `UnsafeDataUtils.getBytes`'s zero-copy fast path), which would alias the
        // caller's reused projection buffer instead of snapshotting this value.
        stored += unsafeRow.copy().getBytes
        numRowsInternal += 1
      }
      private var numRowsInternal: Long = 0
      override def length: Long = numRowsInternal
      override def generateIterator(startIndex: Int): Iterator[UnsafeRow] =
        new Iterator[UnsafeRow] {
          private var i = startIndex
          private val reusable = new UnsafeRow(1)
          override def hasNext: Boolean = i < stored.length
          override def next(): UnsafeRow = {
            val bytes = stored(i)
            reusable.pointTo(bytes, bytes.length)
            i += 1
            reusable
          }
        }
    }
    val values = Seq(10, 20, 30, 40, 50)
    values.foreach(v => rows.add(toUnsafeRow(new GenericInternalRow(Array[Any](v)))))
    assert(rows.isSpillBacked)

    // ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING.
    val frame = new SlidingWindowFunctionFrame(
      target, processor = null, lbound = RowBoundOrdering(-1), ubound = RowBoundOrdering(1))
    frame.prepare(rows)
    values.indices.foreach(i => frame.write(i, InternalRow.empty))

    // The window for the last row covers values 40 and 50; if the frame had kept references into
    // the reused row instead of copying, every buffered row would wrongly show the last value.
    val bufferedValues = new Array[InternalRow](frame.buffer.size())
    frame.buffer.toArray(bufferedValues)
    assert(bufferedValues.map(_.getInt(0)).toSeq == Seq(40, 50))
  }
}
