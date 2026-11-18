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

import java.util.Properties

import org.apache.spark.{SparkConf, SparkFunSuite, SparkUnsupportedOperationException, TaskContext,
  TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Direct, low-level tests of [[UnsafeRowArena]], independent of
 * [[HashRowNumberGroupLimitAccumulator]].
 */
class UnsafeRowArenaSuite extends SparkFunSuite {

  private val schema = StructType(
    StructField("a", IntegerType) :: StructField("b", StringType) :: Nil)
  private val projection = UnsafeProjection.create(schema)

  private def row(a: Int, b: String) =
    projection(new GenericInternalRow(Array[Any](a, UTF8String.fromString(b)))).copy()

  /**
   * Mirrors HashWindowGroupLimitSuite's forced-OOM helper: a standalone TaskMemoryManager/
   * TaskContext, not routed through SparkEnv, so this doesn't need a SharedSparkSession. Note
   * `budgetBytes` must be at least `UnsafeRowArena`'s initial page size (the constructor
   * allocates that page eagerly) for construction itself to succeed.
   */
  private def withArena[T](budgetBytes: Long = Long.MaxValue, linked: Boolean = false)
      (f: (UnsafeRowArena, TestMemoryManager) => T): T = {
    val memoryManager = new TestMemoryManager(new SparkConf(false))
    memoryManager.limit(budgetBytes)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    val prevTaskContext = TaskContext.get()
    val taskContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = 0,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpuAmount = 1)
    TaskContext.setTaskContext(taskContext)
    try {
      f(new UnsafeRowArena(taskMemoryManager, linked), memoryManager)
    } finally {
      if (prevTaskContext != null) {
        TaskContext.setTaskContext(prevTaskContext)
      } else {
        TaskContext.unset()
      }
    }
  }

  test("append then getRow round-trips a single row") {
    withArena() { (arena, _) =>
      val r = row(42, "hello")
      val address = arena.append(r)
      val result = arena.getRow(address, new UnsafeRow(2))
      assert(result.getInt(0) == 42)
      assert(result.getString(1) == "hello")
      arena.free()
    }
  }

  test("multiple rows at different addresses remain independently readable") {
    withArena() { (arena, _) =>
      val rows = (0 until 500).map(i => row(i, s"value-$i"))
      val addresses = rows.map(arena.append)
      val resultRow = new UnsafeRow(2)
      // Read back out of insertion order to make sure addresses aren't accidentally aliased.
      (499 to 0 by -1).foreach { i =>
        val result = arena.getRow(addresses(i), resultRow)
        assert(result.getInt(0) == i, s"row $i: expected a=$i, got a=${result.getInt(0)}")
        assert(result.getString(1) == s"value-$i")
      }
      arena.free()
    }
  }

  test("growth across many pages preserves earlier addresses") {
    // UnsafeRowArena's initial page is fixed-size (not configurable), so forcing several growth
    // cycles just means appending enough rows to exceed it several times over.
    withArena() { (arena, _) =>
      val n = 50000
      val addresses = (0 until n).map(i => arena.append(row(i, s"row-$i")))
      val resultRow = new UnsafeRow(2)
      (0 until n).foreach { i =>
        val result = arena.getRow(addresses(i), resultRow)
        assert(result.getInt(0) == i)
        assert(result.getString(1) == s"row-$i")
      }
      arena.free()
    }
  }

  test("free() is idempotent") {
    withArena() { (arena, _) =>
      arena.append(row(1, "x"))
      arena.free()
      arena.free() // must not throw
    }
  }

  test("append throws SparkOutOfMemoryError (not something uncaught/unexpected) when the task" +
      " budget is exhausted") {
    // Large enough for the initial page (so construction itself succeeds) plus a little growth
    // room, but far too small for 100,000 rows with ~100-byte payloads each.
    withArena(budgetBytes = 1024 * 512) { (arena, _) =>
      val caught = intercept[SparkOutOfMemoryError] {
        (0 until 100000).foreach(i => arena.append(row(i, "x" * 100)))
      }
      assert(caught.getMessage != null)
      arena.free()
    }
  }

  test("append throws SparkUnsupportedOperationException (not an uncaught" +
      " IllegalArgumentException) for a row at or beyond the 256MB address-packing limit") {
    withArena() { (arena, _) =>
      // UnsafeRow.pointTo(Object, long, int) only validates sizeInBytes against the backing
      // array's length when baseObject is a byte[]; a null baseObject (as every off-heap
      // UnsafeRow already uses) skips that check, so this exercises append's size check --
      // which runs, and throws, before ever touching the page or calling copyMemory -- without
      // really allocating ~256MB.
      val oversized = new UnsafeRow(2)
      oversized.pointTo(null, 0L, 1 << 28)
      val caught = intercept[SparkUnsupportedOperationException] {
        arena.append(oversized)
      }
      assert(caught.getMessage != null)
      arena.free()
    }
  }

  test("appendLinked chains rows via getNext, terminated by UnsafeRowArena.NO_NEXT") {
    withArena(linked = true) { (arena, _) =>
      val third = arena.appendLinked(row(3, "c"), UnsafeRowArena.NO_NEXT)
      val second = arena.appendLinked(row(2, "b"), third)
      val head = arena.appendLinked(row(1, "a"), second)

      val resultRow = new UnsafeRow(2)
      var address = head
      val seen = scala.collection.mutable.ArrayBuffer[Int]()
      while (address != UnsafeRowArena.NO_NEXT) {
        seen += arena.getRow(address, resultRow).getInt(0)
        address = arena.getNext(address)
      }
      assert(seen.toSeq == Seq(1, 2, 3))
      arena.free()
    }
  }

  test("appendLinked throws when the arena was not constructed for linked mode") {
    withArena(linked = false) { (arena, _) =>
      intercept[IllegalArgumentException] {
        arena.appendLinked(row(1, "a"), UnsafeRowArena.NO_NEXT)
      }
      arena.free()
    }
  }

  test("append throws when the arena was constructed for linked mode") {
    withArena(linked = true) { (arena, _) =>
      intercept[IllegalArgumentException] {
        arena.append(row(1, "a"))
      }
      arena.free()
    }
  }

  test("bytesUsed reflects page allocation, not per-row growth") {
    withArena() { (arena, _) =>
      assert(arena.bytesUsed > 0, "the initial page allocation should already count as used")
      val before = arena.bytesUsed
      arena.append(row(1, "small"))
      // A single small row within the already-allocated initial page should not trigger growth.
      assert(arena.bytesUsed == before,
        "a row that fits in the current page must not change bytesUsed")
      arena.free()
    }
  }
}
