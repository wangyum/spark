# Hash-Based WindowGroupLimitExec (ROW_NUMBER()=1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `WindowGroupLimitExec` skip the mandatory full sort for the
`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1` pattern by replacing
it, for that case only, with a hash-based single-pass accumulator.

**Architecture:** `WindowGroupLimitExec` gains an `isHashEligible` flag
(`RowNumber`, `limit == 1`, order-key types all `UnsafeRow`-mutable, config
on). When eligible, `requiredChildOrdering` becomes `Nil` and execution uses a
new hash-based evaluator built on `UnsafeFixedWidthAggregationMap` (fixed-width
index: order-key fields + a row pointer) plus a plain JVM-heap array of full
candidate rows; on memory pressure it drains into `UnsafeExternalRowSorter`
(`SortExec`'s own sorter) and finishes via the existing sort-based
`GroupedLimitIterator`/`SimpleLimitIterator`. Whole-stage codegen is added in
a later task, mirroring the same algorithm inline.

**Tech Stack:** Scala/Java, Spark SQL Catalyst/Tungsten
(`UnsafeFixedWidthAggregationMap`, `UnsafeExternalRowSorter`,
`GenerateOrdering`, `UnsafeProjection`), ScalaTest (`QueryTest` +
`SharedSparkSession`).

**Spec:** `docs/superpowers/specs/2026-11-16-hash-window-group-limit-design.md`

## Global Constraints

- Eligibility is `rankLikeFunction.isInstanceOf[RowNumber] && limit == 1 &&`
  every `orderSpec(i).child.dataType` satisfies `UnsafeRow.isMutable` `&&`
  `conf.getConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED)`. This is a
  **static, plan-time** decision — no runtime fallback to the sort-based
  algorithm except for the memory-pressure spill case in Task 2.
- Config key: `spark.sql.windowExec.groupLimit.hashPushdown.enabled`,
  default `true`, `.version("4.4.0")`.
- `WindowGroupLimitExec.output`, `outputPartitioning`, and
  `requiredChildDistribution` must not change.
- No source line may exceed 100 characters (project lint rule); no non-ASCII
  characters in code/comments.
- Every new/changed Scala file must compile under `build/sbt catalyst/compile`
  or `build/sbt sql/compile` as appropriate before moving to the next task.

---

### Task 1: Add the `hashPushdown.enabled` config

**Files:**
- Modify: `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala:5377` (insert after `WINDOW_GROUP_LIMIT_THRESHOLD`, i.e. after line 5387, before `COLLAPSE_WINDOW_WITH_EMPTY_ORDER_SPEC_IN_CHILD`)
- Modify: `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala:10530` (insert accessor after `def windowGroupLimitThreshold`)
- Test: `sql/catalyst/src/test/scala/org/apache/spark/sql/internal/SQLConfSuite.scala`

**Interfaces:**
- Produces: `SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED: ConfigEntry[Boolean]`
  and `SQLConf.windowGroupLimitHashPushdownEnabled: Boolean` (instance method
  on `SQLConf`), both consumed by Task 2.

- [ ] **Step 1: Write the failing test**

  Add to `SQLConfSuite.scala` (find the existing `test(...)` block list and
  add a new one near other simple default-value tests, e.g. after any test
  that checks a boolean conf's default):

  ```scala
  test("SPARK-XXXXX: windowGroupLimitHashPushdownEnabled defaults to true") {
    assert(spark.sessionState.conf.windowGroupLimitHashPushdownEnabled)
  }
  ```

  (Replace `SPARK-XXXXX` with the real JIRA key once created per
  `CLAUDE.md`'s PR workflow, before opening the PR — not required to compile
  or pass tests.)

- [ ] **Step 2: Run test to verify it fails**

  Run: `build/sbt 'catalyst/testOnly org.apache.spark.sql.internal.SQLConfSuite -- -z "windowGroupLimitHashPushdownEnabled"'`
  Expected: FAIL — `windowGroupLimitHashPushdownEnabled` is not a member of `SQLConf`.

- [ ] **Step 3: Add the config entry and accessor**

  In `SQLConf.scala`, immediately after `WINDOW_GROUP_LIMIT_THRESHOLD`'s
  closing `.createWithDefault(1000)` (line 5387) and before the blank line
  preceding `COLLAPSE_WINDOW_WITH_EMPTY_ORDER_SPEC_IN_CHILD`:

  ```scala
  val WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED =
    buildConf("spark.sql.windowExec.groupLimit.hashPushdown.enabled")
      .doc("When true, WindowGroupLimitExec uses an in-memory hash-based " +
        "accumulator instead of full sorting for ROW_NUMBER() window group " +
        "limits of 1, when the order key's type supports it.")
      .version("4.4.0")
      .booleanConf
      .createWithDefault(true)
  ```

  Then, immediately after `def windowGroupLimitThreshold: Int = getConf(WINDOW_GROUP_LIMIT_THRESHOLD)`
  (line 10530):

  ```scala
  def windowGroupLimitHashPushdownEnabled: Boolean =
    getConf(WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED)
  ```

- [ ] **Step 4: Run test to verify it passes**

  Run: `build/sbt 'catalyst/testOnly org.apache.spark.sql.internal.SQLConfSuite -- -z "windowGroupLimitHashPushdownEnabled"'`
  Expected: PASS

- [ ] **Step 5: Commit**

  ```bash
  git add sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala \
          sql/catalyst/src/test/scala/org/apache/spark/sql/internal/SQLConfSuite.scala
  git commit -m "Add spark.sql.windowExec.groupLimit.hashPushdown.enabled config"
  ```

---

### Task 2: Hash-based evaluator + wiring into `WindowGroupLimitExec`

**Files:**
- Create: `sql/core/src/main/scala/org/apache/spark/sql/execution/window/HashRowNumberGroupLimitEvaluatorFactory.scala`
- Modify: `sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala`
- Test: `sql/core/src/test/scala/org/apache/spark/sql/execution/window/HashWindowGroupLimitSuite.scala` (new)

**Interfaces:**
- Consumes: `SQLConf.windowGroupLimitHashPushdownEnabled` from Task 1;
  `UnsafeRow.isMutable(DataType): Boolean` (existing,
  `org.apache.spark.sql.catalyst.expressions.UnsafeRow`);
  `UnsafeFixedWidthAggregationMap` (existing,
  `org.apache.spark.sql.execution`); `UnsafeExternalRowSorter` (existing,
  `org.apache.spark.sql.execution`); `GroupedLimitIterator`,
  `SimpleLimitIterator` (existing, same package, from
  `WindowGroupLimitExec.scala`).
- Produces:
  - `WindowGroupLimitExec.isHashEligible: Boolean` (a `lazy val`, used by
    Task 3's codegen methods too).
  - `class HashRowNumberGroupLimitEvaluatorFactory(partitionSpec: Seq[Expression], orderSpec: Seq[SortOrder], childOutput: Seq[Attribute], numOutputRows: SQLMetric) extends PartitionEvaluatorFactory[InternalRow, InternalRow]`,
    with `createEvaluator(): PartitionEvaluator[InternalRow, InternalRow]`
    whose `eval(partitionIndex: Int, inputs: Iterator[InternalRow]*): Iterator[InternalRow]`
    implements the algorithm below. Task 3 does not call this class directly
    (codegen is independent generated code) but must produce identical
    results, so its test suite is the ground truth for codegen equivalence
    tests.

#### Algorithm for `HashRowNumberGroupLimitEvaluatorFactory`'s evaluator

```
groupingProjection = UnsafeProjection.create(partitionSpec, childOutput)
orderKeyProjection  = UnsafeProjection.create(orderSpec.map(_.child), childOutput)
n = orderSpec.length
bufferSchema = StructType(
  orderSpec.zipWithIndex.map { case (so, i) =>
    StructField(s"o$i", so.child.dataType, so.child.nullable)
  } :+ StructField("winnerIndex", IntegerType, nullable = false))
emptyBuffer = InternalRow(<n nulls>, -1)  // -1 sentinel for "unset"

// ordering: compares two rows using BoundReference(0..n-1) with each
// orderSpec(i)'s direction/nullOrdering, via GenerateOrdering
boundOrdering = GenerateOrdering.create(
  orderSpec.zipWithIndex.map { case (so, i) =>
    so.copy(child = BoundReference(i, so.child.dataType, so.child.nullable))
  })

map = new UnsafeFixedWidthAggregationMap(
  emptyBuffer, bufferSchema, groupingKeySchema, TaskContext.get(), 1 << 16, pageSizeBytes)
winnerRows = new ArrayBuffer[UnsafeRow]
var fallbackIterator: Iterator[InternalRow] = null

while (fallbackIterator == null && input.hasNext) {
  val row = input.next().asInstanceOf[UnsafeRow]
  val key = groupingProjection(row)
  val buffer = map.getAggregationBufferFromUnsafeRow(key)
  if (buffer == null) {
    fallbackIterator = spillFallback(map, winnerRows, row, input)  // Task 2, see below
  } else if (buffer.getInt(n) == -1) {
    val idx = winnerRows.length
    winnerRows += row.copy()
    val orderKey = orderKeyProjection(row)
    writeOrderKeyIntoBuffer(buffer, orderKey, orderSpec)  // per-field setters by type
    buffer.setInt(n, idx)
  } else {
    val orderKey = orderKeyProjection(row)
    if (boundOrdering.compare(orderKey, buffer) < 0) {  // orderKey "less"=better per SortOrder
      writeOrderKeyIntoBuffer(buffer, orderKey, orderSpec)
      winnerRows(buffer.getInt(n)) = row.copy()
    }
  }
}

if (fallbackIterator != null) fallbackIterator
else winnerRows.iterator.map { r => numOutputRows += 1; r }
```

`writeOrderKeyIntoBuffer` dispatches per `orderSpec(i).child.dataType` using
`UnsafeRow`'s typed setters (`setBoolean`/`setByte`/`setShort`/`setInt`/
`setLong`/`setFloat`/`setDouble`/`setDecimal(ordinal, value, precision)`),
matching `UnsafeRow.isMutable`'s type list exactly — this is a straight-line
`match` on `DataType`, not reflection.

`boundOrdering.compare(orderKey, buffer)`: both arguments must be read via
the SAME ordinal positions 0 until n, which both `orderKey` (an `n`-field
row) and `buffer` (an `(n+1)`-field row, extra trailing `winnerIndex`)
satisfy — `GenerateOrdering`'s generated code only ever reads ordinals
`0 until n` on each side independently, so the extra trailing field on
`buffer` is never touched and the mismatched total field count is safe. The
sign convention follows `orderSpec`'s own `Ascending`/`Descending` encoding,
so "compare < 0" means "first argument sorts first", i.e. `orderKey` is
better than whatever is stored in `buffer` under `orderSpec`'s semantics
(e.g. for `ORDER BY x DESC`, a strictly greater `x` sorts first and wins).

#### Spill fallback (`spillFallback`)

```
def spillFallback(
    map: UnsafeFixedWidthAggregationMap,
    winnerRows: ArrayBuffer[UnsafeRow],
    pendingRow: UnsafeRow,
    rest: Iterator[InternalRow]): Iterator[InternalRow] = {
  val sortOrder = partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec
  val ordering = RowOrdering.create(sortOrder, childOutput)
  val boundSortExpression = BindReferences.bindReference(sortOrder.head, childOutput)
  val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)
  val prefixExpr = SortPrefix(boundSortExpression)
  val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
  val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
    private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
    override def computePrefix(row: InternalRow) = {
      val prefix = prefixProjection.apply(row)
      result.isNull = prefix.isNullAt(0)
      result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
      result
    }
  }
  val schema = DataTypeUtils.fromAttributes(childOutput)
  val sorter = UnsafeExternalRowSorter.create(
    schema, ordering, prefixComparator, prefixComputer,
    SparkEnv.get.memoryManager.pageSizeBytes, /* canUseRadixSort = */ false)
  winnerRows.foreach(sorter.insertRow)
  sorter.insertRow(pendingRow)
  rest.foreach(r => sorter.insertRow(r.asInstanceOf[UnsafeRow]))
  map.free()
  val sortedIter = sorter.sort()
  val limitFunc = (iter: Iterator[InternalRow]) =>
    SimpleLimitIterator(iter, 1, numOutputRows)
  if (partitionSpec.isEmpty) {
    limitFunc(sortedIter)
  } else {
    new GroupedLimitIterator(sortedIter, childOutput, partitionSpec, limitFunc)
  }
}
```

This reuses `WindowGroupLimitExec.scala`'s existing `SimpleLimitIterator` and
`GroupedLimitIterator` classes directly (already in scope, same file/package)
— no new grouping/limiting logic, matching spec Section 4.5. Note
`canUseRadixSort = false`: radix sort only helps single-column numeric sorts
and this is a rare fallback path, not a case worth the extra complexity of
wiring up `SortExec`'s `canUseRadixSort` detection here.

#### `WindowGroupLimitExec` changes

```scala
lazy val isHashEligible: Boolean =
  conf.windowGroupLimitHashPushdownEnabled &&
    rankLikeFunction.isInstanceOf[RowNumber] &&
    limit == 1 &&
    orderSpec.forall(so => UnsafeRow.isMutable(so.child.dataType))

override def requiredChildOrdering: Seq[Seq[SortOrder]] =
  if (isHashEligible) {
    Seq(Nil)
  } else {
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
  }
```

`doExecute()` branches on `isHashEligible` to pick
`HashRowNumberGroupLimitEvaluatorFactory` vs. the existing
`WindowGroupLimitEvaluatorFactory`, keeping the existing
`conf.usePartitionEvaluator` branch (`mapPartitionsWithEvaluator` vs.
`mapPartitionsWithIndexInternal`) for both.

- [ ] **Step 1: Write the failing tests**

  Create `HashWindowGroupLimitSuite.scala`:

  ```scala
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

  import org.apache.spark.sql.{QueryTest, Row}
  import org.apache.spark.sql.execution.SortExec
  import org.apache.spark.sql.internal.SQLConf
  import org.apache.spark.sql.test.SharedSparkSession

  class HashWindowGroupLimitSuite extends QueryTest with SharedSparkSession {
    import testImplicits._

    private def dedupLatest(df: org.apache.spark.sql.DataFrame) = {
      df.selectExpr(
        "*",
        "row_number() over (partition by k order by ts desc) as rn")
        .where("rn = 1")
        .drop("rn")
    }

    test("SPARK-XXXXX: hash path matches sort-based path for top-1 row_number") {
      val df = Seq(
        (1, 100L, "a"), (1, 200L, "b"), (2, 50L, "c"),
        (2, 50L, "d"), (3, 10L, "e")
      ).toDF("k", "ts", "v")

      val withHash = withSQLConf(
        SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
        dedupLatest(df).collect()
      }
      val withoutHash = withSQLConf(
        SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "false") {
        dedupLatest(df).collect()
      }
      checkAnswer(withHash.toSeq.toDF("k", "ts", "v"),
        withoutHash.toSeq.toDF("k", "ts", "v").collect().toSeq)
    }

    test("SPARK-XXXXX: hash path handles nulls in order key") {
      val df = Seq((1, Some(5L)), (1, None), (1, Some(10L)), (2, None))
        .toDF("k", "ts")
      withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
        checkAnswer(dedupLatest(df), Seq(Row(1, 10L), Row(2, null)))
      }
    }

    test("SPARK-XXXXX: global top-1 (empty partition spec) uses hash path") {
      val df = Seq((1L, "a"), (5L, "b"), (3L, "c")).toDF("ts", "v")
      withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
        val result = df.selectExpr("*", "row_number() over (order by ts desc) as rn")
          .where("rn = 1").drop("rn")
        checkAnswer(result, Seq(Row(5L, "b")))
      }
    }

    test("SPARK-XXXXX: string order key is not hash-eligible (plan unchanged)") {
      val df = Seq((1, "b"), (1, "a")).toDF("k", "v")
      withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
        val plan = dedupLatest(df).queryExecution.executedPlan
        assert(plan.exists(_.isInstanceOf[SortExec]),
          "string order key must still use the sort-based path")
      }
    }

    test("SPARK-XXXXX: numeric order key is hash-eligible (no SortExec before" +
        " WindowGroupLimitExec)") {
      val df = Seq((1, 1L), (1, 2L)).toDF("k", "ts")
      withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
        val plan = dedupLatest(df).queryExecution.executedPlan
        val limits = plan.collect { case w: WindowGroupLimitExec => w }
        assert(limits.nonEmpty && limits.forall(_.isHashEligible))
      }
    }

    test("SPARK-XXXXX: hash path survives a forced spill") {
      withSQLConf(
        SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
        "spark.testing.memory" -> "80000000") {
        val n = 20000
        val df = (0 until n).map(i => (i, i.toLong, i.toLong)).toDF("k", "ts", "v")
        withSQLConf(SQLConf.WINDOW_EXEC_BUFFER_IN_MEMORY_THRESHOLD.key -> "10") {
          val result = dedupLatest(df).collect()
          assert(result.length == n)
        }
      }
    }
  }
  ```

  (Replace `SPARK-XXXXX` in test names with the real JIRA key once created.
  The forced-spill test's exact memory-pressure trigger may need adjustment
  once `UnsafeFixedWidthAggregationMap`'s page size is wired in Step 3 — the
  goal is a `TaskContext` with a page size small enough that inserting
  ~20,000 distinct keys forces at least one `getAggregationBufferFromUnsafeRow`
  to return `null`; prefer a dedicated small-page-size constructor argument
  threaded through a test-only constructor overload if `spark.testing.memory`
  proves too coarse, rather than relying on incidental sizing.)

- [ ] **Step 2: Run tests to verify they fail**

  Run: `build/sbt 'sql/testOnly org.apache.spark.sql.execution.window.HashWindowGroupLimitSuite'`
  Expected: FAIL — `isHashEligible` doesn't exist, `WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED` not referenced correctly, or results differ (since there's no hash path yet, `"true"` config does nothing and `SortExec` always appears, failing the "numeric order key is hash-eligible" test).

- [ ] **Step 3: Implement `HashRowNumberGroupLimitEvaluatorFactory`**

  Create the file with the algorithm and spill fallback described above,
  translated to compiling Scala. Reuse `SimpleLimitIterator`,
  `GroupedLimitIterator` from `WindowGroupLimitExec.scala` (same package, no
  import needed). Key imports:
  `org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeRow, ...}`,
  `org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering`,
  `org.apache.spark.sql.execution.{RowOrdering, SortPrefixUtils, UnsafeExternalRowSorter}`,
  `org.apache.spark.sql.catalyst.util.DataTypeUtils`,
  `org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, StructField, StructType}`,
  `scala.collection.mutable.ArrayBuffer`.

- [ ] **Step 4: Wire `isHashEligible` and `requiredChildOrdering` into `WindowGroupLimitExec`**

  Edit `WindowGroupLimitExec.scala`:

  ```scala
  lazy val isHashEligible: Boolean =
    conf.windowGroupLimitHashPushdownEnabled &&
      rankLikeFunction.isInstanceOf[RowNumber] &&
      limit == 1 &&
      orderSpec.forall(so => UnsafeRow.isMutable(so.child.dataType))

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    if (isHashEligible) {
      Seq(Nil)
    } else {
      Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
    }
  ```

  And in `doExecute()`, replace the single `evaluatorFactory` val with a
  branch:

  ```scala
  val evaluatorFactory =
    if (isHashEligible) {
      new HashRowNumberGroupLimitEvaluatorFactory(
        partitionSpec, orderSpec, child.output, longMetric("numOutputRows"))
    } else {
      new WindowGroupLimitEvaluatorFactory(
        partitionSpec, orderSpec, rankLikeFunction, limit, child.output,
        longMetric("numOutputRows"))
    }
  ```

  leaving the `conf.usePartitionEvaluator` branch below it untouched (it
  already just calls `evaluatorFactory.createEvaluator()` generically).

- [ ] **Step 5: Run tests to verify they pass**

  Run: `build/sbt 'sql/testOnly org.apache.spark.sql.execution.window.HashWindowGroupLimitSuite'`
  Expected: PASS. If the forced-spill test doesn't actually exercise the
  `buffer == null` branch, adjust the page-size/row-count constants (not the
  assertion) until it does — confirm via a temporary log line or debugger,
  then remove the temporary diagnostic before committing.

- [ ] **Step 6: Run the broader existing suites to check for regressions**

  Run:
  ```bash
  build/sbt 'sql/testOnly org.apache.spark.sql.DataFrameWindowFunctionsSuite'
  build/sbt 'sql/testOnly org.apache.spark.sql.execution.RemoveRedundantWindowGroupLimitsSuite'
  build/sbt 'catalyst/testOnly org.apache.spark.sql.catalyst.optimizer.InferWindowGroupLimitSuite'
  ```
  Expected: PASS, no new failures. If `DataFrameWindowFunctionsSuite` has
  plan-shape assertions that assumed a `SortExec` always precedes
  `WindowGroupLimitExec`, they will need updating to account for the
  hash-eligible case — update the assertion to check `isHashEligible`-aware
  conditions rather than deleting the check.

- [ ] **Step 7: Commit**

  ```bash
  git add sql/core/src/main/scala/org/apache/spark/sql/execution/window/HashRowNumberGroupLimitEvaluatorFactory.scala \
          sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala \
          sql/core/src/test/scala/org/apache/spark/sql/execution/window/HashWindowGroupLimitSuite.scala
  git commit -m "Add hash-based accumulator path for top-1 ROW_NUMBER() window group limit"
  ```

---

### Task 3: Whole-stage codegen for the hash path

**Files:**
- Modify: `sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala`
- Test: `sql/core/src/test/scala/org/apache/spark/sql/execution/window/HashWindowGroupLimitSuite.scala`

**Interfaces:**
- Consumes: `isHashEligible` from Task 2.
- Produces: `WindowGroupLimitExec extends UnaryExecNode with CodegenSupport`
  (adds the trait); `doProduce`/`doConsume` active only when `isHashEligible`
  (when not eligible, codegen is unsupported for this operator exactly as
  today — `supportCodegen` should return `isHashEligible`).

- [ ] **Step 1: Write the failing test**

  Add to `HashWindowGroupLimitSuite.scala`:

  ```scala
  test("SPARK-XXXXX: hash path fuses into WholeStageCodegenExec") {
    val df = Seq((1, 1L, "a"), (1, 2L, "b")).toDF("k", "ts", "v")
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true") {
      val plan = dedupLatest(df).queryExecution.executedPlan
      assert(plan.exists {
        case w: org.apache.spark.sql.execution.WholeStageCodegenExec =>
          w.child.exists(_.isInstanceOf[WindowGroupLimitExec])
        case _ => false
      }, "hash-eligible WindowGroupLimitExec should fuse into whole-stage codegen")
    }
  }

  test("SPARK-XXXXX: codegen and interpreted hash paths agree") {
    val df = (0 until 500).map(i => (i % 37, i.toLong, i.toString)).toDF("k", "ts", "v")
    val codegenOn = withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      dedupLatest(df).collect().toSet
    }
    val codegenOff = withSQLConf(
      SQLConf.WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      dedupLatest(df).collect().toSet
    }
    assert(codegenOn == codegenOff)
  }
  ```

- [ ] **Step 2: Run tests to verify they fail**

  Run: `build/sbt 'sql/testOnly org.apache.spark.sql.execution.window.HashWindowGroupLimitSuite -- -z "codegen"'`
  Expected: FAIL — `WindowGroupLimitExec` does not mix in `CodegenSupport`,
  so it never appears fused inside a `WholeStageCodegenExec`.

- [ ] **Step 3: Implement `CodegenSupport`**

  Add `with CodegenSupport` to `WindowGroupLimitExec`'s declaration. Add:

  ```scala
  override def supportCodegen: Boolean = isHashEligible

  override def usedInputs: AttributeSet = AttributeSet.empty

  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  override protected def doProduce(ctx: CodegenContext): String = {
    val mapTerm = ctx.addMutableState(
      classOf[UnsafeFixedWidthAggregationMap].getName, "groupLimitMap")
    val winnersTerm = ctx.addMutableState(
      "java.util.ArrayList<UnsafeRow>", "groupLimitWinners",
      v => s"$v = new java.util.ArrayList<UnsafeRow>();")
    val initialized = ctx.addMutableState(
      CodeGenerator.JAVA_BOOLEAN, "groupLimitInitialized", v => s"$v = false;")
    val fallbackIterTerm = ctx.addMutableState(
      "scala.collection.Iterator<InternalRow>", "groupLimitFallbackIter")
    val consumeFuncName = ctx.addNewFunction("initGroupLimitMap", ...)
    val outputRow = ctx.freshName("groupLimitRow")
    val i = ctx.freshName("i")
    s"""
       | if (!$initialized) {
       |   $consumeFuncName(partitionIndex);
       |   $initialized = true;
       | }
       | if ($fallbackIterTerm != null) {
       |   while ($limitNotReachedCond $fallbackIterTerm.hasNext()) {
       |     UnsafeRow $outputRow = (UnsafeRow) $fallbackIterTerm.next();
       |     ${consume(ctx, null, outputRow)}
       |     if (shouldStop()) return;
       |   }
       | } else {
       |   int $i = 0;
       |   while ($limitNotReachedCond $i < $winnersTerm.size()) {
       |     UnsafeRow $outputRow = $winnersTerm.get($i);
       |     $i++;
       |     ${consume(ctx, null, outputRow)}
       |     if (shouldStop()) return;
       |   }
       | }
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    // Generates: project partition key, look up/insert into $mapTerm, on null
    // buffer fall back by draining $winnersTerm + remaining rows into an
    // UnsafeExternalRowSorter exactly as the interpreted spillFallback does,
    // then set $fallbackIterTerm; otherwise branch on buffer.getInt(n) == -1
    // (new key: append to $winnersTerm, write order key fields) vs compare
    // order key against buffer fields (existing key: overwrite buffer fields
    // and $winnersTerm.set(idx, row) on a win, else drop).
    ...
  }
  ```

  Implement `doConsume` by generating, per `orderSpec` field, the same
  `UnsafeRow` setter dispatch as the interpreted `writeOrderKeyIntoBuffer`,
  and the same ordinal-bound comparison, but as inline Java source strings
  (following exactly the code-generation style
  `HashAggregateExec.doConsumeWithKeys`'s slow path uses for its own
  `UnsafeFixedWidthAggregationMap` calls — copy its structure for emitting
  per-field comparisons/updates, do not invent a new codegen idiom). Reuse
  `CodeGenerator.javaType`/`CodeGenerator.getValue`/`CodeGenerator.setColumn`
  helpers already used throughout `HashAggregateExec.scala` for this.

  This is the one step in the plan too open-ended to fully inline as a
  literal diff (the exact generated-code strings depend on `orderSpec`'s
  field types, mirroring patterns already in `HashAggregateExec.scala`'s
  `doConsumeWithKeys`), but the row-by-row semantics must exactly match
  `HashRowNumberGroupLimitEvaluatorFactory`'s algorithm from Task 2 — that
  evaluator is the executable spec for this step; implement by direct,
  side-by-side translation of its Scala logic into generated Java source,
  reusing `HashAggregateExec`'s existing per-type codegen helper calls
  (`CodeGenerator.setColumn(bufferVar, dataType, ordinal, valueVar)` etc.)
  rather than writing new type-dispatch code.

- [ ] **Step 4: Run tests to verify they pass**

  Run: `build/sbt 'sql/testOnly org.apache.spark.sql.execution.window.HashWindowGroupLimitSuite'`
  Expected: PASS — all tests from Task 2 and Task 3.

- [ ] **Step 5: Run the broader regression suites again**

  Run:
  ```bash
  build/sbt 'sql/testOnly org.apache.spark.sql.DataFrameWindowFunctionsSuite'
  build/sbt 'sql/testOnly org.apache.spark.sql.execution.WholeStageCodegenSuite'
  build/sbt 'sql/testOnly org.apache.spark.sql.execution.RemoveRedundantWindowGroupLimitsSuite'
  ```
  Expected: PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala \
          sql/core/src/test/scala/org/apache/spark/sql/execution/window/HashWindowGroupLimitSuite.scala
  git commit -m "Add whole-stage codegen for the hash-based window group limit path"
  ```

---

### Task 4: Golden-file regeneration and final verification

**Files:**
- Potentially modify: TPC-DS plan-stability golden files under
  `sql/core/src/test/resources/tpcds-plan-stability*` wherever `q44`/`q67`/
  `q70` (or any other query using top-1 `row_number()`) appear, per
  `CLAUDE.md`'s golden-file policy.
- No new source files.

**Interfaces:** None (verification-only task).

- [ ] **Step 1: Run the plan-stability suites to see what (if anything) changed**

  Run: `build/sbt 'sql/testOnly org.apache.spark.sql.TPCDSV1_4_PlanStabilitySuite'`
  (and the `WithStats`/`v2_7` variants if present) and read the diff output
  for any failing golden comparisons.

- [ ] **Step 2: Regenerate only the golden files that actually changed**

  Per `CLAUDE.md`: do not hand-edit `.sql.out`/plan golden files. If Step 1
  reports mismatches caused by this change (fewer `Sort` nodes, a fused
  `WholeStageCodegenExec`), regenerate via the suite's documented
  regeneration flag/env var (check `SQLQueryTestSuite`/plan-stability suite
  class doc for the exact mechanism, e.g. `SPARK_GENERATE_GOLDEN_FILES=1`)
  and then **review the diff**, confirming every changed line is explained by
  "sort removed" or "fused into whole-stage codegen" — not by an unrelated or
  unexpected plan change.

- [ ] **Step 3: Run the full affected-module test suites**

  Run:
  ```bash
  build/sbt 'sql/testOnly org.apache.spark.sql.execution.window.*'
  build/sbt 'sql/testOnly org.apache.spark.sql.DataFrameWindowFunctionsSuite'
  build/sbt 'sql/testOnly org.apache.spark.sql.execution.RemoveRedundantWindowGroupLimitsSuite'
  build/sbt 'catalyst/testOnly org.apache.spark.sql.catalyst.optimizer.InferWindowGroupLimitSuite'
  build/sbt 'catalyst/testOnly org.apache.spark.sql.internal.SQLConfSuite'
  ```
  Expected: all PASS.

- [ ] **Step 4: Lint check the changed files**

  Run (per `CLAUDE.md`'s guidance):
  ```bash
  { git diff --name-only --diff-filter=ACM upstream/master; } \
    | grep -E '\.(scala|java)$' | sort -u \
    | xargs -r awk 'length>100 && $0 !~ /^[[:space:]]*(import|package) / && $0 !~ /https?:\/\// \
        {print FILENAME":"FNR": "length" chars"}'
  grep -rn -P "[^\x00-\x7F]" $(git diff --name-only --diff-filter=ACM upstream/master | grep -E '\.(scala|java)$')
  ```
  Fix any hits before proceeding.

- [ ] **Step 5: Commit any golden-file regenerations**

  ```bash
  git add sql/core/src/test/resources/tpcds-plan-stability*
  git commit -m "Regenerate TPC-DS plan-stability goldens for hash-based window group limit"
  ```
  (Skip this commit entirely if Step 1 found no golden-file diffs.)

## Self-Review Notes

- **Spec coverage**: Section 3 (scope/eligibility) -> Task 2's `isHashEligible`
  and its eligibility test. Section 4.1 (one class) -> Task 2/3 modify
  `WindowGroupLimitExec` in place, no new case class. Section 4.2
  (`requiredChildOrdering`) -> Task 2 Step 4. Section 4.3 (algorithm) -> Task
  2's evaluator. Section 4.4 (codegen) -> Task 3. Section 4.5 (spill
  fallback) -> Task 2's `spillFallback`. Section 5 (planner) -> no task
  needed, confirmed unchanged, covered by Task 2/3's regression-suite runs.
  Section 6 (config) -> Task 1. Section 7 (testing) -> distributed across
  Task 2/3's test steps plus Task 4's suite runs and golden regeneration.
- **Type consistency check**: `isHashEligible` (Task 2) is referenced
  identically in Task 3's `supportCodegen`; `HashRowNumberGroupLimitEvaluatorFactory`'s
  constructor signature (Task 2) is not altered by Task 3 (codegen is a
  parallel, independent code path, not a caller of the evaluator factory).
  `WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED` (Task 1) and
  `windowGroupLimitHashPushdownEnabled` (Task 1) are the only names used for
  this config across Tasks 2-4.
