# Design: Hash-Based `WindowGroupLimitExec` for `ROW_NUMBER() = 1`

**Status**: Approved for implementation planning
**JIRA**: Not yet created — per `CLAUDE.md`'s PR workflow, created from the PR
title when implementation starts, not during design.
**Target version**: `4.4.0` — per `CLAUDE.md`'s versioning policy, this is a
normal `master`-targeted change expected to also ship in the next
`branch-<N>.x` (not master-only: it's a planner/exec optimization behind a
default-on config flag, with no binary-incompatible or breaking surface), so
it uses `branch-4.x`'s version from `dev/next_version_candidates.py`
(`master 5.0.0`, `branch-4.x 4.4.0`), not `master`'s.
**Scope**: Phase 1 of a larger effort described in
`/Users/yumwang/work/hdmi-cac/DESIGN_HASH_BASED_CODEGEN_WINDOW_GROUP_LIMIT.md`.
This spec intentionally narrows that document's scope; see "Explicitly out of
scope" below.

## 1. Problem

`WindowGroupLimitExec` (`sql/core/src/main/scala/org/apache/spark/sql/execution/window/WindowGroupLimitExec.scala`)
prunes rows before `WindowExec` for top-K-per-partition queries
(`ROW_NUMBER()`/`RANK()`/`DENSE_RANK()` filtered by a small limit). Today it
always declares `requiredChildOrdering = partitionSpec ++ orderSpec`
(`WindowGroupLimitExec.scala:64-65`), forcing a full `SortExec` of every row in
every partition in both `Partial` and `Final` stages, even though the operator
only ever keeps the first `limit` rows per group and discards the rest. For the
dominant real-world case — `ROW_NUMBER() OVER (PARTITION BY k ORDER BY o DESC) = 1`
("keep latest/best row per key") — this means sorting (and potentially
spilling) the entire input just to keep one row per key.

Note that `WindowExec` itself (the operator downstream of `WindowGroupLimitExec`
that actually computes `ROW_NUMBER()`) has its own, independent
`requiredChildOrdering` and will still force a sort of its input. This design
does not eliminate that sort — it shrinks what gets sorted. Today's sort is
over the *entire* input (every row of every partition); after this change,
`WindowGroupLimitExec` bounds its output to at most one row per partition key
before that sort happens, so `WindowExec`'s own sort is over at most
(number of distinct partition keys) rows instead of the full input. The
expensive, large-scale sort is eliminated; a cheap, small, already-bounded
sort remains.

## 2. Relationship to existing work

Two other branches touch this area; this work is an **independent
replacement** that does not build on either:

- `SPARK-61743` added whole-stage codegen to the *existing* sort-based
  algorithm (same `requiredChildOrdering`, same iterators, just codegen'd).
  This spec's hash path makes that sort requirement (and therefore that
  codegen work) unnecessary for the cases it covers, so it supersedes rather
  than extends it.
- `SPARK-61729` added a logical-plan rule that eliminates a redundant
  `Window`/`Filter` entirely when top-1 `row_number()` is provably safe to
  drop downstream. That rule operates one level up in the logical plan and is
  orthogonal — it keeps working unchanged regardless of how
  `WindowGroupLimitExec` executes physically.

## 3. Scope for this phase

**In scope:** `rankLikeFunction.isInstanceOf[RowNumber] && limit == 1`, for
both `Partial` and `Final` modes, any partition-key expression type, and
order-key expressions whose data types are all `UnsafeRow`-mutable
(`UnsafeRow.isMutable`: numeric primitives, `Date`, `Timestamp`/`TimestampNTZ`,
fixed-precision `Decimal`, `CalendarInterval` — not `String`, `Binary`,
`Array`, `Map`, or `Struct`). See Section 4.3 for why this additional
constraint on the order key (not present in the original draft of this spec)
is required — it is not a simplification of convenience, it follows from how
in-place value updates are actually possible in Spark's hash map primitives.
Partition-key types are unconstrained because the partition key is only ever
used as an opaque hash-map key, never mutated in place.

When the order key doesn't satisfy this constraint, planning falls back to
today's sort-based `WindowGroupLimitExec` entirely (a static, plan-time
decision, same as the `Rank`/`DenseRank`/`limit > 1` exclusions) — not a
runtime fallback.

**Why limited to `RowNumber` with `limit == 1`:**

- *Correctness, not just risk reduction.* `Rank`/`DenseRank` with `limit == 1`
  must retain **every** row tied for the best order key (ties share rank 1).
  That is not a bounded, O(1)-space-per-key accumulation — in the worst case
  (all rows in a group tied) it requires keeping the whole group. The existing
  sort-based iterators (`RankLimitIterator`/`DenseRankLimitIterator`) already
  handle this correctly by relying on sorted input to keep ties adjacent; a
  hash-based rewrite of this case is a separate, harder problem deferred
  indefinitely (not just to the K>1 follow-up).
- `RowNumber` ties, by contrast, are already arbitrary/non-deterministic under
  current semantics (whichever row the sort happens to place first wins rank
  1). Picking an arbitrary winner via hash-based ArgMax is a different
  arbitrary choice, not a correctness regression.
- `K > 1` (general bounded top-K buffer) is explicitly deferred to a follow-up
  PR/JIRA once this phase is validated, per the 98.66%-of-queries-are-K=1
  statistic in the parent design doc.

All ineligible cases (`Rank`, `DenseRank`, or `limit > 1`) continue to use
today's `WindowGroupLimitExec` code path, completely unchanged.

## 4. Operator design

### 4.1 One class, not two

`WindowGroupLimitExec` is evolved in place rather than introduced as a new
`HashWindowGroupLimitExec` sibling class. Two existing rules pattern-match on
the concrete case class and its `Partial`/`Final` tag:

- `RemoveRedundantWindowGroupLimits` (`sql/core/.../execution/RemoveRedundantWindowGroupLimits.scala:37`)
  matches `WindowGroupLimitExec(_, _, _, _, Final, child)` and tries to strip
  the paired `Partial` node.
- `InferWindowGroupLimit` (`sql/catalyst/.../optimizer/InferWindowGroupLimit.scala`)
  produces the logical `WindowGroupLimit` node that `SparkStrategies.WindowGroupLimit`
  (`SparkStrategies.scala:759-769`) turns into the Partial→Final physical pair.

Keeping one class means both rules keep working with at most a small tweak
(e.g., `RemoveRedundantWindowGroupLimits` needs no change at all, since it
only inspects mode/shape, not the execution strategy inside). A second class
would require duplicating or branching all of this integration.

### 4.2 Eligibility and `requiredChildOrdering`

Add a private `isHashEligible: Boolean` val computed from
`rankLikeFunction.isInstanceOf[RowNumber] && limit == 1` (also gated by the new
config flag, see Section 7). `requiredChildOrdering` becomes:

```scala
override def requiredChildOrdering: Seq[Seq[SortOrder]] =
  if (isHashEligible) Seq(Nil) else Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
```

`output`, `outputPartitioning`, and the per-mode `requiredChildDistribution`
are unchanged — `SparkStrategies.WindowGroupLimit` still always plans a
Partial→Final pair; only the sort requirement (and therefore whether
`EnsureRequirements` injects a `SortExec`) changes.

### 4.3 Algorithm

**Correction from the original draft of this spec:** the parent design doc's
codegen sketch calls `loc.updateValue(...)` on a `BytesToBytesMap.Location` to
overwrite a slot's value in place. That method does not exist on
`BytesToBytesMap.Location` — only `append`. In-place value mutation in Spark's
Tungsten maps works *only* because `UnsafeFixedWidthAggregationMap` restricts
every buffer field to a fixed byte width forever (`Aggregate.isAggregateBufferMutable`),
so the same bytes can be rewritten via `UnsafeRow` setters without ever
changing the record's total length. A "full candidate row" is generally
variable-width (strings, etc.), so it cannot be the value of a map slot that
needs in-place overwriting. The design below reuses
`UnsafeFixedWidthAggregationMap` for exactly what it's good at (a fixed-width,
mutable, spill-aware index) and keeps full row storage outside the Tungsten
map entirely.

Both `Partial` and `Final` become blocking hash operators (consume all input,
then emit), modeled on `HashAggregateExec`'s with-keys codegen shape
(`HashAggregateExec.scala` `doProduceWithKeys`/`doConsumeWithKeys`), with two
pieces of state:

1. **`UnsafeFixedWidthAggregationMap`** (existing class, reused as-is, not
   reimplemented): grouping key = `partitionSpec` projected via
   `UnsafeProjection.create(partitionSpec, child.output)` (any types — the
   map places no mutability constraint on keys, only on the aggregation
   buffer). Aggregation buffer schema = `orderSpec`'s data types (one field
   per order column, each `UnsafeRow`-mutable per Section 3) followed by one
   extra `IntegerType` field, `winnerIndex`. `emptyAggregationBuffer` has
   `winnerIndex = -1` as a sentinel for "just inserted, not yet populated."
2. **`winnerRows`**: a plain growable JVM-heap array (e.g.
   `scala.collection.mutable.ArrayBuffer[UnsafeRow]`) of full candidate rows,
   each a `.copy()` of a `child.output`-shaped `UnsafeRow`. This is *not*
   Tungsten/`TaskMemoryManager`-accounted memory — see Section 4.5 for the
   consequence and why it's bounded and acceptable.

- **Order-key comparison**: a comparator generated once via
  `GenerateOrdering`, bound against `BoundReference`s at ordinals
  `0 until orderSpec.length` with each `orderSpec(i)`'s direction/null
  ordering — i.e. the same *kind* of generated comparator
  `RankLimitIterator`/`DenseRankLimitIterator` already use, just bound to
  ordinal positions instead of named attributes so it can compare the
  aggregation buffer's leading fields against a separately-projected
  order-key-only row (see below) without needing them to share a schema
  beyond those leading ordinals.
- **Per row**:
  1. `buffer = map.getAggregationBufferFromUnsafeRow(partitionKey)`. If
     `null`, the map could not acquire memory — go to the spill fallback
     (Section 4.5).
  2. If `buffer.getInt(orderSpec.length) == -1` (new key, just inserted):
     append `row.copy()` to `winnerRows`, write its index into
     `buffer.setInt(orderSpec.length, newIndex)`, and copy the row's
     order-key values into the buffer's leading fields (via the standard
     per-type `UnsafeRow` setters codegen already uses elsewhere, e.g.
     `setLong`/`setInt`/`setDouble`/`setDecimal`).
  3. Else (existing key): compare the incoming row's order key (evaluated
     directly from the row, not yet materialized into its own row object)
     against the buffer's stored order-key fields via the generated
     comparator. If the incoming row wins: overwrite the buffer's order-key
     fields in place (same fixed width every time — this is exactly the case
     `UnsafeRow` in-place mutation supports) and overwrite
     `winnerRows(buffer.getInt(orderSpec.length)) = row.copy()`. Else: drop
     the row — zero allocation beyond the comparison itself.
- **At end of input**: iterate `winnerRows` directly and emit each row. (No
  need to iterate the Tungsten map's own bytes at all — `winnerRows` already
  holds exactly the current best-known full row per key, deduplicated by
  construction, which is the actual output.)
- **Empty `partitionSpec`** (global top-1, no `PARTITION BY`): handled by the
  same algorithm with a zero-length grouping key — the map holds exactly one
  entry, `winnerRows` holds at most one row. No special-casing needed.

### 4.4 Whole-stage codegen

`doProduce`/`doConsume` follow the `HashAggregateExec` pattern: `doConsume`
generates the lookup/compare/insert-or-overwrite-or-drop logic inline (calling
into `UnsafeFixedWidthAggregationMap.getAggregationBufferFromUnsafeRow`
exactly like `HashAggregateExec`'s own "slow path" codegen does, plus the
`winnerRows` bookkeeping from Section 4.3); `doProduce` generates the
end-of-input loop over `winnerRows` that drives the downstream pipeline. No
"fast hash map" two-level specialization
(`RowBasedHashMapGenerator`/`VectorizedHashMapGenerator`) in this phase.
Adding one is a possible future optimization, not required for this phase's
correctness or its asymptotic win.

### 4.5 Memory management and spill fallback — zero new spill code

**`winnerRows` is plain JVM heap, not Tungsten-accounted.** Its size is
bounded by the number of distinct partition keys seen so far (each key's
slot is assigned once and overwritten in place thereafter, never appended to
again), so it grows in lockstep with the `UnsafeFixedWidthAggregationMap`'s
own key count — the same signal that already bounds the map's memory. It is
not an unbounded-growth risk, but it is heap garbage proportional to the
number of overwrites over the task's lifetime (replaced rows become garbage,
exactly as replacing a scalar `MaxBy` buffer value already does today, just
at the granularity of a whole row instead of a column) — a performance/GC
consideration, not a correctness one, and not materially different from
existing mutable-accumulator patterns elsewhere in Spark.

When `map.getAggregationBufferFromUnsafeRow(...)` returns `null` (the
existing, already-handled signal `TungstenAggregationIterator` reacts to for
`HashAggregateExec`), do **not** write new spill or merge logic. Instead:

1. Create an `UnsafeExternalRowSorter` — the exact class `SortExec` itself
   uses for its own (spill-capable) sorting, via the same construction
   `SortExecSorterHolder.createSorter()` uses: `ordering = RowOrdering.create(sortOrder, output)`
   with `sortOrder = partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec`
   — i.e. **the exact ordering `WindowGroupLimitExec.requiredChildOrdering`
   already specifies today**, direction and null-ordering included (this is
   why `UnsafeExternalRowSorter` is the right reuse target instead of
   `UnsafeKVExternalSorter`: the latter's key-schema-based comparator loses
   `SortOrder` direction/null-ordering information, which would silently
   mis-sort any `DESC` or explicit `NULLS LAST`/`NULLS FIRST` order spec).
2. `insertRow(...)` every row currently in `winnerRows` (already-deduplicated
   best-known candidates), then `insertRow(...)` every remaining unread row
   from the input iterator.
3. Call `.sort()` to get a correctly globally-ordered `Iterator[InternalRow]`
   (spilling to disk internally as needed — this is `SortExec`'s own
   production-hardened spill path, not new code).
4. Feed that sorted iterator into the **existing, already-correct**
   `GroupedLimitIterator` + `SimpleLimitIterator` — i.e., finish the task
   using today's interpreted sort-based `WindowGroupLimitExec` logic,
   unchanged.
5. Free the `UnsafeFixedWidthAggregationMap` (`map.free()`); its bytes are no
   longer needed since `winnerRows` already held the decoded, authoritative
   per-key candidates.

This means the OOM-safety guarantee requires zero new correctness-critical
sorting/merging code: both the sorter and the post-sort grouping/limiting
logic already exist and are already tested (`SortExec`'s sorter;
`WindowGroupLimitExec`'s own iterators).

## 5. Planner integration

No changes needed to `SparkStrategies.WindowGroupLimit` — it keeps
unconditionally planning a `Partial` → `Final` pair of `WindowGroupLimitExec`;
the new `isHashEligible` branching lives entirely inside the operator
(`requiredChildOrdering`, `doExecute`, `doProduce`/`doConsume`).
`RemoveRedundantWindowGroupLimits` and `InferWindowGroupLimit` need no changes
for the same reason (Section 4.1).

## 6. Config

A single new config, matching the parent doc's key but dropping the
`maxLimit` config for this phase (eligibility is hard-coded to `limit == 1`
until K>1 lands, so a configurable threshold has nothing to vary yet):

```scala
val WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED =
  buildConf("spark.sql.windowExec.groupLimit.hashPushdown.enabled")
    .doc("When true, WindowGroupLimitExec uses an in-memory hash-based " +
         "accumulator and whole-stage codegen instead of full sorting, " +
         "for ROW_NUMBER() window group limits of 1.")
    .version("4.4.0")
    .booleanConf
    .createWithDefault(true)
```

## 7. Testing strategy

- **Correctness equivalence**: for the eligible case (`RowNumber`, `limit=1`),
  randomized tests comparing hash-path output against the config-disabled
  (interpreted sort-based) path's output as sets, across partition key types
  including strings/structs (unconstrained), `UnsafeRow`-mutable order-key
  types (numeric, date, timestamp, decimal) with nulls
  (`NullsFirst`/`NullsLast`), ties, and multi-column order specs. Extend
  `DataFrameWindowFunctionsSuite` and/or add a focused new suite alongside
  `RemoveRedundantWindowGroupLimitsSuite`.
- **Eligibility-boundary test**: a `String`-typed (or other non-mutable-type)
  order key must plan identically to today (sort-based, no hash path),
  verifying the Section 3 type restriction is actually enforced at planning
  time.
- **Spill/fallback test**: force a low task-memory budget
  (`withSQLConf`/memory manager test hooks, same style as existing
  `HashAggregateExec` spill tests) and assert correctness is preserved when
  the hash map overflows mid-task.
- **Plan-shape test**: `explain` assertions confirming no `SortExec` is
  injected for the eligible case, and that ineligible cases
  (`Rank`/`DenseRank`, `limit > 1`) produce an unchanged plan (still sorted).
- **Config-off equivalence**: with
  `WINDOW_GROUP_LIMIT_HASH_PUSHDOWN_ENABLED=false`, plan/behavior must exactly
  match current (pre-change) output — this is the safety net for the whole
  change.
- TPC-DS plan-stability goldens likely need mechanical regeneration wherever
  `q44`/`q67`/`q70`-style top-1 `row_number()` queries appear, per
  `CLAUDE.md`'s golden-file regeneration guidance (regenerate, then review the
  diff).

## 8. Explicitly out of scope for this PR

- `K > 1` generalized bounded top-K buffer (follow-up PR/JIRA).
- A hash path for `Rank`/`DenseRank` at any limit (deferred indefinitely per
  Section 3's correctness argument, not merely to the K>1 follow-up).
- The parent design doc's custom off-heap `UnsafeBoundedPriorityQueue` and
  custom `MemoryConsumer` integration — superseded by reusing
  `UnsafeFixedWidthAggregationMap` + `UnsafeExternalRowSorter` as described in
  Sections 4.3 and 4.5.
- A configurable `hashPushdown.maxLimit` (nothing to vary while limit is
  hard-coded to 1).
- Fast two-level hash map codegen specialization (possible future
  optimization).
- Any AQE-specific adaptive re-planning beyond what `EnsureRequirements` /
  `RemoveRedundantWindowGroupLimits` already provide.
