# Indexed SortMergeJoin (embedded range index for equi-key + residual range joins)

## Background

SPARK-8682 (already merged) added `BroadcastRangeJoinExec`: when one join
input is small enough to broadcast and the join condition is a recognizable
range-predicate shape (`ExtractRangeJoinKeys` in `patterns.scala`), the
broadcast side is built into a `RangeIndex`/`RangeRelation` instead of a plain
row array, and the stream side probes it with an augmented interval tree
(`IntervalIndex`/`LongIntervalIndex`) or a sorted-array binary search
(`PointIndex`/`LongPointIndex`).

Separately, PR #21109 (closed, unmerged; JIRA SPARK-24020) proposed a
different optimization for a different shape: a *real* equi-join key `X`
plus a range condition on a secondary column `Y`
(`t1.X = t2.X AND t1.Y BETWEEN t2.Y - d AND t2.Y + d`), where both tables are
too large to broadcast. Its approach modified `SortMergeJoinExec` directly to
replace the per-`X`-group row buffer with a sliding window keyed on `Y`.

This spec targets that same real-world shape (equi key `X` + residual range
condition on `Y`, neither side broadcastable) by embedding a range index
directly into `SortMergeJoinScanner`'s per-key buffered-match step, reusing
SPARK-8682's existing `RangeRelation` index classes rather than inventing a
new data structure.

### Why this is a real problem, not just a theoretical one

`SortMergeJoinScanner.bufferMatchingRows()` buffers every buffered-side row
sharing an equi-key into an `ExternalAppendOnlyUnsafeRowArray`. For every
streamed-side row sharing that key, the scanner (via
`SortMergeJoinEvaluatorFactory`) calls `bufferedMatches.generateIterator()`
again and linearly scans looking for rows that satisfy the residual
condition. `ExternalAppendOnlyUnsafeRowArray.generateIterator()`
(`ExternalAppendOnlyUnsafeRowArray.scala`) is backed by an in-memory
`ArrayBuffer` until the group spills to `UnsafeExternalSorter`; once spilled,
`generateIterator()` calls `spillableArray.getIterator(startIndex)`, which
re-reads the spilled data from the start on *every* call. A skewed equi-key
whose buffered-side group spills therefore pays a full re-read of that
group's spilled data once per streamed-side row sharing the key - on top of
the O(M x N) comparisons against the full group instead of just the rows an
index would return as candidates. This is the same architecture, independent
of workload-specific numbers.

## Goals

- Recognize a range-predicate shape (reusing `RangeJoinKind` /
  `RangePredicate` classification: `PointInRangeJoin`, `IntervalOverlapJoin`,
  `PartialRangeJoin`) inside the residual `condition` of an equi-join, where
  the range predicate's columns are distinct from the equi-join's key
  columns.
- When a buffered-side key group exceeds a small row-count threshold, build
  an index over that group once and probe it per streamed row, instead of
  linearly rescanning the group (and, in the spilled case, re-reading disk)
  for every streamed row.
- Cover every join type that shares `SortMergeJoinScanner`: Inner, LeftOuter,
  RightOuter, LeftSemi, LeftAnti, and `ExistenceJoin`. One change to the
  scanner's candidate-iteration step covers all of them.
- Zero regression for key groups at or below the threshold (the common
  unique-or-low-cardinality-key case): behavior is byte-for-byte the
  existing `bufferedMatches.generateIterator()` path.

## Non-goals (this version)

- `FullOuter`, which uses a separate scanner (`SortMergeFullOuterJoinScanner`)
  with plain in-memory `ArrayBuffer`s and no spill path - out of scope.
- Whole-stage codegen support. `SortMergeJoinExec.supportCodegen` returns
  `false` when a range-join spec is attached, so the plan falls back to the
  existing interpreted path (an already-supported, already-correct code
  path - the same technique this file uses today to disable codegen for
  `FullOuter`/`ExistenceJoin` under certain confs). Codegen is a tracked
  follow-up.
- Changing `BroadcastRangeJoinExec`, `ExtractRangeJoinKeys`, or the
  broadcast-range-join planning path. Those are untouched; the new extractor
  and the probe dispatcher are factored out for reuse, not rewritten.
- A new interval-tree or point-index data structure. `IntervalIndex`,
  `LongIntervalIndex`, `PointIndex`, and `LongPointIndex` in
  `RangeRelation.scala` are reused as-is.
- Memory-contention fallback / `TaskMemoryManager`-budgeted index allocation.
  The index is built from plain heap arrays per key group and dropped (GC's
  job) when the group advances, exactly like `BroadcastRangeJoinExec`'s
  broadcast relation has no special fallback. If a pathologically large
  single-key group turns out to need one, that's a follow-up motivated by a
  real report, not a speculative feature.

## Design

### 1. Condition recognition

New extractor in `patterns.scala`, e.g. `ExtractEquiRangeJoinKeys`, returning
the same shape as `ExtractRangeJoinKeys.ReturnType`
(`(leftKeys: Seq[Expression], rightKeys: Seq[Expression], RangeJoinKind)`) so
it plugs into the same kind of consumer. Given an equi-join's `nonEquiCond`
(from `ExtractEquiJoinKeys`), it splits into conjuncts and finds one
`RangeJoinKind` shape among them, reusing `ExtractRangeJoinKeys`'s existing
shape-matching (`pairJoin`/`partialRange`/`place`/`supportedKeys`/
`supportedType`), refactored into methods shared by both extractors rather
than duplicated. A predicate is only eligible if its columns are disjoint
from the equi-join's own `leftKeys`/`rightKeys` columns - otherwise the
"range" condition is really re-describing the equi-key and there is nothing
to index.

The full original `nonEquiCond` remains `SortMergeJoinExec`'s `condition`,
unreduced, and is still evaluated on every row the index hands back. The
index only narrows which rows are scanned; it never replaces the semantic
check. This is the same superset-then-recheck contract `BroadcastRangeJoinExec`
already relies on, so it introduces no new correctness surface.

### 2. Shared probe dispatch

`BroadcastRangeJoinExec.makeCandidateFn` already contains the "dispatch on
`LongIntervalIndex`/`LongPointIndex`/`IntervalIndex`/`PointIndex`, evaluate
the probe key(s), call `overlapping`/`upTo`/`from`" logic, with no
broadcast-specific content. Hoist it out of `BroadcastRangeJoinExec`'s
companion object into a shared, non-private location in `RangeRelation.scala`
(e.g. `private[joins] object RangeRelationProbe`), used verbatim by both
`BroadcastRangeJoinExec` and the new `SortMergeJoinScanner` code. This is the
only change to existing range-join code; `IntervalIndex`, `LongIntervalIndex`,
`PointIndex`, `LongPointIndex`, `RangeRelationBroadcastMode`, and
`BroadcastRangeJoinExec`'s own behavior are otherwise untouched.

### 3. `SortMergeJoinExec.rangeJoinSpec`

New optional case-class field, following the existing precedent of
`isSkewJoin: Boolean = false`:

```scala
case class SortMergeRangeJoinSpec(
    rangeJoinKind: RangeJoinKind,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression])

case class SortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false,
    rangeJoinSpec: Option[SortMergeRangeJoinSpec] = None) extends ShuffledJoin
```

Wired in `SparkStrategies.createSortMergeJoin()`:

```scala
def createSortMergeJoin() = {
  if (canMerge(joinType) && RowOrdering.isOrderable(leftKeys)) {
    val rangeJoinSpec = if (conf.sortMergeJoinRangeIndexEnabled) {
      nonEquiCond.flatMap(ExtractEquiRangeJoinKeys.unapply(_, leftKeys, rightKeys, left, right))
        .map { case (lk, rk, kind) => SortMergeRangeJoinSpec(kind, lk, rk) }
    } else {
      None
    }
    Some(Seq(joins.SortMergeJoinExec(
      leftKeys, rightKeys, joinType, nonEquiCond, planLater(left), planLater(right),
      rangeJoinSpec = rangeJoinSpec)))
  } else {
    None
  }
}
```

Inside `SortMergeJoinExec`, the existing `((streamedPlan, streamedKeys),
(bufferedPlan, bufferedKeys))` lazy val (already keyed off `joinType`, at
line 167 today) is extended with a parallel orientation for
`rangeJoinSpec.leftKeys`/`rightKeys` into streamed/buffered range keys, bound
to each side's output via `BindReferences.bindReferences`. The bound range
keys, plus `rangeJoinKind`, are passed into `SortMergeJoinEvaluatorFactory`
and from there into `SortMergeJoinScanner`'s constructor.

### 4. `SortMergeJoinScanner`: the probe-time fix

New constructor parameters: bound buffered range keys, bound streamed range
keys, `RangeJoinKind`, and the threshold conf value (all `Option`/unused when
no range-join spec is attached). New method, replacing direct calls to
`bufferedMatches.generateIterator()` in every `SortMergeJoinEvaluatorFactory`
join-type branch:

```scala
def candidateIterator(streamedRow: InternalRow): Iterator[InternalRow]
```

Note the return type is `Iterator[InternalRow]`, not `Iterator[UnsafeRow]`: index
probe results (`overlapping`/`upTo`/`from` in `RangeRelation.scala`) are typed
`Iterator[InternalRow]`, even though every row is in practice an `UnsafeRow`
(`InternalRow.copy()` on an `UnsafeRow` returns an `UnsafeRow`). The call
sites in `SortMergeJoinEvaluatorFactory` and `OneSideOuterIterator` that
currently declare `rightMatchesIterator: Iterator[UnsafeRow]` loosen to
`Iterator[InternalRow]` accordingly; this is harmless since every use passes
the result straight into `JoinedRow`, which already takes `InternalRow`.

Behavior:

- No range spec, or `bufferedMatches.length <= rangeIndexThreshold`: identical
  to today, `bufferedMatches.generateIterator()`. This is the zero-regression
  path and covers the common unique/low-cardinality-key case untouched.
- Otherwise: lazily build an index for the *current* key group (cached in a
  scanner-private field, invalidated whenever `bufferMatchingRows()` starts
  buffering a new group) via:

  ```scala
  val mode = RangeRelationBroadcastMode(boundBufferedRangeKeys, RangeIndexKind(rangeJoinKind))
  val index: RangeRelation = mode.transform(bufferedMatches.generateIterator(), Some(bufferedMatches.length))
  ```

  This is one sequential read of the group (spilled or not) to build the
  index, replacing what is today one re-read per streamed row. Each
  streamed row then probes the cached index via the shared
  `RangeRelationProbe` dispatcher from step 2, using the bound streamed range
  keys evaluated against `streamedRow`.
- `RangeRelationBroadcastMode`'s existing row-copy behavior
  (`IntervalIndex.apply`/`LongIntervalIndex.apply`/etc. all call `r.copy()`
  when staging) already handles the hazard that
  `ExternalAppendOnlyUnsafeRowArray`'s spilled-array iterator repoints a
  single reused `UnsafeRow` on every `next()` - no new handling needed here.

This single scanner change is used by every `SortMergeJoinEvaluatorFactory`
branch that already calls `smjScanner.getBufferedMatches.generateIterator()`
today: Inner, LeftOuter, RightOuter, LeftSemi, LeftAnti, and `ExistenceJoin`.

### 5. Codegen

`SortMergeJoinExec.supportCodegen` returns `false` when `rangeJoinSpec` is
defined, so the plan falls back to the interpreted path above - the same
approach this file already uses to gate codegen for `FullOuter`
(`ENABLE_FULL_OUTER_SORT_MERGE_JOIN_CODEGEN`) and `ExistenceJoin`
(`ENABLE_EXISTENCE_SORT_MERGE_JOIN_CODEGEN`). Whole-stage codegen support
(mirroring `BroadcastRangeJoinExec`'s `genCandidateIterator`/
`genProbeKeys`/`genLongProbeKeys`) is an explicit follow-up, not part of this
change.

### 6. Configuration

Two new confs in `SQLConf.scala`, next to `BROADCAST_RANGE_JOIN_ENABLED` and
`SORT_MERGE_JOIN_EXEC_BUFFER_*`:

| Config Key | Default | Description |
| :--- | :---: | :--- |
| `spark.sql.join.sortMergeJoin.rangeIndex.enabled` | `true` | Recognize an equi-join's residual range predicate and build a per-key-group range index in `SortMergeJoinScanner` instead of a linear rescan. Defaults on, following `broadcastRangeJoin.enabled`'s precedent: it only replaces a strictly more expensive path. |
| `spark.sql.join.sortMergeJoin.rangeIndex.threshold` | `16` | Minimum buffered-side row count for a key group before an index is built for it, matching `RangeIndex.SMALL_BUILD_OVERLAP_ROWS` (the existing cutoff below which the index classes themselves fall back to a linear scan). |

## Testing

- `ExtractEquiRangeJoinKeysSuite`, mirroring `ExtractRangeJoinKeysSuite`'s
  structure: all three `RangeJoinKind` shapes, plus negative cases (range
  predicate reuses an equi-join column, no equi-join present, unsupported
  type).
- `SortMergeJoinSuite` additions: correctness for all three `RangeJoinKind`
  shapes across Inner, LeftOuter, RightOuter, LeftSemi, LeftAnti, and
  `ExistenceJoin`, asserting output matches the index disabled
  (`rangeIndex.enabled=false`) path. Includes a case that forces
  `sortMergeJoinExecBufferSpillThreshold` low enough to exercise the spilled
  path, to directly cover the bug motivating this change.
- Benchmark: extend `RangeJoinBenchmark` with a skewed equi-key cardinality
  (many rows per key on the buffered side) scenario for `SortMergeJoinExec`,
  comparing `rangeIndex.enabled=true` against `false`, including a variant
  large enough to force a spill.

## JIRA

Same JIRA as before (SPARK-24020): this supersedes the shuffled-range-join
(AQE/`ShuffledHashJoinExec`-based) approach previously drafted here, which is
not being pursued.

## Follow-ups (explicitly out of scope here)

- Whole-stage codegen support for the indexed probe path.
- Outer/semi/anti-equivalent handling for `FullOuter` (separate scanner,
  no spill path today, lower priority).
- Memory-budgeted index construction / fallback under contention, if a real
  workload shows the plain-heap-array approach is insufficient for a single
  pathologically large key group.
