# Shuffled range join (AQE-time composite hash+range index)

## Background

SPARK-8682 (already merged) added `BroadcastRangeJoinExec`: when one join
input is small enough to broadcast and the join condition is a recognizable
range-predicate shape (`ExtractRangeJoinKeys` in `patterns.scala`), the
broadcast side is built into a `RangeIndex`/`RangeRelation` instead of a plain
row array, and the stream side probes it. `ExtractRangeJoinKeys` does not
require, or look at, any equality predicate - it classifies the condition
purely from its range predicates (`<`, `<=`, `>`, `>=`) into
`PointInRangeJoin`, `IntervalOverlapJoin`, or `PartialRangeJoin`. An equality
predicate elsewhere in the condition is just evaluated as a residual filter
on each index hit.

Separately, PR #21109 (closed, unmerged; JIRA SPARK-24020) proposed a
different, older optimization for a different shape: a *real* equi-join key
`X` plus a range condition on a secondary column `Y`
(`t1.X = t2.X AND t1.Y BETWEEN t2.Y - d AND t2.Y + d`), where both tables are
too large to broadcast. Its approach modified `SortMergeJoinExec` directly to
replace the per-`X`-group row buffer with a sliding window keyed on `Y`.

This spec is **not** a continuation of PR #21109's approach. It targets the
same real-world shape (equi key `X` + residual range condition on `Y`) but
solves it differently: by giving the already-existing AQE machinery that
converts `SortMergeJoinExec` to `ShuffledHashJoinExec`
(`ConvertSortMergeJoinToShuffledHashJoin`) a further option - when the build
side turns out small post-shuffle *and* the residual condition contains a
range-predicate shape, build a composite index instead of a plain hash
table, so the per-`X`-bucket scan-and-filter becomes a per-`X`-bucket range
probe.

## Goals

- Recognize a range-predicate shape (reusing `RangeJoinKind` /
  `RangePredicate` classification: `PointInRangeJoin`, `IntervalOverlapJoin`,
  `PartialRangeJoin`) inside the residual `condition` of an equi-join, where
  the equi-join keys are unrelated to the range predicate's columns.
- At AQE time, when a `ShuffledHashJoinExec` node's build side is small
  enough per real post-shuffle statistics, replace the plain per-key row
  list with a per-key `RangeRelation`, cutting the residual-condition
  evaluation from "every row in the bucket" to "every row the range probe
  returns."
- Cover Inner joins only in this version.

## Non-goals (this version)

- Outer/semi/anti join semantics for the range-indexed path (unmatched-row
  bookkeeping interacts with the index differently; left as follow-up).
- The PR #21109 sliding-window `SortMergeJoinExec` approach. If neither side
  is small enough for a hash map, this design does not help; that remains a
  separate, unimplemented idea.
- Changing `BroadcastRangeJoinExec`, `ExtractRangeJoinKeys`, or the
  broadcast-range-join planning path. Those are untouched; this is a new,
  parallel capability for the shuffled case.

## Design

### 1. Condition recognition

New extraction logic (new object, e.g. `ExtractResidualRangePredicate` in
`patterns.scala`, next to `ExtractRangeJoinKeys`): given a
`ShuffledHashJoinExec`'s `condition`, split it into conjuncts, find a pair
(or single, for `PartialRangeJoin`) of range predicates that classify as one
`RangeJoinKind` the same way `ExtractRangeJoinKeys.pairJoin`/`partialRange`
already do, where:

- the range predicate's expressions are evaluable one-side-each (same
  `place`/`supportedKeys` check already in `patterns.scala`), and
- the matched conjuncts are distinct from `leftKeys`/`rightKeys` (the real
  equi-join keys already fixed by the join).

Returns the matched `RangeJoinKind` plus its bound expressions, and the
remaining conjuncts as the still-residual condition to evaluate per
candidate. This reuses `RangeJoinKind`, `RangePredicate`, and the
shape-matching helpers; it does not reuse `ExtractRangeJoinKeys.unapply`
itself, since that treats the range predicate as *the* join key and ignores
true equi-keys entirely - the opposite of what's needed here.

### 2. AQE rule

New rule, e.g. `OptimizeShuffledHashRangeJoin`, registered alongside (not
inside) `ConvertSortMergeJoinToShuffledHashJoin` in the AQE physical-plan
rule list. It transforms the plan looking for any `ShuffledHashJoinExec`
(however it was planned - static `JoinSelection` or the AQE SMJ-to-SHJ
conversion - matching is unconditional on origin):

1. Must be `Inner` join type.
2. Its `condition` must contain a range shape per `ExtractResidualRangePredicate`.
3. Its build side's input shuffle stage must be materialized with stats, and
   those stats must pass the existing "small enough for a local hash map"
   check (`findShuffleStage` / `preferShuffledHashJoin`, factored out of
   `ConvertSortMergeJoinToShuffledHashJoin` into a shared location both
   rules call - this is the one shared-code change to that file).

When all hold, replace the node with `ShuffledRangeJoinExec` carrying the
same `leftKeys`/`rightKeys`/`buildSide`/children, the matched `RangeJoinKind`
and its bound range expressions, and the reduced residual `condition`.
Otherwise leave the node as `ShuffledHashJoinExec`. Gated by a new conf,
`spark.sql.adaptive.shuffledRangeJoin.enabled` (default true, following the
existing `broadcastRangeJoin.enabled` precedent of defaulting new
range-index paths on since they only replace a strictly more expensive
per-row scan).

### 3. `ShuffledRangeJoinExec`

New physical operator, parallel to `ShuffledHashJoinExec` and
`BroadcastRangeJoinExec`, in `joins/ShuffledRangeJoinExec.scala`:

- `requiredChildDistribution` / `outputPartitioning`: identical to
  `ShuffledHashJoinExec` (`HashPartitioning` on the equi-join keys on both
  sides) - this is a drop-in replacement for a `ShuffledHashJoinExec` node,
  not a new distribution requirement.
- Build-side index: group the build side's local partition rows by the
  equi-join key (same key extraction `HashedRelation` uses), and for each
  group call `RangeRelationBroadcastMode.transform(groupIter, sizeHint)` -
  the same entry point `BroadcastRangeJoinExec` uses, reused verbatim - to
  get a `RangeRelation` for that group. The result is a
  `Map`-like structure from equi-key to `RangeRelation` (new small class,
  e.g. `RangeHashedRelation`, living next to `RangeRelation.scala`).
- Probe: for each stream row, look up its equi-key in `RangeHashedRelation`;
  on a hit, probe the returned `RangeRelation` with the row's range
  expression(s) exactly as `BroadcastRangeJoinExec` does; evaluate the
  reduced residual condition (if any) on each candidate.
- Codegen: supports both the interpreted (`doExecute`) and whole-stage
  codegen (`doProduce`/`doConsume`) paths, following
  `BroadcastRangeJoinExec`'s existing split for the two.
- Code sharing: factor the "probe a `RangeRelation` and apply a residual
  condition" logic that's currently inline in `BroadcastRangeJoinExec` into
  a shared trait (parallel to how `HashJoin` is shared between
  `BroadcastHashJoinExec` and `ShuffledHashJoinExec`), so
  `BroadcastRangeJoinExec` and `ShuffledRangeJoinExec` differ only in how
  they obtain their `RangeRelation`(s) - one `RangeRelation` from a
  broadcast, versus one `RangeRelation` per equi-key group from a local
  partition.

### 4. Metrics and EXPLAIN

Same metric set as `ShuffledHashJoinExec` (`numOutputRows`, `buildDataSize`,
`buildTime`), plus the index-specific breakdown `BroadcastRangeJoinExec`
already tracks, so the two range-join operators stay consistent in EXPLAIN
output and SQL metrics tooling.

## Testing

- Unit tests for `ExtractResidualRangePredicate`, mirroring the existing
  `ExtractRangeJoinKeysSuite` structure, covering all three `RangeJoinKind`
  shapes plus negative cases (range predicate reuses an equi-join column,
  no equi-join present, unsupported type).
- `ShuffledRangeJoinExec` correctness suite mirroring
  `RangeJoinCorrectnessSuite`/`RangeJoinSuite`, run with AQE on, asserting
  output matches plain `ShuffledHashJoinExec` with the full condition.
- AQE plan-shape test (like existing `AdaptiveQueryExecSuite` conversion
  tests) asserting the rule fires only when the build side is small and
  backs off correctly when it is not, and that it composes correctly when
  the node arrived via `ConvertSortMergeJoinToShuffledHashJoin`.
- Benchmark extending `RangeJoinBenchmark`'s approach for the shuffled case:
  a large equi-key cardinality with many rows per key, range condition
  narrowing the match set, comparing against plain `ShuffledHashJoinExec`.

## JIRA

This is a different approach from PR #21109 (SPARK-24020): no
`SortMergeJoinExec` change, no sliding window, and it specifically requires
AQE with a small build side rather than handling the "both sides huge"
case. Per repo convention, file a new JIRA rather than reusing SPARK-24020.

## Follow-ups (explicitly out of scope here)

- Outer/semi/anti join support for `ShuffledRangeJoinExec`.
- The "both sides too large for any hash map" case PR #21109 targeted -
  still unaddressed by this design.
