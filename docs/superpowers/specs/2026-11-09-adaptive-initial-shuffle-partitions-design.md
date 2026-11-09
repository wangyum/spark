# Adaptive Initial Shuffle Partition Count (Planner-Side)

**Date:** 2026-11-09  
**Status:** Draft for review  
**Primary use case:** Multi-tenant clusters where small SQL jobs are hurt by an oversized fixed `initialPartitionNum`  
**Scope:** Phase 1 — planner estimate-based sizing (Trino / Kyuubi style). Stage-runtime sizing is out of scope for Phase 1.

## 1. Problem

With AQE and shuffle partition coalescing enabled, Spark sets the number of shuffle partitions for `ShuffleExchangeExec` from:

- `spark.sql.adaptive.coalescePartitions.initialPartitionNum`, or else
- `spark.sql.shuffle.partitions`

via `SQLConf.numShufflePartitions`, used by `EnsureRequirements` when it must invent a partition count.

AQE `CoalesceShufflePartitions` then merges contiguous **reduce** partitions after map output statistics are available. That helps reduce-side task count and small output files, but it does **not** reduce map-side work:

- Each mapper still writes `N` partitions (files / buffers / metadata ∝ mappers × N).
- Remote shuffle services (e.g. Celeborn / RSS) still see high partition fan-out.
- The driver and scheduler still deal with large shuffle dependency metadata.

So a cluster-wide large `initialPartitionNum` (chosen so big ETL can coalesce down) **taxes every small query’s map writes**. Small SQL cannot “afford” that N even if reduce tasks are later coalesced away.

**Issue in one sentence:** fixed oversized initial shuffle partition count makes small multi-tenant queries pay large map-side / RSS / scheduling cost that post-shuffle coalesce cannot recover.

## 2. Goals and Non-Goals

### Goals (Phase 1)

1. Size **default** shuffle partition counts from estimated input bytes and `advisoryPartitionSizeInBytes`, clamped to a min/max.
2. Reinterpret `initialPartitionNum` as an **upper bound** (max), not a fixed always-on value, when the feature is enabled.
3. Decide N **per co-partitioning group**, not once for the whole query.
4. Preserve bucket joins and any path where partition count is dictated by an existing layout (`picked` partitioning / `numBuckets`).
5. Keep AQE coalesce and skew join as safety nets for skew and estimate error.

### Non-Goals (Phase 1)

1. Stage-runtime re-sizing of not-yet-materialized shuffles from `MapOutputStatistics` (Flink-style follow-up).
2. Changing how coalesce packs reduce partitions, or replacing coalesce.
3. Inferring join broadcast vs shuffle (existing AQE / CBO handles that).
4. Guaranteeing perfect N when catalyst stats are missing or wrong (fallback must be safe).

## 3. Related Systems

| System | Approach | Takeaway |
|--------|----------|----------|
| **Trino** | Plan-time hash partition count from input size/rows; clamp to `min_hash_partition_count` … `max_hash_partition_count`; `min_input_size_per_task` drives small-query downsizing | Max is a ceiling; small queries get fewer partitions for cluster concurrency |
| **Kyuubi** | Extension rule: `sum(scan\|shuffleRead) / advisory`, rewrite `ShuffleExchangeExec`, cap with max | Validates estimate-based rewrite on Spark; global sum → one N is too coarse for multi-input queries |
| **Flink Adaptive Batch** | After upstream finishes, set downstream parallelism from real bytes / avg volume | Best for multi-stage accuracy; weak for Spark’s first shuffle when scan and shuffle write share a stage |

Phase 1 follows Trino/Kyuubi (plan-time estimate). Phase 2 may add Flink-like stage awareness inside AQE re-optimize.

## 4. Design Overview

When `EnsureRequirements` would use `conf.numShufflePartitions` as the default partition count, instead compute:

```text
N = clamp(
  ceil(bytes(G) / advisoryPartitionSizeInBytes),
  minN,
  maxN
)
```

Where:

- `G` is the **co-partitioning group** being planned (see §5).
- `advisoryPartitionSizeInBytes` is the existing AQE advisory size (target bytes per partition).
- `maxN` is today’s `numShufflePartitions` (`initialPartitionNum` if set, else `spark.sql.shuffle.partitions`).
- `minN` is a new conf (default **1** for multi-tenant small SQL).

**Do not** apply this formula when partition count comes from:

- `distribution.requiredNumPartitions` (explicit),
- a **picked** child layout (`picked.member.createPartitioning(...)`) — including bucket / SPJ alignment,
- user-specified repartition counts handled outside this default path.

Semantic change when the feature is on: `spark.sql.adaptive.coalescePartitions.initialPartitionNum` means **maximum** initial partitions, not the exact count.

## 5. Multi-Input and Co-Partition Groups

### Decision unit

**Wrong:** one N for the whole query from `sum(all scans) / advisory`, applied to every exchange (over-partitions intermediate shuffles when many tables are involved).

**Right:** one N per **co-partitioning group** — the set of shuffles that must share the same `numPartitions`:

- Both sides of a shuffle join that `EnsureRequirements` co-partitions together.
- A single exchange feeding an aggregate / window / etc.
- Independent children under `Union` (when they need not be compatible) get independent groups.

### Bytes for a group

For multi-tenant small-SQL (map-write focused):

```text
bytes(G) = max(estimatedBytes(child) for each shuffle input child in G)
```

Rationale:

- Join sides must share N; the large side dominates map cost and needed parallelism.
- Using `sum(left, right)` is at most ~2× more partitions (more conservative for reduce task size). Phase 1 defaults to **max**; coalesce still merges small reduce partitions.
- Star-schema dims that should not shuffle should be broadcast; if they shuffle-join with a large fact, N correctly follows the fact via `max`.

### Worked examples

| Query shape | Group bytes | Effect |
|-------------|-------------|--------|
| 8 tables × 100MB, bushy shuffle joins | per join `max≈100MB` | Small N per hop, not `800MB/advisory` on every hop |
| Fact 1GB + dim 10MB shuffle join | `max=1GB` | N follows fact; dim writes many sparse partitions (inherent to co-partitioning; prefer broadcast) |
| Single agg over 20MB scan | `20MB` | N near `minN` or `ceil(20MB/advisory)` |

## 6. Size Estimation

Reuse the pattern already used inside `EnsureRequirements` (e.g. single-partition preference, partially clustered join side selection):

```text
estimatedBytes(plan) =
  plan.logicalLink.map(_.stats.sizeInBytes).filter(_ > 0)
```

| Situation | Behavior |
|-----------|----------|
| Logical stats available (including filter selectivity when CBO/stats allow) | Use `sizeInBytes` |
| Missing / unusable stats | **Fallback to `maxN`** (safe / conservative; do not invent a tiny N) |
| Operator subtree | Estimate the **input to the shuffle** (child size), not join output cardinality |

Phase 1 does not require splitting scan and shuffle write into separate stages to observe runtime scan size.

## 7. Hook Point and Bucket Join Safety

### Why bucket joins break if done wrong

`EnsureRequirements` has two ways to choose partitioning for a child that does not yet satisfy a clustered distribution:

1. **Picked layout** — copy partitioning from a sibling that already has a usable layout (typical: bucketed scan with `numBuckets`). The shuffle on the other side **must** use that partition count.
2. **Default** — `requiredNumPartitions.getOrElse(conf.numShufflePartitions)` when no layout can be picked.

A post-hoc rule that rewrites **all** `ShuffleExchangeExec` nodes to `size/advisory` can change (1) and break co-partitioning with bucketed data, or encourage plans that disable useful bucketed scans (`DisableUnnecessaryBucketedScan`).

`CoalesceBucketsInJoin` adjusts `FileSourceScanExec.optionalNumCoalescedBuckets` and must remain untouched by this feature.

### Required rule

**Only replace the default branch** (`getOrElse(conf.numShufflePartitions)`). Never override `picked.member.createPartitioning(...)`.

Conceptual change (two call sites today: single-child resolve and co-partition resolve):

```scala
val numPartitions = distribution.requiredNumPartitions
  .getOrElse(adaptiveNumPartitions(groupInputs))
```

```text
adaptiveNumPartitions(inputs):
  if !enabled: return conf.numShufflePartitions
  bytes = max(estimatedBytes(i) for i in inputs)   // or FALLBACK
  if FALLBACK: return maxN
  return clamp(ceil(bytes / advisory), minN, maxN)
```

### Bucket scenario matrix

| Scenario | Path | Adaptive N? | Bucket read |
|----------|------|-------------|-------------|
| Both sides bucket-satisfied | No shuffle inserted | No | Preserved |
| One side bucket; other shuffled to match | `picked` → N = bucket count | **No** | Preserved |
| Neither side has usable layout | Default → adaptive | **Yes** | N/A |
| `CoalesceBucketsInJoin` | Scan coalesced bucket count | No interaction | Preserved |

Also leave alone: stateful operators with explicit `requiredNumPartitions`, user `repartition(n)`, broadcast exchanges.

## 8. Interaction with AQE Coalesce and Skew Join

| Mechanism | Role after this change |
|-----------|----------------------|
| **Adaptive initial N** | Chooses map-side partition count before shuffle write |
| **CoalesceShufflePartitions** | Still merges small contiguous reduce partitions; safety net for skew-free unevenness and mild overestimate of N |
| **OptimizeSkewedJoin** | Still splits skewed partitions; less often starved by a tiny global N on large data (because large estimated bytes raise N up to `maxN`) |
| **parallelismFirst / minPartitionSize** | Continue to govern **post-coalesce** parallelism; do not conflate with `minN` for initial map partitions |

For small queries, expected pattern: adaptive N already small → coalesce often no-ops or merges little → fewer empty/near-empty map outputs.

## 9. Configuration

| Key | Role | Proposed default |
|-----|------|------------------|
| `spark.sql.adaptive.optimizer.initialPartitionNumBySize.enabled` | Feature flag | `false` initially; enable after soak |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | Bytes per partition target (existing) | unchanged (64MB) |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | **maxN** when feature on (document semantic change) | unchanged |
| `spark.sql.adaptive.optimizer.initialPartitionNumBySize.minPartitionNum` | minN | `1` |

When the feature is **disabled**, behavior remains exactly as today (`numShufflePartitions` is the exact initial count).

When **enabled** and `initialPartitionNum` is unset, `maxN` is `spark.sql.shuffle.partitions`.

Doc updates must state clearly: with the feature on, set `initialPartitionNum` large enough as a **ceiling** for the biggest jobs; small jobs no longer always hit that ceiling.

## 10. Implementation Sketch

Primary touch points (Phase 1):

1. `SQLConf` — new flag + `minPartitionNum`; doc string updates for `COALESCE_PARTITIONS_INITIAL_PARTITION_NUM`.
2. `EnsureRequirements` — helper `adaptiveNumPartitions`; replace both `getOrElse(conf.numShufflePartitions)` default sites used for shuffle partitioning; pass the relevant child plan(s) for stats.
3. Tests — unit / planner tests (see §11).
4. `docs/sql-performance-tuning.md` — describe feature and semantic change.

Prefer **in-place EnsureRequirements** over a Kyuubi-style “rewrite every ShuffleExchange” rule so bucket/`picked` paths stay correct by construction.

## 11. Testing Matrix

| Case | Expectation |
|------|-------------|
| Small scan → agg, feature on, good stats | `ShuffleExchange` N ≪ maxN, ≈ `ceil(size/advisory)` clamped |
| Large scan, size ≫ `maxN * advisory` | N = maxN |
| Missing stats | N = maxN |
| SMJ both bucketed same keys/buckets | No shuffle; bucketed scans remain |
| One bucketed + one non-bucketed | Non-bucketed shuffle N equals bucket count, **not** size-based |
| Multi-join bushy, several small tables | Per-join N from per-group max, not sum of all scans |
| Feature off | Bit-identical to current planning for partition counts |
| AQE coalesce still runs | Map stats coalesce when adaptive N still slightly high |
| Explicit `repartition(n)` | Unchanged n |
| `requiredNumPartitions` on distribution | Unchanged |

## 12. Rollout

1. Land behind flag default `false`.
2. Enable in multi-tenant / gateway environments first (Kyuubi-like), with `minPartitionNum=1` and a high `initialPartitionNum` ceiling.
3. Watch: shuffle write bytes/files, RSS load, small-query latency, and any join correctness regressions (especially bucket / SPJ).
4. Consider default `true` only after soak; document migration (“initial = max”).

## 13. Future Work (Phase 2)

- On AQE `reOptimize`, when upstream query stages are materialized, refine N for **not-yet-materialized** default shuffles using runtime `sizeInBytes` / map stats (Flink-like), still never overriding bucket/`picked` counts.
- Optional `sum` vs `max` conf for group bytes if some workloads prefer reduce-task sizing over map-write aggressiveness.
- Optional tighter fallback than maxN when only file-size upper bounds exist (still never below a safe floor without evidence).

## 14. Decision Summary

| Topic | Decision |
|-------|----------|
| Primary pain | Multi-tenant small SQL map / RSS / scheduling cost |
| Phase 1 approach | Planner estimate-based adaptive initial N |
| Formula | `clamp(ceil(bytes/advisory), minN, maxN)` |
| Group bytes | `max` of shuffle input estimates |
| `initialPartitionNum` | Max ceiling when feature enabled |
| Hook | Only `EnsureRequirements` default-N branch |
| Bucket / picked layout | Never overridden |
| Missing stats | Fall back to maxN |
| Coalesce / skew | Remain as safety nets |

## 15. Open Points (Explicitly Closed for Phase 1)

| Question | Resolution |
|----------|------------|
| Whole-query one N vs per group? | **Per co-partition group** |
| `max` vs `sum` for join group bytes? | **`max`** (map-write / small-SQL priority) |
| Include runtime stage sizing in Phase 1? | **No** — Phase 2 |
| Post-hoc rewrite all exchanges? | **No** — hook default branch only |
