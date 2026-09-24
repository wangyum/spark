-- Range-predicate join with the feature flag off: planner falls back to
-- BroadcastNestedLoopJoin. EXPLAIN FORMATTED is the plan-shape gate.

--SET spark.sql.join.broadcastRangeJoin.enabled=false
--SET spark.sql.adaptive.enabled=false

CREATE OR REPLACE VIEW range_points(p) AS VALUES (1), (2), (5);
CREATE OR REPLACE VIEW range_ranges(lo, hi) AS VALUES (0, 2), (3, 4);

EXPLAIN FORMATTED
SELECT p.p, r.lo, r.hi FROM range_points p JOIN range_ranges r
  ON p.p >= r.lo AND p.p <= r.hi;

SELECT p.p, r.lo, r.hi FROM range_points p JOIN range_ranges r
  ON p.p >= r.lo AND p.p <= r.hi;
