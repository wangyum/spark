CREATE table  explain_temp1 USING PARQUET AS SELECT id % 5 AS key, id % 3 AS val FROM range(100);
CREATE table  explain_temp2 USING PARQUET AS SELECT id % 6 AS key, id % 4 AS val FROM range(100);
CREATE table  explain_temp3 USING PARQUET clustered by (key) sorted by(val) into 10 buckets AS (SELECT CAST(id % 6 AS string) AS key, id % 4 AS val FROM range(100));
CREATE table  explain_temp4 USING PARQUET clustered by (key) sorted by(val) into 10 buckets AS (SELECT CAST(id % 6 AS decimal) AS key, id % 4 AS val FROM range(100));

EXPLAIN OPTIMIZE SELECT * FROM explain_temp1 t1 JOIN explain_temp2 t2 ON t1.key = t2.key;
EXPLAIN OPTIMIZE SELECT SUM(val) OVER (PARTITION BY key) FROM explain_temp1;
EXPLAIN OPTIMIZE SELECT * FROM explain_temp1 t1 WHERE t1.key NOT IN (SELECT t2.key FROM explain_temp2 t2);
SET spark.sql.autoBroadcastJoinThreshold=1;
EXPLAIN OPTIMIZE SELECT * FROM explain_temp3 t1 JOIN explain_temp4 t2 ON t1.key = t2.key;
EXPLAIN OPTIMIZE SELECT * FROM explain_temp1 t1 JOIN explain_temp2 t2 ON t1.key = t2.key and t1.val = t2.val;

DROP TABLE explain_temp1;
DROP TABLE explain_temp2;
DROP TABLE explain_temp3;
DROP TABLE explain_temp4;