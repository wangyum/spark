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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class TemporaryTableSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.REWRITE_TEMP_VIEW_TO_TEMP_TABLE, true)

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  test("create temporary table across different sessions") {
    val sparkSession1 = spark.newSession()
    val sparkSession2 = spark.newSession()

    val t1 = TableIdentifier("t1")

    sparkSession1.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(2)")
    sparkSession2.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(2)")

    assert(sparkSession1.sessionState.catalog.getTableMetadata(t1).isTemporary)
    assert(sparkSession1.sessionState.catalog.getTableMetadata(t1).tableType ===
      CatalogTableType.TEMPORARY)
    assert(sparkSession2.sessionState.catalog.getTableMetadata(t1).isTemporary)
    assert(sparkSession2.sessionState.catalog.getTableMetadata(t1).tableType ===
      CatalogTableType.TEMPORARY)

    checkAnswer(sparkSession1.table("t1"), Seq(Row(0), Row(1)))
    checkAnswer(sparkSession2.table("t1"), Seq(Row(0), Row(1)))
  }

  test("drop temporary table") {
    val sparkSession = spark.newSession()
    sparkSession.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(2)")

    val t1 = TableIdentifier("t1")

    assert(sparkSession.sessionState.catalog.tempTableExists(t1))
    assert(sparkSession.sessionState.catalog.tableExists(t1))

    sparkSession.sql("DROP TABLE t1")

    assert(!sparkSession.sessionState.catalog.tempTableExists(t1))
    assert(!sparkSession.sessionState.catalog.tableExists(t1))
  }

  test("analyze temporary table") {
    val sparkSession = spark.newSession()
    sparkSession.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(100)")
    sparkSession.sql("ANALYZE TABLE t1 COMPUTE STATISTICS FOR ALL COLUMNS")

    val t1 = TableIdentifier("t1")

    val stats = sparkSession.sessionState.catalog.getTableMetadata(t1).stats
    assert(stats.nonEmpty)
    assert(stats.get.rowCount.contains(100))
    assert(stats.get.colStats.contains("id"))
  }

  test("create temporary table and insert values") {
    val sparkSession = spark.newSession()
    sparkSession.sql("CREATE TEMPORARY TABLE t1(id bigint)")
    sparkSession.sql("INSERT INTO t1 SELECT * FROM range(2)")
    sparkSession.sql("INSERT INTO t1 VALUES (2), (3)")
    sparkSession.sql("INSERT INTO t1(id) VALUES (4), (5)")

    checkAnswer(sparkSession.table("t1"), Seq(Row(0), Row(1), Row(2), Row(3), Row(4), Row(5)))
  }

  // Do not support
  test("alter temporary table") {
    val sparkSession = spark.newSession()
    sparkSession.sql("CREATE TEMPORARY TABLE t1 AS SELECT * FROM range(100)")
    sparkSession.sql("ANALYZE TABLE t1 COMPUTE STATISTICS FOR ALL COLUMNS")

    val t1 = TableIdentifier("t1")

    val stats = sparkSession.sessionState.catalog.getTableMetadata(t1).stats
    assert(stats.nonEmpty)
    assert(stats.get.rowCount.contains(100))
    assert(stats.get.colStats.contains("id"))
  }

  test("temporary table and non-temporary table") {
    val sparkSession = spark.newSession()
    sparkSession.sql("CREATE TABLE t1 using parquet AS SELECT * FROM range(2)")
    sparkSession.sql("CREATE TEMPORARY TABLE t1 using parquet AS SELECT * FROM range(3)")
    checkAnswer(sparkSession.table("t1"), Seq(Row(0), Row(1), Row(2)))
    sparkSession.sql("DROP TABLE t1") // drop temporary table
    checkAnswer(sparkSession.table("t1"), Seq(Row(0), Row(1)))
    sparkSession.sql("DROP TABLE t1") // drop non-temporary table
    assert(!sparkSession.sessionState.catalog.tableExists(TableIdentifier("t1")))
  }

  test("show create temporary table2") {
    val sparkSession = spark.newSession()
    sparkSession.sql(
      "CREATE TEMPORARY TABLE t1 using parquet AS SELECT id, id % 4 AS part FROM range(20)")
    val createTblStr = sparkSession.sql("SHOW CREATE TABLE t1").collect().head.getString(0)
    assert(createTblStr.startsWith("CREATE TEMPORARY TABLE"))
  }

  test("create temporary table like") {
    val sparkSession = spark.newSession()
    sparkSession.sql("CREATE TEMPORARY TABLE t1(id int)")
    sparkSession.sql("CREATE TEMPORARY TABLE t2 LIKE t1")
    sparkSession.sql("CREATE TABLE t3 LIKE t2 USING parquet")
  }

  // test unsupported command
  test("create partition temporary table") {
    val e1 = intercept[ParseException] {
      spark.newSession().sql(
        """
          |CREATE temporary TABLE t1 using parquet partitioned BY (part) AS
          |SELECT id,
          |       id % 2 AS part
          |FROM   range(10)
          |""".stripMargin)
    }
    assert(e1.getMessage.contains("Operation not allowed"))

    withTable("t1") {
      withTempTable("tt1") {
        sql("CREATE TABLE t1 (id int, dt int) USING parquet PARTITIONED BY (dt)")
        val e2 = intercept[AnalysisException] {
          sql("CREATE TEMPORARY TABLE tt1 LIKE t1 USING PARQUET")
        }
        assert(e2.getMessage.contains("The feature is not supported"))
      }
    }
  }

  private def testUnsupportedCommand(sparkSession: SparkSession, sqlStr: String) = {
    val e = intercept[AnalysisException] {
      sparkSession.sql(sqlStr)
    }
    assert(e.getMessage.contains("The feature is not supported"))
  }

  test("unsupported commands") {
    val sparkSession = spark.newSession()
    sparkSession.sql("CREATE TEMPORARY TABLE t1 using parquet AS SELECT * FROM range(3)")

    testUnsupportedCommand(
      sparkSession,
      "ALTER TABLE t1 SET TBLPROPERTIES ('key1' = 'val1')")
    testUnsupportedCommand(
      sparkSession,
      "ALTER TABLE t1 UNSET TBLPROPERTIES IF EXISTS ('key1')")

    testUnsupportedCommand(
      sparkSession,
      "ALTER TABLE t1 RENAME COLUMN id TO user_id")

    testUnsupportedCommand(
      sparkSession,
      "ALTER TABLE t1 ADD COLUMNS (name string)")

    testUnsupportedCommand(
      sparkSession,
      "ALTER TABLE t1 SET serdeproperties ('test'='test')")

    testUnsupportedCommand(
      sparkSession,
      "ALTER TABLE t1 SET LOCATION '/tmp/spark'")
    testUnsupportedCommand(
      sparkSession,
      "ALTER TABLE t1 RENAME TO t2")
  }

  test("create temp table in database") {
    withDatabase("db1") {
      spark.sql("create database db1")
      val sparkSession = spark.newSession()
      sparkSession.sql("create temporary table db1.t1 as select 1 as id")
      checkAnswer(sparkSession.table("db1.t1"), Seq(Row(1)))
      sparkSession.sql("use db1")
      checkAnswer(sparkSession.table("t1"), Seq(Row(1)))
    }
  }

  test("create temporary table using data source") {
    withTempTable("tt1", "sameName", "`bad.name`") {
      sql("create temporary table tt1 (id int) using parquet")
      sql("insert into table tt1 values (1)")
      assert(spark.table("tt1").collect === Array(Row(1)))

      withTempView("same") {
        sql("create temporary view same as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary table same (id int) using parquet"))
        assert(e.message.contains("`spark_catalog`.`default`.`same` because it already exists"))
      }

      withTempView("same") {
        sql("create temporary table same using parquet as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary view same (id int) using parquet"))
        assert(e.message.contains("the temporary view `same` because it already exists"))
      }

      val e = intercept[AnalysisException](
        sql("create temporary table `bad.name` (id int) using parquet"))
      assert(e.message.contains("`bad.name` is not a valid name for tables/databases."))
    }
  }

  test("create temporary table using data source as select") {
    withTempTable("tt1", "sameName", "`bad.name`") {
      sql("create temporary table tt1 using parquet as select 1")
      assert(spark.table("tt1").collect === Array(Row(1)))

      withTempView("same") {
        sql("create temporary view same as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary table same using parquet as select 1"))
        assert(e.message.contains("`spark_catalog`.`default`.`same` because it already exists"))
      }

      withTempView("same") {
        sql("create temporary table same using parquet as select 1")
        val e = intercept[AnalysisException](
          sql("create temporary view same as select 1"))
        assert(e.message.contains("temporary view `same` because it already exists"))
      }

      val e = intercept[AnalysisException](
        sql("create temporary table `bad.name` using parquet as select 1"))
      assert(e.message.contains("`bad.name` is not a valid name for tables/databases."))
    }
  }

  test("create temporary table with location is not allowed") {
    withTempTable("tt1") {
      val e = intercept[AnalysisException](
        sql(
          """
            |create temporary table tt1 (id int) using parquet
            |LOCATION '/PATH'
            |""".stripMargin))
      assert(e.message.contains("specify LOCATION in temporary table"))
    }
  }

  test("create a same name temporary table and permanent table") {
    withDatabase("db1") {
      sql("create database db1")
      withTable("same1", "same2", "same3", "same4", "same5", "same6") {
        withTempTable("same1", "same2", "same3", "same4", "same5", "same6") {
          // create temporary table then create table
          sql("create temporary table same using parquet as select 'temp_table'")
          sql("create table same using parquet as select 'table'")

          sql("create temporary table same2 (key int) using parquet")
          sql("create table same2 (key int) using parquet")

          sql("create temporary table db1.same3 (key int) using parquet")
          sql("create table same3 (key int) using parquet")
          sql("create table db1.same3 (key int) using parquet")

          // create table then create temporary table
          sql("create table same4 using parquet as select 'table'")
          sql("create temporary table same4 using parquet as select 'temp_table'")

          sql("create table same5 (key int) using parquet")
          sql("create temporary table same5 (key int) using parquet")

          sql("create table db1.same6 (key int) using parquet")
          sql("create temporary table same6 (key int) using parquet")
          sql("create temporary table db1.same6 (key int) using parquet")
        }
      }
    }
  }

  test("drop temporary table2") {
    withDatabase("db1") {
      withTempTable("t1", "db1.t1") {
        sql("create temporary table t1 using parquet as select 1")
        assert(spark.table("t1").collect === Array(Row(1)))
        sql("drop table t1")
        assert(!spark.sessionState.catalog.tempTableExists(TableIdentifier("t1")))

        sql("CREATE DATABASE db1")
        sql("create temporary table db1.t1 using parquet as select 1")
        assert(spark.table("db1.t1").collect === Array(Row(1)))
        sql("drop table db1.t1")
        assert(!spark.sessionState.catalog.tempTableExists(TableIdentifier("db1.t1")))
      }
    }
  }

  test("create temporary partition table is not allowed") {
    withTempTable("t1", "t2") {
      val e = intercept[ParseException] {
        sql("create temporary table t1 (a int, b int) " +
          "using parquet partitioned by (b)")
      }.getMessage
      assert(e.contains("Operation not allowed"))
      val e1 = intercept[ParseException] {
        sql("CREATE TEMPORARY TABLE t2 using parquet PARTITIONED BY (key)" +
          " AS SELECT key, value FROM (SELECT 1 as key, 2 as value) tmp")
      }.getMessage
      assert(e1.contains("Operation not allowed"))
    }
  }

  test("insert into temporary table") {
    withDatabase("dba") {
      sql("CREATE DATABASE dba")
      withTempTable("dba.tt1", "tt1", "tt2") {
        sql("CREATE TEMPORARY TABLE dba.tt1 (key int) USING PARQUET")
        sql("CREATE TEMPORARY TABLE tt1 USING PARQUET AS SELECT 1")
        checkAnswer(spark.table("dba.tt1"), Seq())
        checkAnswer(spark.table("tt1"), Seq(Row(1)))

        sql("INSERT INTO TABLE dba.tt1 SELECT * FROM tt1")
        sql("INSERT OVERWRITE TABLE tt1 VALUES (2)")
        checkAnswer(spark.table("dba.tt1"), Seq(Row(1)))
        checkAnswer(spark.table("tt1"), Seq(Row(2)))
      }
    }
  }

  test("show tables with temporary tables") {
    withDatabase("db1") {
      withTempTable("tt1", "tt2", "db1.tt1", "db1.tt2") {
        sql("create database db1")
        sql("create temporary table tt1 using parquet as select 1")
        sql("create table tt2 using parquet as select 1")
        sql("create temporary table db1.tt1 using parquet as select 1")
        sql("create table db1.tt2 using parquet as select 1")

        QueryTest.checkAnswer(sql("show tables in db1"),
          Seq(Row("db1", "tt1", true), Row("db1", "tt2", false)))
        QueryTest.checkAnswer(sql("show tables"),
          Seq(Row("default", "tt1", true), Row("default", "tt2", false)))
      }
    }
  }

  test("Using VOLATILE table as an alias of TEMPORARY table") {
    Seq("TEMPORARY", "TEMP", "VOLATILE").foreach { tableType =>
      withTempTable("t1") {
        val s1 =
          s"""
             |CREATE $tableType TABLE t1 USING PARQUET AS SELECT 1
             |""".stripMargin
        sql(s1)
        assert(spark.sessionState.catalog.tempTableExists(TableIdentifier("t1")))
      }
    }
  }

  test("show create table") {
    withTable("t1") {
      sql("CREATE TABLE t1 USING PARQUET AS SELECT 1")
      checkKeywordsExist(sql("SHOW CREATE TABLE t1"),
        "CREATE TABLE")
    }
  }

  test("show create temporary table") {
    withTempTable("t1") {
      sql("CREATE TEMPORARY TABLE t1 USING PARQUET AS SELECT 1")
      checkKeywordsExist(sql("SHOW CREATE TABLE t1"),
        "CREATE TEMPORARY TABLE")
    }
  }

  test("alter temporary table The feature is not supported") {
    withTempTable("table1") {
      sql("create temporary table table1 using parquet as select 1 AS id")
      var e = intercept[AnalysisException](
        sql("ALTER TABLE table1 ADD PARTITION (part = '1')"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 ADD COLUMNS (c int)"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1')"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 UNSET TBLPROPERTIES IF EXISTS ('key1')"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 CHANGE COLUMN id TYPE bigint"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 SET SERDE 'whatever'"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 PARTITION (a='1', b='2') RENAME TO PARTITION (a='100', b='200')"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 DROP PARTITION (a='1', b='2')"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 RECOVER PARTITIONS"))
      assert(e.message.contains("The feature is not supported"))

      e = intercept[AnalysisException](
        sql("ALTER TABLE table1 SET LOCATION \"loc\""))
      assert(e.message.contains("The feature is not supported"))
    }
  }

  test("analyzes temporary table") {
    withTempTable("table1") {
      sql("create temporary table table1 (id int, col string) using parquet")
      var sizeInBytes = spark.table("table1").queryExecution.optimizedPlan.stats.sizeInBytes
      assert(sizeInBytes == 0)
      sql("insert into table1 values (1, null), (2, null), (3, null)")
      sizeInBytes = spark.table("table1").queryExecution.optimizedPlan.stats.sizeInBytes
      assert(sizeInBytes > 1000)
      sql("ANALYZE TABLE table1 COMPUTE STATISTICS")
      val stats1 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("table1")).stats
      assert(stats1.nonEmpty)
      assert(stats1.get.sizeInBytes > 1000)

      sql("ANALYZE TABLE table1 COMPUTE STATISTICS FOR COLUMNS id")
      val stats2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("table1")).stats
      assert(stats2.nonEmpty)
      assert(stats2.get.sizeInBytes > 1000)
      assert(stats2.get.colStats.contains("id"))
    }
  }

  test("show tables display temporary table") {
    val sparkSession = spark.newSession()
    withTempTable("t1") {
      sparkSession.sql("CREATE TEMPORARY TABLE t1 USING parquet AS SELECT 1")
      QueryTest.checkAnswer(
        sparkSession.sql("SHOW TABLES"),
        Row("default", "t1", true) :: Nil)
    }
  }

  test("create temporary table without using should use parquet by default") {
    val sparkSession = spark.newSession()
    def checkTemporaryTableBase(name: String): Unit = {
      assert(sparkSession.sessionState.catalog.getTableMetadata(TableIdentifier(name)).provider ===
        Some("parquet"))
      assert(sparkSession.sessionState.catalog.getTableMetadata(TableIdentifier(name)).tableType
        === CatalogTableType.TEMPORARY)
    }

    withTempTable("t1", "t2", "t3") {
      sparkSession.sql("CREATE TEMPORARY TABLE t1 AS SELECT 1")
      checkTemporaryTableBase("t1")
      sparkSession.sql("CREATE TEMPORARY TABLE t2 (id int)")
      checkTemporaryTableBase("t2")
      sparkSession.sql("CREATE TEMPORARY TABLE t3 LIKE t2")
      checkTemporaryTableBase("t3")
    }
  }
}
