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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
import org.apache.hadoop.mapred.TextInputFormat

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.StructType

class Hive40PartitionProjectionSuite extends HiveVersionSuite("4.0") {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val hadoopConf = new Configuration()
    client = buildClient(hadoopConf)
  }

  override def afterAll(): Unit = {
    client = null
    super.afterAll()
  }

  test("Hive 4.0 partition projection under real IsolatedClientLoader") {
    val tableName = "proj_test"
    val table = CatalogTable(
      identifier = TableIdentifier(tableName, Some("default")),
      tableType = CatalogTableType.MANAGED,
      schema = new StructType()
        .add("id", "int")
        .add("p1", "string")
        .add("p2", "string"),
      partitionColumnNames = Seq("p1", "p2"),
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = Some(classOf[TextInputFormat].getName),
        outputFormat = Some(classOf[HiveIgnoreKeyTextOutputFormat[_, _]].getName),
        serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
        compressed = false,
        properties = Map.empty
      )
    )

    client.createTable(table, ignoreIfExists = false)
    try {
      val partitions = Seq(
        CatalogTablePartition(Map("p1" -> "1", "p2" -> "a"), CatalogStorageFormat.empty),
        CatalogTablePartition(Map("p1" -> "1", "p2" -> "b"), CatalogStorageFormat.empty),
        CatalogTablePartition(Map("p1" -> "2", "p2" -> "c"), CatalogStorageFormat.empty)
      )
      client.createPartitions(table, partitions, ignoreIfExists = false)

      // 1. Full projection (all partitions) with LocationOnly mask
      val summaries = client.getPartitionSummaries(
        "default", tableName, None, PartitionFieldMask.LocationOnly)
      assert(summaries.length == 3)
      assert(summaries.map(_.spec).toSet == Set(
        Map("p1" -> "1", "p2" -> "a"),
        Map("p1" -> "1", "p2" -> "b"),
        Map("p1" -> "2", "p2" -> "c")
      ))
      assert(summaries.forall(_.locationUri.isDefined))

      // 2. Prefix partition pushdown with BY_VALUES filter
      val prefixFiltered = client.getPartitionSummaries(
        "default", tableName, Some(Map("p1" -> "1")), PartitionFieldMask.LocationOnly)
      assert(prefixFiltered.length == 2)
      assert(prefixFiltered.map(_.spec).toSet == Set(
        Map("p1" -> "1", "p2" -> "a"),
        Map("p1" -> "1", "p2" -> "b")
      ))
      assert(prefixFiltered.forall(_.locationUri.isDefined))

      // 3. SpecOnly projection (no location)
      val specOnly = client.getPartitionSummaries(
        "default", tableName, None, PartitionFieldMask.SpecOnly)
      assert(specOnly.length == 3)
      assert(specOnly.forall(_.locationUri.isEmpty))
    } finally {
      client.dropTable("default", tableName, ignoreIfNotExists = true, purge = true)
    }
  }
}
