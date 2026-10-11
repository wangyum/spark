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

import org.apache.hadoop.hive.metastore.api.FieldSchema

import org.apache.spark.{SparkFunSuite, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, PartitionFieldMask}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.hive.{HiveUtils, StaticInitFlags, StaticInitInputFormat, StaticInitOutputFormat}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class HiveClientImplSuite extends SparkFunSuite with SQLHelper {

  test("SPARK-59330: toHiveTable skips the format class static initializer when " +
    "spark.sql.hive.initializeMetastoreFormatClasses is false") {
    // Both call sites are exercised: toHiveTable resolves the input format via toInputFormat and
    // the output format via toOutputFormat, so each format class has its own flag.
    val table = CatalogTable(
      identifier = TableIdentifier("t", Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        inputFormat = Some(classOf[StaticInitInputFormat].getName),
        outputFormat = Some(classOf[StaticInitOutputFormat].getName)),
      schema = new StructType().add("a", "int"))

    def toHiveTableWith(initialize: Boolean): Unit = {
      val conf = new SQLConf()
      conf.setConf(HiveUtils.INITIALIZE_METASTORE_FORMAT_CLASSES, initialize)
      SQLConf.withExistingConf(conf) {
        HiveClientImpl.toHiveTable(table)
      }
    }

    // The false half must run first: class initialization is one-way per JVM. Resolving the
    // format class names without initializing them must not run their static initializers.
    toHiveTableWith(initialize = false)
    assert(!StaticInitFlags.inputFormatInitialized)
    assert(!StaticInitFlags.outputFormatInitialized)

    // With initialization enabled (the default), resolving the class names runs the initializers.
    toHiveTableWith(initialize = true)
    assert(StaticInitFlags.inputFormatInitialized)
    assert(StaticInitFlags.outputFormatInitialized)
  }

  test("SPARK-59330: toHiveTable still resolves the format class when " +
    "spark.sql.hive.initializeMetastoreFormatClasses is false") {
    // false skips the static initializer but still loads the class, so a missing class fails here
    // rather than being silently deferred; this pins that contract (the reason for initialize =
    // false over dropping the resolution entirely).
    val table = CatalogTable(
      identifier = TableIdentifier("t", Some("default")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        inputFormat = Some("org.apache.spark.sql.hive.DoesNotExistInputFormat")),
      schema = new StructType().add("a", "int"))
    val conf = new SQLConf()
    conf.setConf(HiveUtils.INITIALIZE_METASTORE_FORMAT_CLASSES, false)
    intercept[ClassNotFoundException] {
      SQLConf.withExistingConf(conf) {
        HiveClientImpl.toHiveTable(table)
      }
    }
  }

  test("SPARK-21529: a clear error is raised for an unsupported Hive union type") {
    val column = new FieldSchema("c", "uniontype<int,string>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"UNIONTYPE<INT,STRING>\"",
        "fieldName" -> "`c`"))
  }

  test("SPARK-21529: a Hive union type nested in a struct is detected") {
    val column = new FieldSchema("c", "struct<a:uniontype<int,string>>", null)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        HiveClientImpl.fromHiveColumn(column)
      },
      condition = "UNSUPPORTED_HIVE_TYPE",
      parameters = Map(
        "fieldType" -> "\"STRUCT<A:UNIONTYPE<INT,STRING>>\"",
        "fieldName" -> "`c`"))
  }

  test("Shim_v4_0 getPartitionSummaries fallback on unpartitioned table") {
    val shim = new Shim_v4_0()
    assert(shim.isInstanceOf[Shim_v3_1])
    val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
    assert(shim.getPartitionSummaries(null, table, new java.util.HashMap(),
      PartitionFieldMask.LocationOnly).isEmpty)
  }

  test("Shim_v4_0 getPartitionSummaries fallback when projection is disabled or fails") {
    val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
    table.setPartCols(java.util.Arrays.asList(new FieldSchema("p", "int", null)))
    assert(table.isPartitioned)

    var fallbackCalled = false
    val testShim = new Shim_v4_0() {
      override def getPartitions(
          hive: org.apache.hadoop.hive.ql.metadata.Hive,
          tbl: org.apache.hadoop.hive.ql.metadata.Table,
          partSpec: java.util.Map[String, String])
        : Seq[org.apache.hadoop.hive.ql.metadata.Partition] = {
        fallbackCalled = true
        Seq.empty
      }
    }

    withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PROJECTION_ENABLED.key -> "false") {
      fallbackCalled = false
      assert(testShim.getPartitionSummaries(null, table, new java.util.HashMap(),
        PartitionFieldMask.LocationOnly).isEmpty)
      assert(fallbackCalled)
    }

    withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PROJECTION_ENABLED.key -> "true") {
      fallbackCalled = false
      assert(testShim.getPartitionSummaries(null, table, new java.util.HashMap(),
        PartitionFieldMask.LocationOnly).isEmpty)
      assert(fallbackCalled)
    }
  }

  class MockHive4ClassLoader extends ClassLoader(classOf[Shim_v4_0].getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] = {
      name match {
        case "org.apache.hadoop.hive.metastore.api.GetPartitionsRequest" =>
          classOf[MockGetPartitionsRequest]
        case "org.apache.hadoop.hive.metastore.api.GetProjectionsSpec" =>
          classOf[MockGetProjectionsSpec]
        case "org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec" =>
          classOf[MockGetPartitionsFilterSpec]
        case "org.apache.hadoop.hive.metastore.api.PartitionFilterMode" =>
          classOf[MockPartitionFilterMode]
        case "org.apache.hadoop.hive.metastore.api.GetPartitionsResponse" =>
          classOf[MockGetPartitionsResponse]
        case "org.apache.hadoop.hive.metastore.api.PartitionSpec" =>
          classOf[MockPartitionSpec]
        case "org.apache.hadoop.hive.metastore.api.PartitionSpecWithSharedSD" =>
          classOf[MockPartitionSpecWithSharedSD]
        case "org.apache.hadoop.hive.metastore.api.PartitionWithoutSD" =>
          classOf[MockPartitionWithoutSD]
        case "org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec" =>
          classOf[MockPartitionListComposingSpec]
        case "org.apache.hadoop.hive.metastore.IMetaStoreClient" =>
          classOf[MockMetaStoreClient]
        case _ =>
          super.loadClass(name, resolve)
      }
    }
  }

  test("Shim_v4_0 partition projection happy path with sharedSDPartitionSpec") {
    val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
    table.setPartCols(java.util.Arrays.asList(
      new FieldSchema("p1", "string", null),
      new FieldSchema("p2", "string", null)))
    table.getSd.setLocation("hdfs://nn/warehouse/t")

    var capturedRequest: MockGetPartitionsRequest = null
    val p1 = new MockPartitionWithoutSD(
      java.util.Arrays.asList("1", "10"), "p1=1/p2=10")
    val p2 = new MockPartitionWithoutSD(
      java.util.Arrays.asList("1", "20"), "/p1=1/p2=20")
    val sharedSD = new MockPartitionSpecWithSharedSD(java.util.Arrays.asList(p1, p2))
    val pSpec = new MockPartitionSpec()
    pSpec.setRootPath("hdfs://nn/warehouse/t")
    pSpec.setSharedSDPartitionSpec(sharedSD)
    val mockResponse = new MockGetPartitionsResponse(
      java.util.Collections.singletonList(pSpec))

    val mockMsc = new MockMetaStoreClient {
      override def getPartitionsWithSpecs(
          request: MockGetPartitionsRequest): MockGetPartitionsResponse = {
        capturedRequest = request
        mockResponse
      }
    }

    val shim = new Shim_v4_0(new MockHive4ClassLoader) {
      override protected def getMetastoreClient(
          hive: org.apache.hadoop.hive.ql.metadata.Hive): AnyRef = mockMsc
    }

    val partSpec = new java.util.HashMap[String, String]()
    partSpec.put("p1", "1")
    val summaries = shim.getPartitionSummaries(
      null, table, partSpec, PartitionFieldMask.LocationOnly)

    assert(capturedRequest != null)
    assert(capturedRequest.getDbName == "default")
    assert(capturedRequest.getTblName == "t")
    assert(capturedRequest.getProjectionSpec.getFieldList ==
      java.util.Arrays.asList("values", "sd.location"))
    assert(capturedRequest.getFilterSpec.getFilterMode == MockPartitionFilterMode.BY_VALUES)
    assert(capturedRequest.getFilterSpec.getFilters == java.util.Arrays.asList("1"))

    assert(summaries.length == 2)
    assert(summaries.head.spec == Map("p1" -> "1", "p2" -> "10"))
    assert(summaries.head.locationUri.contains(
      new java.net.URI("hdfs://nn/warehouse/t/p1=1/p2=10")))
    assert(summaries(1).spec == Map("p1" -> "1", "p2" -> "20"))
    assert(summaries(1).locationUri.contains(
      new java.net.URI("hdfs://nn/warehouse/t/p1=1/p2=20")))
  }

  test("Shim_v4_0 partition projection happy path with partitionList (custom locations)") {
    val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
    table.setPartCols(java.util.Collections.singletonList(
      new FieldSchema("p1", "string", null)))

    val hivePart = new org.apache.hadoop.hive.metastore.api.Partition()
    hivePart.setValues(java.util.Collections.singletonList("custom_val"))
    val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
    sd.setLocation("hdfs://custom/path/p1=custom_val")
    hivePart.setSd(sd)

    val pList = new MockPartitionListComposingSpec(
      java.util.Collections.singletonList(hivePart))
    val pSpec = new MockPartitionSpec()
    pSpec.setPartitionList(pList)
    val mockResponse = new MockGetPartitionsResponse(
      java.util.Collections.singletonList(pSpec))

    val mockMsc = new MockMetaStoreClient {
      override def getPartitionsWithSpecs(
          request: MockGetPartitionsRequest): MockGetPartitionsResponse = mockResponse
    }

    val shim = new Shim_v4_0(new MockHive4ClassLoader) {
      override protected def getMetastoreClient(
          hive: org.apache.hadoop.hive.ql.metadata.Hive): AnyRef = mockMsc
    }

    val summaries = shim.getPartitionSummaries(
      null, table, java.util.Collections.emptyMap(), PartitionFieldMask.LocationOnly)

    assert(summaries.length == 1)
    assert(summaries.head.spec == Map("p1" -> "custom_val"))
    assert(summaries.head.locationUri.contains(
      new java.net.URI("hdfs://custom/path/p1=custom_val")))
  }

  test("Shim_v4_0 partition projection with SpecOnly mask") {
    val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
    table.setPartCols(java.util.Collections.singletonList(
      new FieldSchema("p1", "string", null)))

    var capturedRequest: MockGetPartitionsRequest = null
    val p = new MockPartitionWithoutSD(
      java.util.Collections.singletonList("1"), "p1=1")
    val sharedSD = new MockPartitionSpecWithSharedSD(java.util.Collections.singletonList(p))
    val pSpec = new MockPartitionSpec()
    pSpec.setSharedSDPartitionSpec(sharedSD)
    val mockResponse = new MockGetPartitionsResponse(
      java.util.Collections.singletonList(pSpec))

    val mockMsc = new MockMetaStoreClient {
      override def getPartitionsWithSpecs(
          request: MockGetPartitionsRequest): MockGetPartitionsResponse = {
        capturedRequest = request
        mockResponse
      }
    }

    val shim = new Shim_v4_0(new MockHive4ClassLoader) {
      override protected def getMetastoreClient(
          hive: org.apache.hadoop.hive.ql.metadata.Hive): AnyRef = mockMsc
    }

    val summaries = shim.getPartitionSummaries(
      null, table, java.util.Collections.emptyMap(), PartitionFieldMask.SpecOnly)

    assert(capturedRequest.getProjectionSpec.getFieldList ==
      java.util.Collections.singletonList("values"))
    assert(summaries.length == 1)
    assert(summaries.head.spec == Map("p1" -> "1"))
    assert(summaries.head.locationUri.isEmpty)
  }

  test("Shim_v4_0 non-prefix partition spec does not push down BY_VALUES filter") {
    val table = new org.apache.hadoop.hive.ql.metadata.Table("default", "t")
    table.setPartCols(java.util.Arrays.asList(
      new FieldSchema("year", "string", null),
      new FieldSchema("month", "string", null)))

    var capturedRequest: MockGetPartitionsRequest = null
    val mockResponse = new MockGetPartitionsResponse(java.util.Collections.emptyList())
    val mockMsc = new MockMetaStoreClient {
      override def getPartitionsWithSpecs(
          request: MockGetPartitionsRequest): MockGetPartitionsResponse = {
        capturedRequest = request
        mockResponse
      }
    }

    val shim = new Shim_v4_0(new MockHive4ClassLoader) {
      override protected def getMetastoreClient(
          hive: org.apache.hadoop.hive.ql.metadata.Hive): AnyRef = mockMsc
    }

    val nonPrefixSpec = new java.util.HashMap[String, String]()
    nonPrefixSpec.put("month", "10")
    shim.getPartitionSummaries(null, table, nonPrefixSpec, PartitionFieldMask.LocationOnly)

    assert(capturedRequest != null)
    // Non-prefix spec does not set filterMode/filters to avoid DirectSQL wildcard matching bugs,
    // while providing an empty filterSpec to avoid Hive 4.0 HMS ObjectStore NPE.
    assert(capturedRequest.getFilterSpec != null)
    assert(capturedRequest.getFilterSpec.getFilterMode == null)
    assert(capturedRequest.getFilterSpec.getFilters == null)
  }
}
