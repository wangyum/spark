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

package org.apache.spark.sql.hive.client;

public class MockGetPartitionsRequest {
  private String dbName;
  private String tblName;
  private String catName;
  private MockGetProjectionsSpec projSpec;
  private MockGetPartitionsFilterSpec filterSpec;

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTblName() {
    return tblName;
  }

  public void setTblName(String tblName) {
    this.tblName = tblName;
  }

  public String getCatName() {
    return catName;
  }

  public void setCatName(String catName) {
    this.catName = catName;
  }

  public MockGetProjectionsSpec getProjectionSpec() {
    return projSpec;
  }

  public void setProjectionSpec(MockGetProjectionsSpec projSpec) {
    this.projSpec = projSpec;
  }

  public MockGetPartitionsFilterSpec getFilterSpec() {
    return filterSpec;
  }

  public void setFilterSpec(MockGetPartitionsFilterSpec filterSpec) {
    this.filterSpec = filterSpec;
  }
}
