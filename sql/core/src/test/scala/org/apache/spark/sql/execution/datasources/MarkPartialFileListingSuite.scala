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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession

class MarkPartialFileListingSuite extends QueryTest with SharedSparkSession {

  private def hasMarkTag(plan: LogicalPlan): Boolean = {
    plan.collect {
      case lr: LogicalRelation => lr.getTagValue(MarkPartialFileListing.markTag).contains(true)
    }.contains(true)
  }

  test("limit-only query marks the relation by default") {
    withTable("t") {
      spark.range(10).write.saveAsTable("t")
      val df = sql("SELECT id FROM t LIMIT 5")
      assert(hasMarkTag(df.queryExecution.optimizedPlan))
    }
  }

  test("offset query marks the relation by default") {
    withTable("t") {
      spark.range(10).write.saveAsTable("t")
      val df = sql("SELECT id FROM t LIMIT 5 OFFSET 2")
      assert(hasMarkTag(df.queryExecution.optimizedPlan))
    }
  }

  test("filter disqualifies marking") {
    withTable("t") {
      spark.range(10).write.saveAsTable("t")
      val df = sql("SELECT id FROM t WHERE id > 2 LIMIT 5")
      assert(!hasMarkTag(df.queryExecution.optimizedPlan))
    }
  }

  test("order by disqualifies marking") {
    withTable("t") {
      spark.range(10).write.saveAsTable("t")
      val df = sql("SELECT id FROM t ORDER BY id LIMIT 5")
      assert(!hasMarkTag(df.queryExecution.optimizedPlan))
    }
  }

  test("aggregate disqualifies marking") {
    withTable("t") {
      spark.range(10).write.saveAsTable("t")
      val df = sql("SELECT count(*) FROM t LIMIT 5")
      assert(!hasMarkTag(df.queryExecution.optimizedPlan))
    }
  }

  test("bucketed table disqualifies marking") {
    withTable("t") {
      spark.range(10).write.bucketBy(2, "id").saveAsTable("t")
      val df = sql("SELECT id FROM t LIMIT 5")
      assert(!hasMarkTag(df.queryExecution.optimizedPlan))
    }
  }
}
