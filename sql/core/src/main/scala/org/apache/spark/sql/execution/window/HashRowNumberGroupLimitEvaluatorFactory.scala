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

package org.apache.spark.sql.execution.window

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * Hash-based evaluator for [[WindowGroupLimitExec]] when
 * [[WindowGroupLimitExec.isHashEligible]] is true. Unlike [[WindowGroupLimitEvaluatorFactory]],
 * this does not require the input to be sorted -- see [[HashRowNumberGroupLimitAccumulator]] for
 * the algorithm.
 */
class HashRowNumberGroupLimitEvaluatorFactory(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric,
    peakMemory: SQLMetric,
    spillSize: SQLMetric,
    numTasksFallBackedOOM: SQLMetric,
    numTasksFallBackedPoorCompaction: SQLMetric,
    minCompaction: Double,
    minRowsBetweenCompactionChecks: Long,
    enableRadixSort: Boolean,
    limit: Int,
    mode: WindowGroupLimitMode = Partial)
  extends HashGroupLimitEvaluatorFactory {

  /**
   * Builds a fresh, per-task [[HashRowNumberGroupLimitAccumulator]]. Used both by
   * [[HashGroupLimitEvaluatorFactory.createEvaluator]] (the interpreted `doExecute()` path) and
   * directly by [[WindowGroupLimitExec]]'s whole-stage codegen, which drives the accumulator with
   * generated code instead.
   */
  override def createAccumulator(): HashRowNumberGroupLimitAccumulator =
    new HashRowNumberGroupLimitAccumulator(
      partitionSpec, orderSpec, childOutput, numOutputRows, peakMemory, spillSize,
      numTasksFallBackedOOM, numTasksFallBackedPoorCompaction, minCompaction,
      minRowsBetweenCompactionChecks, enableRadixSort, limit = limit, mode = mode)
}
