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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

/**
 * This optimizer rule marks a limit-only query's scan so it can list only a handful of files
 * instead of every file under the table: a LIMIT with no Filter, Sort, or Aggregate above an
 * unbucketed file-based scan. `FileSourceScanExec` reads the mark and, when eligible, caps the
 * file index's listing and falls back to listing more files on the driver if the given ones do
 * not satisfy the limit. See `FileSourceScanExec.isPartialFileListing`.
 *
 * The matching is deliberately strict and conservative: any operator that needs to see every row
 * (a filter, a sort, an aggregate) makes the plan ineligible, since capping the listing could then
 * silently return fewer rows than the query would otherwise produce. This mirrors
 * [[MarkSingleTaskExecution]]'s shape, but the two optimizations solve different problems (listing
 * fewer files vs. eliding a shuffle for an already-fully-listed scan) and must stay independent:
 * `FileSourceScanExec` gives a relation marked here priority over single-task execution, since the
 * latter requires an accurate file count that a capped listing does not have.
 */
object MarkPartialFileListing extends Rule[LogicalPlan] {

  /**
   * Tag placed on a [[LogicalRelation]] eligible for partial file listing. `FileSourceStrategy`
   * reads this tag to propagate the decision to the physical
   * [[org.apache.spark.sql.execution.FileSourceScanExec]].
   */
  val markTag: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("__partial_file_listing")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // A Limit or Offset must actually be present at the root: capping the listing is only sound
    // when something bounds how many rows the query can need. A bare Project/relation with no
    // Limit above it would otherwise be marked "eligible" despite expecting every row.
    plan match {
      case (_: GlobalLimit | _: LocalLimit | _: Offset) if isSupportedShape(plan) =>
        // Mark a private clone of the plan rather than the plan itself, for the same reason
        // MarkSingleTaskExecution does: tagging a shared node in place would leak the mark into
        // unrelated plans through `TreeNode.clone`/`copyTagsFrom`.
        val cloned = plan.clone()
        markPartialFileListing(cloned)
        cloned
      case _ => plan
    }
  }

  /**
   * Returns true if every operator in the plan is a Project, a Limit, an Offset, or an
   * unbucketed file-based relation. Any other operator -- in particular a Filter, Sort, or
   * Aggregate -- makes the plan ineligible.
   */
  private def isSupportedShape(plan: LogicalPlan): Boolean = plan match {
    case LogicalRelation(relation: HadoopFsRelation, _, _, _, _) =>
      relation.bucketSpec.isEmpty
    case _: Project | _: GlobalLimit | _: LocalLimit | _: Offset =>
      plan.children.forall(isSupportedShape)
    case _ => false
  }

  /** Sets the mark on the scan in the given (already validated) plan. */
  private def markPartialFileListing(plan: LogicalPlan): Unit = plan match {
    case lr: LogicalRelation =>
      lr.setTagValue(markTag, true)
    case other =>
      other.children.foreach(markPartialFileListing)
  }
}
