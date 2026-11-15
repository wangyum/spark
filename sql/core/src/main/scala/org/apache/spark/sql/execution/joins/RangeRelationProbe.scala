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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Shared range-index probe dispatch for broadcast and per-key-group sort-merge joins.
 */
private[joins] object RangeRelationProbe {

  /**
   * Create candidate probe function for a materialized range relation and bound probe keys.
   * `pointScan` is by-name so it is only forced inside the point-index branches.
   */
  def makeCandidateFn(
      relation: RangeRelation,
      boundStreamKeys: Seq[Expression],
      pointScan: => (PointScan, Boolean)): InternalRow => Iterator[InternalRow] = {
    val isOneKey = boundStreamKeys.length == 1
    val lowExpr = boundStreamKeys.head
    val highExpr = if (isOneKey) null else boundStreamKeys(1)

    relation match {
      case longs: LongIntervalIndex => row =>
        val low = lowExpr.eval(row)
        if (low == null) {
          Iterator.empty
        } else {
          val high = if (isOneKey) low else highExpr.eval(row)
          if (high == null) {
            Iterator.empty
          } else {
            longs.overlapping(longs.toLong(low), longs.toLong(high))
          }
        }
      case points: LongPointIndex =>
        val (direction, strict) = pointScan
        val scan: Long => Iterator[InternalRow] = direction match {
          case PointScan.UpTo => points.upTo(_, strict)
          case PointScan.From => points.from(_, strict)
        }
        row =>
          val low = lowExpr.eval(row)
          if (low == null) Iterator.empty else scan(points.toLong(low))
      case intervals: IntervalIndex => row =>
        val low = lowExpr.eval(row)
        if (low == null) {
          Iterator.empty
        } else {
          val high = if (isOneKey) low else highExpr.eval(row)
          intervals.overlapping(low, high)
        }
      case points: PointIndex =>
        val (direction, strict) = pointScan
        val scan: Any => Iterator[InternalRow] = direction match {
          case PointScan.UpTo => points.upTo(_, strict)
          case PointScan.From => points.from(_, strict)
        }
        row =>
          val low = lowExpr.eval(row)
          if (low == null) Iterator.empty else scan(low)
    }
  }
}