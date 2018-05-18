package org.apache.spark.sql.me.optimizer

import org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughJoin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class MeOptimizer extends RuleExecutor[LogicalPlan]{
  val batches = Batch("LocalRelation", FixedPoint(100), PushPredicateThroughJoin) :: Nil
}
