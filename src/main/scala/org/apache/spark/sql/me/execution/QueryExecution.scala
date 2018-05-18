package org.apache.spark.sql.me.execution

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.me.MeSession
import org.apache.spark.sql.execution.{QueryExecution => SQLQueryExecution, SparkPlan}

class QueryExecution(val meSession: MeSession, val meLogical: LogicalPlan) extends SQLQueryExecution(meSession, meLogical) {

  lazy val matrixData: LogicalPlan ={
    assertAnalyzed()
    withCachedData
  }

  override lazy val optimizedPlan: LogicalPlan ={
    meSession.sessionState.meOptimizer.execute(meSession.sessionState.getSQLOptimizer.execute(matrixData))
  }

  override lazy val sparkPlan: SparkPlan ={
    MeSession.setActiveSession(meSession)
    meSession.sessionState.mePlanner.plan(optimizedPlan).next()
  }
}
