package org.apache.spark.sql.me

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{execution => SQLexecution, _}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.me.optimizer.MeOptimizer
import org.apache.spark.sql.me.execution.MePlanner

private[me] class MeSessionState(meSession: MeSession) extends SessionState(meSession){
  self =>

  protected[me] lazy val meConf = new MeConf

  protected[me] def getSQLOptimizer = optimizer

  protected[me] lazy val meOptimizer: MeOptimizer = new MeOptimizer

  protected[me] val mePlanner: SQLexecution.SparkPlanner = {
    new MePlanner(meSession, conf, experimentalMethods.extraStrategies)
  }

  override def executePlan(plan: LogicalPlan) = new execution.QueryExecution(meSession, plan)

  def setConf(key: String, value: String) :Unit ={
    if(key.startsWith("me.")) meConf.setConfString(key, value)
    else conf.setConfString(key, value)
  }

  def getConf(key: String) :String ={
    if(key.startsWith("me.")) meConf.getConfString(key)
    else conf.getConfString(key)
  }

  def getConf(key:String, defaultValue: String): String={
    if(key.startsWith("me.")) conf.getConfString(key, defaultValue)
    else conf.getConfString(key, defaultValue)
  }

  def getAllConfs: scala.collection.immutable.Map[String, String] ={
    conf.getAllConfs ++ meConf.getAllConfs
  }
}
