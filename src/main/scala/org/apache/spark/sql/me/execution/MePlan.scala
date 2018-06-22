package org.apache.spark.sql.me.execution

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.me.MeSession

import scala.collection.mutable

abstract class MePlan extends SparkPlan{
  @transient
  protected[me] final val meSessionState = MeSession.getActiveSession.map(_.sessionState).orNull

  protected override def sparkContext: SparkContext = MeSession.getActiveSession.map(_.sparkContext).orNull

}
