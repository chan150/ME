package org.apache.spark.sql.me

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{Encoder,Row,  me, Dataset => SQLDataset}
import org.apache.spark.sql.me.optimizer._

class Dataset[T] private[me] (@transient val meSession: me.MeSession,
                              @transient override val queryExecution: QueryExecution,
                              encoder: Encoder[T]) extends SQLDataset[T](meSession, queryExecution.logical, encoder){



  def this(sparkSession: me.MeSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) ={
    this(sparkSession, sparkSession.sessionState.executePlan(logicalPlan), encoder)
  }


  def transpose(data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan{
    TransposeOperator(this.logicalPlan)
  }

  def matrixMultiply(
                     leftRowNum: Long, leftColNum: Long,
                     right: Dataset[_], rightRowNum: Long, rightColNum: Long,
                     blkSize: Int, data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    MatrixMatrixMultiplicationOperator(this.logicalPlan, leftRowNum, leftColNum, right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  def divideElement(p:Int, q: Int,
                    leftRowNum: Long, leftColNum: Long,
                    right: Dataset[_], rightRowNum: Long, rightColNum: Long,
                    blkSize: Int, data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    MatrixElementDivideOperator(p, q, this.logicalPlan, leftRowNum, leftColNum, right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  def multiplyElement(p:Int, q: Int,
                      leftRowNum: Long, leftColNum: Long,
                      right: Dataset[_], rightRowNum: Long, rightColNum: Long,
                      blkSize: Int, data: Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan {
    MatrixElementMultiplyOperator(p, q, this.logicalPlan, leftRowNum, leftColNum, right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  def addElement(p:Int, q: Int,
                 leftRowNum: Long, leftColNum: Long,
                 right: Dataset[_], rightRowNum: Long, rightColNum: Long,
                 blkSize: Int, data:Seq[Attribute] = this.queryExecution.analyzed.output): DataFrame = withPlan{
    MatrixElementAddOperator(p, q, this.logicalPlan, leftRowNum, leftColNum, right.logicalPlan, rightRowNum, rightColNum, blkSize)
  }

  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame ={
    Dataset.ofRows(meSession, logicalPlan)
  }
}

private[me] object Dataset{
  def apply[T: Encoder](sparkSession: me.MeSession, logicalPlan: LogicalPlan): Dataset[T] ={
    new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(sparkSession: me.MeSession, logicalPlan: LogicalPlan): DataFrame ={
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
}