package org.apache.spark.sql.me.execution

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Strategy, catalyst}
import org.apache.spark.sql.execution.{SparkPlanner, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

import org.apache.spark.sql.me.MeSession
import org.apache.spark.sql.me.optimizer._


class MePlanner(val meContext: MeSession,
                override val conf: SQLConf,
                override val extraStrategies: Seq[Strategy])
  extends SparkPlanner(meContext.sparkContext, conf, extraStrategies){

  override def strategies: Seq[Strategy] = (MatrixOperators :: Nil) ++ super.strategies
}

object MatrixOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match{
    case TransposeOperator(child) =>
      child match {
        case TransposeOperator(gchild) => planLater(gchild) :: Nil
        case _ => MatrixTransposeExecution(planLater(child)) :: Nil
      }

    case MatrixElementDivideOperator(p, q, left, leftRowNum, leftColNum, right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementDivideExecution(p, q, planLater(left), leftRowNum, leftColNum, planLater(right), rightRowNum, rightColNum, blkSize) :: Nil

    case MatrixElementMultiplyOperator(p, q, left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementMultiplyExecution(p, q, planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, blkSize) :: Nil

    case MatrixElementAddOperator(p, q, left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      MatrixElementAddExecution(p, q, planLater(left), leftRowNum, leftColNum,
        planLater(right), rightRowNum, rightColNum, blkSize) :: Nil

    case MatrixMatrixMultiplicationOperator(p, q, left, leftRowNum, leftColNum,
    right, rightRowNum, rightColNum, blkSize) =>
      if (leftRowNum == 1L && leftColNum > 1L) {
        right match {
          case TransposeOperator(ch) =>
            MatrixTransposeExecution(planLater(
              MatrixMatrixMultiplicationOperator(p, q, ch, rightColNum, rightRowNum,
                TransposeOperator(left), leftColNum, leftRowNum, blkSize))) :: Nil
          case _ => MatrixMatrixMultiplicationExecution(p, q, planLater(left), leftRowNum, leftColNum,
            planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
        }
      } else if (rightRowNum > 1L && rightColNum == 1L) {
        left match {
          case TransposeOperator(ch) =>
            MatrixTransposeExecution(planLater(
              MatrixMatrixMultiplicationOperator(p, q, TransposeOperator(right), rightColNum, rightRowNum,
                ch, leftColNum, leftRowNum, blkSize))) :: Nil
          case _ => MatrixMatrixMultiplicationExecution(p, q, planLater(left), leftRowNum, leftColNum,
            planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
        }
      } else {
        MatrixMatrixMultiplicationExecution(p, q, planLater(left), leftRowNum, leftColNum,
          planLater(right), rightRowNum, rightColNum, blkSize) :: Nil
      }

    case _ => Nil
  }
}
