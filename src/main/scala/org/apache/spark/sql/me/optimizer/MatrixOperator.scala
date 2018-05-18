package org.apache.spark.sql.me.optimizer

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.expressions.Attribute


case class TransposeOperator(child: LogicalPlan) extends UnaryNode{
  override def output: Seq[Attribute] = child.output
}

case class MatrixScalarAddOperator(child: LogicalPlan, scalar: Double) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class MatrixScalarMultiplyOperator(child:LogicalPlan, scalar: Double) extends  UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class MatrixElementAddOperator(p:Int, q: Int,
                                    leftChild: LogicalPlan,
                                    leftRowNum: Long,
                                    leftColNum: Long,
                                    rightChild: LogicalPlan,
                                    rightRowNum: Long,
                                    rightColNum: Long,
                                    blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output
  override def left: LogicalPlan = leftChild
  override def right: LogicalPlan = rightChild
}

case class MatrixElementMultiplyOperator(p:Int, q: Int,
                                         leftChild: LogicalPlan,
                                         leftRowNum: Long,
                                         leftColNum: Long,
                                         rightChild: LogicalPlan,
                                         rightRowNum: Long,
                                         rightColNum: Long,
                                         blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output
  override def left: LogicalPlan = leftChild
  override def right: LogicalPlan = rightChild
}

case class MatrixElementDivideOperator(p:Int, q: Int,
                                       leftChild: LogicalPlan,
                                       leftRowNum: Long,
                                       leftColNum: Long,
                                       rightChild: LogicalPlan,
                                       rightRowNum: Long,
                                       rightColNum: Long,
                                       blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output
  override def left: LogicalPlan = leftChild
  override def right: LogicalPlan = rightChild
}

case class MatrixMatrixMultiplicationOperator(p:Int, q: Int,
                                              leftChild: LogicalPlan,
                                              leftRowNum: Long,
                                              leftColNum: Long,
                                              rightChild: LogicalPlan,
                                              rightRowNum: Long,
                                              rightColNum: Long,
                                              blkSize: Int) extends BinaryNode {
  override def output: Seq[Attribute] = leftChild.output
  override def left: LogicalPlan = leftChild
  override def right: LogicalPlan = rightChild
}

case class MatrixDivideColumnVector(p:Int, q: Int,
                                    leftChild: LogicalPlan,
                                    leftRowNum: Long,
                                    leftColNum: Long,
                                    rightChild: LogicalPlan,
                                    rightRowNum: Long,
                                    rightColNum: Long,
                                    blkSize: Int) extends BinaryNode {

  override def output: Seq[Attribute] = leftChild.output

  override def left: LogicalPlan = leftChild

  override def right: LogicalPlan = rightChild
}

case class MatrixDivideRowVector(p:Int, q: Int,
                                 leftChild: LogicalPlan,
                                 leftRowNum: Long,
                                 leftColNum: Long,
                                 rightChild: LogicalPlan,
                                 rightRowNum: Long,
                                 rightColNum: Long,
                                 blkSize: Int) extends BinaryNode {

  override def output: Seq[Attribute] = leftChild.output

  override def left: LogicalPlan = leftChild

  override def right: LogicalPlan = rightChild
}