package org.apache.spark.sql.me.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.me.serializer.DMatrixSerializer
import org.apache.spark.sql.me.partitioner.RowPartitioner

import scala.collection.mutable

case class MatrixTransposeExecution(child: SparkPlan) extends MePlan {

  override def output: Seq[Attribute] =  child.output

  override def children: Seq[SparkPlan] = child :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val rootRdd = child.execute()
    rootRdd.map{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val matrixInternalRow = row.getStruct(3, 7)
      val res = new GenericInternalRow(4)
      val matrix = DMatrixSerializer.deserialize(matrixInternalRow)
      val matrixRow = DMatrixSerializer.serialize(matrix.transpose)
      res.setInt(0, pid)
      res.setInt(1, cid)
      res.setInt(2, rid)
      res.update(3, matrixRow)

      res
    }
  }
}

case class MatrixElementDivideExecution(p:Int, q: Int,
                                        left: SparkPlan,
                                       leftRowNum: Long,
                                       leftColNum: Long,
                                       right: SparkPlan,
                                       rightRowNum: Long,
                                       rightColNum: Long,
                                       blkSize: Int) extends MePlan {
  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftRowNum == rightRowNum, s"Row number not match, leftRowNum = $leftRowNum, rightRowNum = $rightRowNum")
    require(leftColNum == rightColNum, s"Row number not match, leftRowNum = $leftColNum, rightRowNum = $rightColNum")

    if (left.children == right.children){
      MeExecutionHelper.selfElementDivide(left.execute())
    } else{
      val rdd1 = left.execute()
      val rdd2 = right.execute()

      if(rdd1.partitioner != None) {
        val part = rdd1.partitioner.get
        MeExecutionHelper.divideWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
      } else if (rdd2.partitioner != None){
        val part = rdd2.partitioner.get
        MeExecutionHelper.divideWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
      } else{
        val part = new RowPartitioner(p*q, leftRowNum/blkSize)
        MeExecutionHelper.divideWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, rdd1),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, rdd2))
      }
    }
  }
}

case class MatrixElementMultiplyExecution(p:Int, q: Int,
                                          left: SparkPlan,
                                          leftRowNum: Long,
                                          leftColNum: Long,
                                          right: SparkPlan,
                                          rightRowNum: Long,
                                          rightColNum: Long,
                                          blkSize: Int) extends MePlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftRowNum == rightRowNum, s"Row number not match, leftRowNum = $leftRowNum, rightRowNum = $rightRowNum")
    require(leftColNum == rightColNum, s"Row number not match, leftRowNum = $leftColNum, rightRowNum = $rightColNum")

    if (left.children == right.children){
      MeExecutionHelper.selfElementMultiply(left.execute())
    } else {
      val A = left.execute()
      val B = right.execute()
      if(A.partitioner != None){
        val part = A.partitioner.get
        MeExecutionHelper.multiplyWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, A),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, B))
      } else if (B.partitioner != None) {
        val part = B.partitioner.get
        MeExecutionHelper.multiplyWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, A),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, B))
      } else {
        val part = new RowPartitioner(p*q, leftRowNum/blkSize)
        MeExecutionHelper.divideWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, A),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, B))
      }

    }
  }
}

case class MatrixElementAddExecution(p:Int, q: Int,
                                     left: SparkPlan,
                                     leftRowNum: Long,
                                     leftColNum: Long,
                                     right: SparkPlan,
                                     rightRowNum: Long,
                                     rightColNum: Long,
                                     blkSize: Int) extends MePlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq(left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftRowNum == rightRowNum, s"Row number not match, leftRowNum = $leftRowNum, rightRowNum = $rightRowNum")
    require(leftColNum == rightColNum, s"Row number not match, leftRowNum = $leftColNum, rightRowNum = $rightColNum")

    if(left.children == right.children) {
      MeExecutionHelper.selfElementAdd(left.execute())
    } else {
      val A = left.execute()
      val B = right.execute()
      if(A.partitioner != None){
        val part = A.partitioner.get
        MeExecutionHelper.addWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, A),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, B))
      } else if (B.partitioner != None) {
        val part = B.partitioner.get
        MeExecutionHelper.addWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, A),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, B))
      } else {
        val part = new RowPartitioner(p*q, leftRowNum/blkSize)
        MeExecutionHelper.addWithPartitioner(
          MeExecutionHelper.repartitionWithTargetPartitioner(part, A),
          MeExecutionHelper.repartitionWithTargetPartitioner(part, B))
      }
    }
  }
}

case class MatrixMatrixMultiplicationExecution(p:Int, q: Int,
                                               left: SparkPlan,
                                               leftRowNum: Long,
                                               leftColNum: Long,
                                               right: SparkPlan,
                                               rightRowNum: Long,
                                               rightColNum: Long,
                                               blkSize: Int) extends MePlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq (left, right)

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftColNum == rightRowNum, s"Matrix dimension not match, leftColNum = $leftColNum, rightRowNum =$rightRowNum")

    val memoryUsage = leftRowNum * rightColNum * 8 / (1024 * 1024 * 1024) * 1.0
    if (memoryUsage > 10) {
      println(s"Caution: matrix multiplication result size = $memoryUsage GB")
    }

    val bcV = mutable.HashMap[(Int, Int), Array[Int]]()

    val bc = left.sqlContext.sparkSession.sparkContext.broadcast(bcV)

    val n = p * q
    val leftRowBlkNum = math.ceil(leftRowNum * 1.0 / blkSize).toInt
    val leftColBlkNum = math.ceil(leftColNum * 1.0 / blkSize).toInt

    val rightRowBlkNum = math.ceil(rightRowNum * 1.0 / blkSize).toInt
    val rightColBlkNum = math.ceil(rightColNum * 1.0 / blkSize).toInt

    if(leftColBlkNum == 1 && rightRowBlkNum == 1){



      if(leftRowBlkNum <= rightColBlkNum){
        MeExecutionHelper.multiplyOuterProductDuplicationLeft(n, left.execute(), right.execute(), rightColBlkNum)
      } else{
        MeExecutionHelper.multiplyOuterProductDuplicationRight(n, left.execute(), right.execute(), leftRowBlkNum)
      }
    } else {
//      MeExecutionHelper.matrixMultiplyGeneral(left.execute(), right.execute(), bc)
//      MeMMExecutionHelper.rmmDuplicationLeft(2, left.execute(), right.execute(),leftRowBlkNum, rightColBlkNum)

//      MeMMExecutionHelper.rmmDuplicationRight(2, left.execute(), right.execute(),leftRowBlkNum, rightColBlkNum)
      MeMMExecutionHelper.rmmWithoutPartition(left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum)

//      MeMMExecutionHelper.cpmm(2, left.execute(), right.execute(),leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, new RowPartitioner(2, leftRowBlkNum))
    }
  }
}