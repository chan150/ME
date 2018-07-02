package org.apache.spark.sql.me.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, GenericInternalRow}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.me.serializer.DMatrixSerializer
import org.apache.spark.sql.me.partitioner.{IndexPartitioner, RedunColPartitioner, RedunRowPartitioner, RowPartitioner}
import org.apache.spark.sql.types.DataType

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
//
//trait MetadataExec extends UnaryExecNode {
//  assert(child.output.length == 1)
//
//  // This operator always need all columns of its child, even it doesn't reference to.
//  override def references: AttributeSet = child.outputSet
//
//  def inputObjectType: DataType = child.output.head.dataType
//}


case class MatrixMatrixMultiplicationExecution(
                                               left: SparkPlan,
                                               leftRowNum: Long,
                                               leftColNum: Long,
                                               right: SparkPlan,
                                               rightRowNum: Long,
                                               rightColNum: Long,
                                               blkSize: Int) extends MePlan {

  override def output: Seq[Attribute] = left.output

  override def children: Seq[SparkPlan] = Seq (left, right)

//  override def metadata: Map[String, String] = right.metadata ++ left.metadata ++ Map((rightRowNum.toString ->rightColNum.toString ))

  override def metadata: Map[String, String] = if(left.metadata.contains("part") && right.metadata.contains("part")){
    val v = left.metadata.get("part").get
    val v2 = right.metadata.get("part").get

    if(v.equals(v2)){
      val newv = v match{
        case "row" => "col"
        case "col" => "row"
        case _ => "error"
      }
      Map(("part"-> newv), ("equls" -> "existing"))
    } else{
      Map(("part"-> v))
    }


  } else if(left.metadata.contains("part") && !right.metadata.contains("part")){
    val newV = left.metadata.get("part").get match {
      case "row" => "col"
      case "col" => "row"
      case _ => "error"
    }
    Map(("part"-> newV), ("left"-> "existing"))
  } else if(!left.metadata.contains("part") && right.metadata.contains("part")) {
    val newV = right.metadata.get("part").get match {
      case "row" => "col"
      case "col" => "row"
      case _ => "error"
    }
    Map(("part"-> newV), ("right"-> "existing"))
  } else{
    Map(("part"-> "row"), ("notboth" ->"none"))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    require(leftColNum == rightRowNum, s"Matrix dimension not match, leftColNum = $leftColNum, rightRowNum =$rightRowNum")

    val memoryUsage = ((leftRowNum * rightColNum * 8) / (1024 * 1024 * 1024 * 1.0))
    println(s"the size of result matrix: $memoryUsage GB")
    if (memoryUsage > 10) {
      println(s"Caution: matrix multiplication result size = $memoryUsage GB")
    }

    org.apache.spark.sql.execution.CoGroupExec

    val blkMemorySize = (blkSize * blkSize * 8) / (1024 * 1024 * 1024 * 1.0)
    val limitNumBlk = (2 / blkMemorySize).toInt

    println(s"the memory size of a block: $blkMemorySize GB")
    println(s"the limitation for the number of block in a partition of RDD: $limitNumBlk")

    val bcV = mutable.HashMap[(Int, Int), Array[Int]]()

    val bc = left.sqlContext.sparkSession.sparkContext.broadcast(bcV)


    val sc = left.sqlContext.sparkSession.sparkContext.getConf
//        sc.getAll.foreach(println)

    require(sc.contains("spark.executor.cores"), s"There are not configuration for spark.executor.cores ")

    val coreExecutor = sc.get("spark.executor.cores").toInt
    val coreTask = sc.contains("spark.task.cpus") match {
      case true =>
        sc.get("spark.task.cpus").toInt
      case false =>
        1
    }

    require(sc.contains("spark.executor.memory"), s"There are not configuration for spark.executor.memory ")
    val memoryExecutor = sc.get("spark.executor.memory").replace("g", "").toDouble

    //    println(left.sqlContext.sparkSession.sparkContext.statusTracker.getExecutorInfos.length)
    //    val execute = left.sqlContext.sparkSession.sparkContext.statusTracker.getExecutorInfos
    //    println(execute.length)
    //    execute.distinct.foreach(a => println(s"${a.host()}"))

    //    println(s"default parallelism: ${left.sqlContext.sparkSession.sparkContext.defaultParallelism}")

    val numExecutors = left.sqlContext.sparkSession.sparkContext.getExecutorIds().length

    val nodeParallelism = (coreExecutor / coreTask)

    val TaskParallelism = nodeParallelism * numExecutors


    //    val n = 10

    println(s"NodeParallelism = $nodeParallelism, TaskParallelism : $TaskParallelism")

    val leftRowBlkNum = math.ceil(leftRowNum * 1.0 / blkSize).toInt
    val leftColBlkNum = math.ceil(leftColNum * 1.0 / blkSize).toInt
    val leftTotalBlkNum = leftRowBlkNum * leftColBlkNum

    val rightRowBlkNum = math.ceil(rightRowNum * 1.0 / blkSize).toInt
    val rightColBlkNum = math.ceil(rightColNum * 1.0 / blkSize).toInt
    val rightTotalBlkNum = rightRowBlkNum * rightColBlkNum


    println(s"left: ($leftRowNum, $leftColNum), blkSize: $blkSize")
    println(s"righ: ($rightRowNum, $rightColNum), blkSize: $blkSize")
    println(s"memoryExecutor/nodeParallelism: ${memoryExecutor/nodeParallelism}")


    println(s"metadata size: ${metadata.size}")
    metadata.foreach(println)

    val master = left.sqlContext.sparkSession.sparkContext.master.replace("spark://", "").split(":")(0)
    val slaves = left.sqlContext.sparkSession.sparkContext.statusTracker.getExecutorInfos.filter(_.host() != master).map{a =>
      a.host()}.sorted

    println(s"${slaves.foreach(println)}, ${slaves.length}")

    val matA = left.execute()
    val matB = right.execute()

    val p = 10
    val q = 6



    println(s"Test: ${new CoLocatedMatrixRDD(left.sqlContext.sparkSession.sparkContext,
      matA.flatMap{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)

      val startingPoint = Math.floor((rid*1.0/(leftRowBlkNum*1.0/p * 1.0))).toInt * q
      (startingPoint to startingPoint + (q-1)).map{ i =>
        (i, ((rid, cid), mat))
      }
    }.groupByKey(new IndexPartitioner(p*q, new RedunRowPartitioner(q, p))),
      matB.flatMap{ row =>
        val pid = row.getInt(0)
        val rid = row.getInt(1)
        val cid = row.getInt(2)
        val mat = row.getStruct(3, 7)

        val startPoint = Math.floor((cid*1.0/(rightColBlkNum*1.0/ q * 1.0)))
        (0 to (p -1)).map{i =>
          ((q*i)+startPoint.toInt, ((rid, cid), mat))
        }
      }.groupByKey(new IndexPartitioner(p*q, new RedunColPartitioner(p, q))), 1, 1, master, slaves).count()}")

        //    if(leftColBlkNum == 1 && rightRowBlkNum == 1){
    //
    //      if(leftRowBlkNum <= rightColBlkNum){
    //        MeExecutionHelper.multiplyOuterProductDuplicationLeft(n, left.execute(), right.execute(), rightColBlkNum)
    //      } else{
    //        MeExecutionHelper.multiplyOuterProductDuplicationRight(n, left.execute(), right.execute(), leftRowBlkNum)
    //      }
    //    } else {
    ////      MeExecutionHelper.matrixMultiplyGeneral(left.execute(), right.execute(), bc)
    //    }

//    MeMMExecutionHelper.rmmDuplicationRight(60, matA, matB, leftRowBlkNum, rightColBlkNum)
    MeMMExecutionHelper.cpmm(10, matA, matB,leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, new RowPartitioner(10, leftRowBlkNum))
//    MeMMExecutionHelper.redundancyInnerMM(2,5, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum)
//    MeMMExecutionHelper.rmmWithoutPartition(left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum)
//    if (leftTotalBlkNum <= limitNumBlk && leftTotalBlkNum <= rightTotalBlkNum) {
    ////      val n = if (TaskParallelism > rightColBlkNum) rightColBlkNum else TaskParallelism
    ////      println(s"In rmmDuplicationLeft, number of partition: $n")
    ////
    ////      MeMMExecutionHelper.rmmDuplicationLeft(n, left.execute(), right.execute(), leftRowBlkNum, rightColBlkNum)
    ////    } else if (rightTotalBlkNum <= limitNumBlk && leftTotalBlkNum > rightTotalBlkNum) {
    ////
    ////      val n = if (TaskParallelism > leftRowBlkNum) leftRowBlkNum else TaskParallelism
    ////      println(s"In rmmDuplicationRight, number of partition: $n")
    ////
    ////      MeMMExecutionHelper.rmmDuplicationRight(n, left.execute(), right.execute(), leftRowBlkNum, rightColBlkNum)
    ////    } else if(leftColBlkNum <= limitNumBlk && rightRowBlkNum <= limitNumBlk && memoryUsage < (memoryExecutor/nodeParallelism)){
    ////      val n = if (TaskParallelism > leftRowBlkNum) leftRowBlkNum else TaskParallelism
    ////      println(s"In cpmm, number of partition: $n")
    ////      MeMMExecutionHelper.cpmm(n, left.execute(), right.execute(),leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, new RowPartitioner(n, leftRowBlkNum))
    ////    } else{
    ////      println(s"In rmmWithoutPartition")
    ////      MeMMExecutionHelper.rmmWithoutPartition(left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum)
    ////    }
  }

}