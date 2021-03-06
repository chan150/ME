package org.apache.spark.sql.me.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, GenericInternalRow}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.me.serializer.DMatrixSerializer
import org.apache.spark.sql.me.partitioner._
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

//  override def metadata: Map[String, String] = if(left.metadata.contains("part") && right.metadata.contains("part")){
//    val v = left.metadata.get("part").get
//    val v2 = right.metadata.get("part").get
//
//    if(v.equals(v2)){
//      val newv = v match{
//        case "row" => "col"
//        case "col" => "row"
//        case _ => "error"
//      }
//      Map(("part"-> newv), ("equls" -> "existing"))
//    } else{
//      Map(("part"-> v))
//    }
//
//
//  } else if(left.metadata.contains("part") && !right.metadata.contains("part")){
//    val newV = left.metadata.get("part").get match {
//      case "row" => "col"
//      case "col" => "row"
//      case _ => "error"
//    }
//    Map(("part"-> newV), ("left"-> "existing"))
//  } else if(!left.metadata.contains("part") && right.metadata.contains("part")) {
//    val newV = right.metadata.get("part").get match {
//      case "row" => "col"
//      case "col" => "row"
//      case _ => "error"
//    }
//    Map(("part"-> newV), ("right"-> "existing"))
//  } else{
//    Map(("part"-> "row"), ("notboth" ->"none"))
//  }

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


    val sc = left.sqlContext.sparkSession.sparkContext
//        sc.getAll.foreach(println)

    require(sc.getConf.contains("spark.executor.cores"), s"There are not configuration for spark.executor.cores ")

    val coreExecutor = sc.getConf.get("spark.executor.cores").toInt
    val coreTask = sc.getConf.contains("spark.task.cpus") match {
      case true =>
        sc.getConf.get("spark.task.cpus").toInt
      case false =>
        1
    }

    require(sc.getConf.contains("spark.executor.memory"), s"There are not configuration for spark.executor.memory ")
    val memoryExecutor = sc.getConf.get("spark.executor.memory").replace("g", "").toDouble

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
    println(s"Memory per task: ${memoryExecutor/nodeParallelism}")

    val leftSparsity = 1
    val rightSparsity = 1


//    val leftSize = leftRowBlkNum * leftColBlkNum * sparsity1 * ((blkSize * blkSize * 8) / (1024 * 1024 * 1024 * 1.0))
    val leftSize = (leftSparsity * leftRowNum * leftColNum * 8) / ((1024 * 1024 * 1024 * 1.0))
    val rightSize = (rightSparsity *  rightRowNum * rightColNum * 8) / ((1024 * 1024 * 1024 * 1.0))
    val resultSize = (1 * leftRowNum * rightColNum * 8) / (1024 * 1024 * 1024 * 1.0)


    println(s"Matrix A:$leftSize, B: $rightSize, C: $resultSize")

    val clusterMemory = 600

    val Candidate = for {
      p <- (1 to leftRowBlkNum)
      q <- (1 to rightColBlkNum)
      r <- (1 to rightRowBlkNum)
      if((leftRowBlkNum % p == 0) && (rightColBlkNum % q == 0) && (rightRowBlkNum % r == 0))
    } yield (p, q, r)

//    val prunCandidate = Candidate.filter{ case (p, q, r) => (leftSize/(p * r) + rightSize/(q * r) + resultSize/(p * q)) < 1.9}
    val prunCandidate = Candidate.filter{ case (p, q, r) => (leftSize/(p * r) + rightSize/(q * r)+ resultSize/(p * q)) < 5.0}

    val costs = prunCandidate.map{ case (p, q, r) =>
      ((p, q, r), q * leftSize + p * rightSize + r * resultSize)
    }
      .filter{ case ((p, q, r), cost) =>
      p * q * r >= TaskParallelism
    }


    val sortedCosts = costs.sortBy(x => x._2)
//    for(i <- 0 until 10){
//      println(s"${sortedCosts(i)._1}, cost:${sortedCosts(i)._2} ")
//    }

    val argcost = costs.min(new Ordering[((Int, Int, Int), Double)]{
      override def compare(x: ((Int, Int, Int), Double), y: ((Int, Int, Int), Double)): Int = {
        if(x._2 == y._2){
          x._1._3 compare y._1._3
        }else {
          x._2 compare y._2
        }
      }
    })


//
//    println(s"metadata size: ${metadata.size}")
//    metadata.foreach(println)

    val master = sc.master.replace("spark://", "").split(":")(0)
    val slaves = sc.statusTracker.getExecutorInfos.filter(_.host() != master).map{a =>
      a.host()}.sorted

//    println(s"${slaves.foreach(println)}, ${slaves.length}")

    val matA = left.execute()
    val matB = right.execute()

//    val p = 1
//    val q = 100
//    val r = 1

    val (p, q, r) = argcost._1

    println(s"p: $p, q: $q, r: $r, cost:${argcost._2}GB")

//    1, 1, master, slaves
//            if(leftColBlkNum == 1 && rightRowBlkNum == 1){
//
//          if(leftRowBlkNum <= rightColBlkNum){
//            MeExecutionHelper.multiplyOuterProductDuplicationLeft(60, left.execute(), right.execute(), rightColBlkNum)
//          } else{
//            MeExecutionHelper.multiplyOuterProductDuplicationRight(60, left.execute(), right.execute(), leftRowBlkNum)
//          }
//        } else {
//          MeExecutionHelper.matrixMultiplyGeneral(left.execute(), right.execute(), bc)
//        }


    val MM = "cpmm"
    val device ="gpu"

    if(MM == "cube") {

      if(device == "gpu")
      /* cubeMM*/
        MeMMExecutionHelper.CubeMMStreamGPUTest(p, q, r, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize, master, slaves, sc)
      else{
        val prunCandidate2 = Candidate.filter{ case (p, q, r) => (leftSize/(p * r) + rightSize/(q * r)+ resultSize/(p * q)) < 3.0}

        val costs2 = prunCandidate.map{ case (p, q, r) =>
          ((p, q, r), q * leftSize + p * rightSize + r * resultSize)
        }
          .filter{ case ((p, q, r), cost) =>
            p * q * r >= 500
          }

        val sortedCosts = costs2.map(x => ((x._1, x._1._1*x._1._2*x._1._3), x._2)).sortBy(x => (x._2, -x._1._2))
        for(i <- 0 until 3){
          println(s"${sortedCosts(i)._1}, cost:${sortedCosts(i)._2} ")
        }

        val argcost2 = costs2.min(new Ordering[((Int, Int, Int), Double)]{
          override def compare(x: ((Int, Int, Int), Double), y: ((Int, Int, Int), Double)): Int = {
              if (x._2 == y._2) {
                x._1._3 compare y._1._3
              } else {
                x._2 compare y._2
              }
            }
        })

        val (cpu_p, cpu_q, cpu_r) = argcost2._1
        println(s"p: $cpu_p, q: $cpu_q, r: $cpu_r, cost:${argcost2._2}GB")
        MeMMExecutionHelper.CubeMM(cpu_p, cpu_q, cpu_r, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, master, slaves, sc)
      }
    }
    /* BroadcastMM */
    else if(MM == "bmm") {
      println("bmm")
//      val numPart = TaskParallelism

      MeMMExecutionHelper.CubeMMStreamGPUTest(1, leftColBlkNum, 1, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize, master, slaves, sc)
    }
    /* CPMM */
    else if(MM == "cpmm") {
      println("cpmm")

        MeMMExecutionHelper.CubeMMStreamGPUTest(1, 1, leftColBlkNum , matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize, master, slaves, sc)


    }else{
      println("rmm")
      val hdfsBlockSize = 0.256
//      val numPart = Math.max(Math.ceil(rightColBlkNum * leftSparsity+leftRowBlkNum * rightSparsity/0.256),1).toInt
      val numPart = leftRowBlkNum * leftColBlkNum * rightColBlkNum
      if(device == "gpu")
        MeMMExecutionHelper.rmmWithoutPartitionGPU(p,q,left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum,blkSize,   numPart)
      else
        MeMMExecutionHelper.rmmWithoutPartition(p,q,left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, numPart,  sc)
//
    }
    //    MeMMExecutionHelper.rmmDuplicationRight(p*q*r, matA, matB, leftRowBlkNum, rightColBlkNum)
//    MeMMExecutionHelper.cpmm(120, matA, matB,leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, new RowPartitioner(120, leftRowBlkNum))
//      MeMMExecutionHelper.rmmWithoutPartition(left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, 1)
//    MeMMExecutionHelper.rmmWithoutPartitionGPU(left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize)
//    MeMMExecutionHelper.sparseGPU(left.execute(), right.execute(), leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize)
//    MeMMExecutionHelper.CubeMMGPU(p, q, r, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize, master, slaves, sc)
//    MeMMExecutionHelper.CubeMMStreamGPU(1, 1, 1, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize, master, slaves, sc)
//    MeMMExecutionHelper.CubeMMStreamGPUTest(1, 1, 1, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize, master, slaves, sc)
//    MeMMExecutionHelper.CubeMMStreamGPUTest(p, q, r, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, blkSize, master, slaves, sc)
//    MeMMExecutionHelper.CubeMM(p, q, r, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum, master, slaves, sc)
//    MeMMExecutionHelper.redundancyCoGroupMM(p,q, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum,master,slaves,sc)
//    MeMMExecutionHelper.redundancyInnerMM(p,q, matA, matB, leftRowBlkNum, leftColBlkNum, rightRowBlkNum, rightColBlkNum)
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