package org.apache.spark.sql.me.execution

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.me.serializer.DMatrixSerializer
import org.apache.spark.sql.me.execution.MeExecutionHelper.{BroadcastPartitions, repartitionWithTargetPartitioner}
import org.apache.spark.sql.me.matrix._
import org.apache.spark.sql.me.partitioner._
import org.apache.spark.{Partitioner, SparkContext, SparkException}
import jcuda._
import jcuda.jcublas._
import jcuda.jcusparse._
import jcuda.driver.CUdevice_attribute._
import jcuda.driver.JCudaDriver._
import jcuda.driver._
import jcuda.runtime._
import jcuda.driver.CUmodule
import jcuda.driver.CUdevice_attribute

import scala.collection.mutable

object MeMMExecutionHelper {

  def CubeMM(p:Int, q:Int, k:Int,
             left: RDD[InternalRow], right: RDD[InternalRow],
             leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int,
             master:String, slaves:Array[String],
             sc: SparkContext): RDD[InternalRow] = {

    val leftRowsInPartition = if(leftRowBlkNum < p) leftRowBlkNum.toDouble else ((leftRowBlkNum * 1.0) / (p * 1.0))
    val leftColsInPartition = if(leftColBlkNum < k) leftColBlkNum.toDouble else ((leftColBlkNum * 1.0) / (k * 1.0))

    val leftRDD = left.flatMap{ row =>
      val i = row.getInt(1)
      val k = row.getInt(2)
      val mat = row.getStruct(3, 7)

      (0 until q).map{ j =>
        ((Math.floor(i * 1.0 / leftRowsInPartition).toInt, j, Math.floor(k * 1.0 / leftColsInPartition).toInt ),((i, k), mat))
      }
    }

    val rightRowsInPartition = if(rightRowBlkNum < k) rightRowBlkNum.toDouble else ((rightRowBlkNum * 1.0) / (k * 1.0))
    val rightColsInPartition = if(rightColBlkNum < q) rightColBlkNum.toDouble else ((rightColBlkNum * 1.0) / (q * 1.0))

    val rightRDD = right.flatMap{ row =>
      val k = row.getInt(1)
      val j = row.getInt(2)
      val mat = row.getStruct(3, 7)

      (0 until p).map{ i =>
        ((i, Math.floor(j * 1.0/ rightColsInPartition).toInt, Math.floor(k * 1.0 / rightRowsInPartition).toInt),((k, j), mat))
      }
    }

    val CubePart = new CubePartitioner(p, q, k)


    val newBlocks = new CoLocatedMatrixRDD[(Int, Int, Int)](sc, Seq(leftRDD, rightRDD), CubePart, k, master, slaves, leftRowBlkNum, rightColBlkNum)
      .mapValues { case Array(vs, w1s) =>
        (vs.asInstanceOf[Iterable[(((Int, Int), InternalRow))]], w1s.asInstanceOf[Iterable[(((Int, Int), InternalRow))]])
      }.mapPartitions( { case a =>
      val partition = a.next()
      val (key, (leftBlocks, rightBlocks)) = (partition._1, (partition._2._1, partition._2._2))
      val res = findResultCube(key, CubePart, leftRowBlkNum, rightColBlkNum, leftRowsInPartition.toInt, rightColsInPartition.toInt)
//      println(s"key: $key, result: $res")
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
//
//      println(s"key: $key, leftBlocks: ${leftBlocks.toMap.keys}")
//      println(s"key: $key, rightBlocks: ${rightBlocks.toMap.keys}")

      res.map{ case (row, col) =>
        leftBlocks.filter(row == _._1._1).map{ case a =>
          rightBlocks.filter(col == _._1._2).filter(a._1._2 == _._1._1).map{ case b =>
//            println(s"key: $key, a: ${a._1}, b: ${b._1}")
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                DMatrixSerializer.deserialize(a._2),
                DMatrixSerializer.deserialize(b._2)
              ))
            }else {
              tmp.put((row, col), Block.incrementalMultiply(DMatrixSerializer.deserialize(a._2),DMatrixSerializer.deserialize(b._2), tmp.get((row, col)).get))
            }
          }
        }
      }
      println(s"partition id: ${CubePart.getPartition(key)}, key: $key, temp: ${tmp.keys}")
      tmp.iterator
    }, true)

    println(newBlocks.partitioner)


    if(k == 1){
      newBlocks.map{ row =>
        val rid = row._1._1
        val cid = row._1._2

        val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

        val pid = resultPart.getPartition((rid, cid))
        val mat = row._2
        val res = new GenericInternalRow(4)
        res.setInt(0, pid)
        res.setInt(1, rid)
        res.setInt(2, cid)
        res.update(3, DMatrixSerializer.serialize(mat))
        res
      }
    } else{

      val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

//      newBlocks.cartesian()
//      newBlocks.count()

     val test =  new CubeToGridRDD[((Int, Int), DistributedMatrix)](sc, newBlocks,p,q,k,resultPart,master,slaves)
        .reduceByKey(resultPart, (a, b) => Block.add(a, b))

      println(test.partitioner)
//      newBlocks.reduceByKey((a,b) => Block.add(a, b))
      test.map{ row =>
        val rid = row._1._1
        val cid = row._1._2

        println(s"In reduce, $rid, $cid")
        val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

        val pid = resultPart.getPartition((rid, cid))
        val mat = row._2
        val res = new GenericInternalRow(4)
        res.setInt(0, pid)
        res.setInt(1, rid)
        res.setInt(2, cid)
        res.update(3, DMatrixSerializer.serialize(mat))
        res
      }
    }
  }

  def CubeMMGPU(p:Int, q:Int, k:Int,
               left: RDD[InternalRow], right: RDD[InternalRow],
               leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int,
               blksize:Int,
               master:String, slaves:Array[String],
               sc: SparkContext): RDD[InternalRow] = {

    val leftRowsInPartition = if(leftRowBlkNum < p) leftRowBlkNum.toDouble else ((leftRowBlkNum * 1.0) / (p * 1.0))
    val leftColsInPartition = if(leftColBlkNum < k) leftColBlkNum.toDouble else ((leftColBlkNum * 1.0) / (k * 1.0))

    val leftRDD = left.flatMap{ row =>
      val i = row.getInt(1)
      val k = row.getInt(2)
      val mat = row.getStruct(3, 7)

      (0 until q).map{ j =>
        ((Math.floor(i * 1.0 / leftRowsInPartition).toInt, j, Math.floor(k * 1.0 / leftColsInPartition).toInt ),((i, k), mat))
      }
    }

    val rightRowsInPartition = if(rightRowBlkNum < k) rightRowBlkNum.toDouble else ((rightRowBlkNum * 1.0) / (k * 1.0))
    val rightColsInPartition = if(rightColBlkNum < q) rightColBlkNum.toDouble else ((rightColBlkNum * 1.0) / (q * 1.0))

    val rightRDD = right.flatMap{ row =>
      val k = row.getInt(1)
      val j = row.getInt(2)
      val mat = row.getStruct(3, 7)

      (0 until p).map{ i =>
        ((i, Math.floor(j * 1.0/ rightColsInPartition).toInt, Math.floor(k * 1.0 / rightRowsInPartition).toInt),((k, j), mat))
      }
    }

    val CubePart = new CubePartitioner(p, q, k)


    val newBlocks = new CoLocatedMatrixRDD[(Int, Int, Int)](sc, Seq(leftRDD, rightRDD), CubePart, k, master, slaves, leftRowBlkNum, rightColBlkNum)
      .mapValues { case Array(vs, w1s) =>
        (vs.asInstanceOf[Iterable[(((Int, Int), InternalRow))]], w1s.asInstanceOf[Iterable[(((Int, Int), InternalRow))]])
      }.mapPartitions( { case a =>
      val partition = a.next()
      val (key, (leftBlocks, rightBlocks)) = (partition._1, (partition._2._1, partition._2._2))
      val res = findResultCube(key, CubePart, leftRowBlkNum, rightColBlkNum, leftRowsInPartition.toInt, rightColsInPartition.toInt)
      //      println(s"key: $key, result: $res")
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()

      //      println(s"key: $key, leftBlocks: ${leftBlocks.toMap.keys}")
      //      println(s"key: $key, rightBlocks: ${rightBlocks.toMap.keys}")

      res.map{ case (row, col) =>
        val Cublas = new jcublas.cublasHandle

        //      var stat = jcublas.JCublas2.cublasCreate(Cublas)
        //      require(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS, s"CUBLAS initialization failed")

        JCublas.cublasInit()
        val Cusparse = new cusparseHandle
        val descra = new cusparseMatDescr

        JCusparse.setExceptionsEnabled(true)
        JCuda.setExceptionsEnabled(true)
//        var count = 0
        val d_C = new Pointer()
        var cudaStat = JCuda.cudaMalloc(d_C, blksize*blksize*Sizeof.DOUBLE)
        require(cudaStat == jcuda.runtime.cudaError.cudaSuccess, s"GPU memory allocation failed")

        JCusparse.cusparseCreate(Cusparse)
        JCusparse.cusparseCreateMatDescr(descra)
        JCusparse.cusparseSetMatType(descra, cusparseMatrixType.CUSPARSE_MATRIX_TYPE_GENERAL)
        JCusparse.cusparseSetMatIndexBase(descra, cusparseIndexBase.CUSPARSE_INDEX_BASE_ZERO)

        leftBlocks.filter(row == _._1._1).map{ case a =>
          rightBlocks.filter(col == _._1._2).filter(a._1._2 == _._1._1).foreach{ case b =>
            //            println(s"key: $key, a: ${a._1}, b: ${b._1}")
            CuBlock.JcuGEMM(DMatrixSerializer.deserialize(a._2), DMatrixSerializer.deserialize(b._2), d_C, Cublas, Cusparse, descra)
//            count = 1+count
//              println(s"key:$row, $col, a: ${a._1}, b: ${b._1}, #GPUcall: $count")
          }
        }
//        count = 0

        val resultBlock = DistributedMatrix.zeros(blksize, blksize).toArray

        JCuda.cudaMemcpy(Pointer.to(resultBlock), d_C, blksize* blksize * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyDeviceToHost)

        JCuda.cudaFree(d_C)
        tmp.put((row, col), DistributedMatrix.dense(blksize, blksize, resultBlock))

        JCublas.cublasShutdown()
        JCusparse.cusparseDestroyMatDescr(descra)
        JCusparse.cusparseDestroy(Cusparse)

      }


//      JCublas2.cublasDestroy(Cublas)
      println(s"partition id: ${CubePart.getPartition(key)}, key: $key, temp: ${tmp.keys}")
      tmp.iterator
    }, true)

    println(newBlocks.partitioner)


    if(k == 1){
      newBlocks.map{ row =>
        val rid = row._1._1
        val cid = row._1._2

        val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

        val pid = resultPart.getPartition((rid, cid))
        val mat = row._2
        val res = new GenericInternalRow(4)
        res.setInt(0, pid)
        res.setInt(1, rid)
        res.setInt(2, cid)
        res.update(3, DMatrixSerializer.serialize(mat))
        res
      }
    } else{

      val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

      //      newBlocks.cartesian()
      //      newBlocks.count()

      new CubeToGridRDD[((Int, Int), DistributedMatrix)](sc, newBlocks,p,q,k,resultPart,master,slaves)
        .reduceByKey(resultPart, (a, b) => Block.add(a, b)).map{ row =>
        val rid = row._1._1
        val cid = row._1._2

        println(s"In reduce, $rid, $cid")
        val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

        val pid = resultPart.getPartition((rid, cid))
        val mat = row._2
        val res = new GenericInternalRow(4)
        res.setInt(0, pid)
        res.setInt(1, rid)
        res.setInt(2, cid)
        res.update(3, DMatrixSerializer.serialize(mat))
        res
      }
    }
  }



  private def findResultCube(key:(Int, Int, Int), part:CubePartitioner, rows:Int, cols:Int, rowsInPartition:Int, colsInPartition:Int):scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()
    val p = part.p
    val q = part.q
    val k = part.k

    val pid = (part.getPartition(key) / k)

//    println(s"key: $key, pid : ${part.getPartition(key)}, result pid: $pid")

    val colsBase = pid % q
    val rowsBase = pid / q

    (0 to cols - 1).filter(i => colsBase == Math.floor(i*1.0/colsInPartition*1.0).toInt).flatMap{ col =>
      (0 to rows -1).filter(j => rowsBase == Math.floor(j*1.0/rowsInPartition*1.0).toInt).map( row =>
        tmp += ((row, col))
      )
    }
    tmp
  }

  def redundancyCoGroupMM(p:Int, q:Int,
                          left: RDD[InternalRow], right: RDD[InternalRow],
                          leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int,
                          master:String, slaves:Array[String],
                          sc: SparkContext): RDD[InternalRow] = {


    val rdd1 = left.flatMap{ row =>
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)

      val startingPoint = Math.floor((rid*1.0/(leftRowBlkNum*1.0/p * 1.0))).toInt * q
      (startingPoint to startingPoint + (q-1)).map{ i =>
        (i, ((rid, cid), mat))
      }
    }

    val rdd2 = right.flatMap{ row =>
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)

      val startPoint = Math.floor((cid*1.0/(rightColBlkNum*1.0/ q * 1.0)))
      (0 to (p -1)).map{i =>
        ((q*i)+startPoint.toInt, ((rid, cid), mat))
      }
    }


    new CoLocatedMatrixRDD[Int](sc, Seq(rdd1, rdd2), new IndexPartitioner(p*q, new RedunRowPartitioner(q, p)), 1, master, slaves, leftRowBlkNum, rightColBlkNum)
      .mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[((Int, Int), InternalRow)]], w1s.asInstanceOf[Iterable[((Int, Int), InternalRow)]])
      }.flatMap{ case (pid, (leftBlocks, rightBlocks)) =>

//        println(s"pid: $pid")

        val res = findResultRI(pid.toInt, p, q, leftRowBlkNum, rightColBlkNum)

        val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()

        res.map{ case (row, col) =>
          leftBlocks.filter(row == _._1._1).map{ case a =>
            rightBlocks.filter(col == _._1._2).filter(a._1._2 == _._1._1).map{ case b =>
//              println(s"key: $row, $col")
              if(!tmp.contains((row, col))){
                tmp.put((row, col), Block.matrixMultiplication(
                  DMatrixSerializer.deserialize(a._2),
                  DMatrixSerializer.deserialize(b._2)
                ))
              }else {
                tmp.put((row, col), Block.incrementalMultiply(DMatrixSerializer.deserialize(a._2),DMatrixSerializer.deserialize(b._2), tmp.get((row, col)).get))
              }
            }
          }
        }
//        println(s"temp size: ${tmp.size}")
        tmp.iterator
      }.map{ row =>
        val rid = row._1._1
        val cid = row._1._2

        val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

        val pid = resultPart.getPartition((rid, cid))
        val mat = row._2
        val res = new GenericInternalRow(4)
        res.setInt(0, pid)
        res.setInt(1, rid)
        res.setInt(2, cid)
        res.update(3, DMatrixSerializer.serialize(mat))
        res
      }
  }

  def redundancyInnerMM(p:Int, q:Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int): RDD[InternalRow] = {


//    CoLocatedMatrixRDD



    val part = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

    val leftRDD = left.flatMap{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)

      val startingPoint = Math.floor((rid*1.0/(leftRowBlkNum*1.0/p * 1.0))).toInt * q
      (startingPoint to startingPoint + (q-1)).map{ i =>
        (i, ((rid, cid), mat))
      }
    }.groupByKey(new IndexPartitioner(p*q, new RedunRowPartitioner(q, p)))

//    leftRDD.preferredLocations(leftRDD.partitions(0))



    val rightRDD = right.flatMap{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)

      val startPoint = Math.floor((cid*1.0/(rightColBlkNum*1.0/ q * 1.0)))
      (0 to (p -1)).map{i =>
        ((q*i)+startPoint.toInt, ((rid, cid), mat))
      }
    }.groupByKey(new IndexPartitioner(p*q, new RedunColPartitioner(p, q)))

//    val A = leftRDD.cogroup(rightRDD)
//    val B = leftRDD.zipPartitions(rightRDD)
//    val C = leftRDD.cartesian(rightRDD)

    leftRDD.zipPartitions(rightRDD, preservesPartitioning = true){ case (iter1, iter2) =>

      val leftBlocks = iter1.next()._2.toList
      val temp = iter2.next()

      val rightBlocks = temp._2.toList
      val pid = temp._1
      val res = findResultRI(pid, p, q, leftRowBlkNum, rightColBlkNum)
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()

      res.map{ case (row, col) =>
        leftBlocks.filter(row == _._1._1).map{ case a =>
          rightBlocks.filter(col == _._1._2).filter(a._1._2 == _._1._1).map{ case b =>
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                DMatrixSerializer.deserialize(a._2),
                DMatrixSerializer.deserialize(b._2)
              ))
            }else {
              tmp.put((row, col), Block.incrementalMultiply(DMatrixSerializer.deserialize(a._2),DMatrixSerializer.deserialize(b._2), tmp.get((row, col)).get))
            }
          }
        }
      }
      tmp.iterator
    }.map{ row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = part.getPartition((rid, cid))
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, DMatrixSerializer.serialize(mat))
      res
    }
  }

  def rmmWithoutPartition(left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int): RDD[InternalRow] ={
    val leftRDD = left.flatMap{ row =>
      val i = row.getInt(1)
      val k = row.getInt(2)
      val matrix = row.getStruct(3, 7)

      (0 to rightColBlkNum).map(j => ((i, j, k), matrix))
    }

    val rightRDD = right.flatMap{ row =>
      val k = row.getInt(1)
      val j = row.getInt(2)
      val matrix = row.getStruct(3, 7)

      (0 to leftRowBlkNum).map(i => ((i, j, k), matrix))
    }

    leftRDD.join(rightRDD).map{ case ((i, j, k), (a, b)) =>
      ((i, j), Block.matrixMultiplication(DMatrixSerializer.deserialize(a),DMatrixSerializer.deserialize(b)))
    }.reduceByKey{(a, b) => Block.add(a, b)}.map{ row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = -1
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, DMatrixSerializer.serialize(mat))
      res
    }
  }

  def cpmm(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowNum: Int, leftColNum: Int, rightRowNum: Int, rightColNum: Int, resultPart: Partitioner): RDD[InternalRow] = {
//    if(left.partitioner != None){
//      left.partitioner.get match {
//        case col:ColumnPartitioner => println("column part left")
//        case row:RowPartitioner => println("row Part left")
//        case _ => println("error")
//      }
//    } else{
//      println("none")
//    }
//
//
//
//    if(right.partitioner != None){
//      right.partitioner.get match {
//        case col:ColumnPartitioner => println("column part right")
//        case row:RowPartitioner => println("row Part right")
//        case _ => println("error")
//      }
//    } else{
//      println("none")
//    }

    val leftRDD = repartitionWithTargetPartitioner(new ColumnPartitioner(n, leftColNum), left)
//    println(leftRDD.partitioner.toString)
    val rightRDD = repartitionWithTargetPartitioner(new RowPartitioner(n, rightRowNum), right)
    val newBlocks =leftRDD.zipPartitions(rightRDD, preservesPartitioning = true){ case (iter1, iter2) =>
      val leftBlocks = iter1.toList
      val rightBlocks = iter2.toList
      val res = findResultCPMM(leftRowNum, rightColNum)
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()

      var count = 0
      res.map{ case (row, col) =>
        leftBlocks.filter(row == _._2._1._1).map{ case a =>
          rightBlocks.filter(col == _._2._1._2).filter(a._2._1._2 == _._2._1._1).map{ case b =>


            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                DMatrixSerializer.deserialize(a._2._2),
                DMatrixSerializer.deserialize(b._2._2)))
            } else {
              tmp.put((row, col), Block.incrementalMultiply(
                DMatrixSerializer.deserialize(a._2._2),
                DMatrixSerializer.deserialize(b._2._2),
                tmp.get((row, col)).get))
            }

              count = count + 1
          }
        }
      }
      tmp.iterator
    }

    resultPart match {
      case rowPart:RowPartitioner =>

        newBlocks.map{ a =>
          val part = new RowPartitioner(n, leftRowNum)
          (part.getPartition(a._1), (a._1, a._2))
        }.groupByKey(new IndexPartitioner(n, new RowPartitioner(n, leftRowNum))).flatMap{ case (pid, blk) =>
          val CPagg = findResultRMMRight(pid, n, leftRowNum, rightColNum)
          val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
          CPagg.map{ case (row, col) =>
            blk.filter((row, col) == _._1).map{ case (idx, mat) =>
              if(!tmp.contains((row, col))){
                tmp.put((row, col), mat)
              } else {
                tmp.put((row, col), Block.add(tmp.get((row, col)).get, mat))
              }
            }
          }
          tmp.iterator
        }.map{ row =>
          val part = new RowPartitioner(n, leftRowNum)
          val rid = row._1._1
          val cid = row._1._2
          val pid = part.getPartition((rid, cid))
          val mat = row._2
          val res = new GenericInternalRow(4)
          res.setInt(0, pid)
          res.setInt(1, rid)
          res.setInt(2, cid)
          res.update(3, DMatrixSerializer.serialize(mat))
          res
        }
      case colPart:ColumnPartitioner =>
        newBlocks.map{ a =>
          val part = new ColumnPartitioner(n, rightColNum)
          (part.getPartition(a._1), (a._1, a._2))
        }.groupByKey(new IndexPartitioner(n, new RowPartitioner(n, leftRowNum))).flatMap{ case (pid, blk) =>
          val CPagg = findResultRMMLeft(pid, n, leftRowNum, rightColNum)
          val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
          CPagg.map{ case (row, col) =>
            blk.filter((row, col) == _._1).map{ case (idx, mat) =>
              if(!tmp.contains((row, col))){
                tmp.put((row, col), mat)
              } else {
                tmp.put((row, col), Block.add(tmp.get((row, col)).get, mat))
              }
            }
          }
          tmp.iterator
        }.map{ row =>
          val part = new ColumnPartitioner(n, rightColNum)
          val rid = row._1._1
          val cid = row._1._2
          val pid = part.getPartition((rid, cid))
          val mat = row._2
          val res = new GenericInternalRow(4)
          res.setInt(0, pid)
          res.setInt(1, rid)
          res.setInt(2, cid)
          res.update(3, DMatrixSerializer.serialize(mat))
          res
        }
      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for $resultPart")
    }
  }

  def rmmDuplicationRight(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, rightColBlkNum: Int): RDD[InternalRow] = {
    val part = new RowPartitioner(n, leftRowBlkNum)
    val leftRDD = repartitionWithTargetPartitioner(part, left)
    val dupRDD = BroadcastPartitions(right, n)

    leftRDD.zipPartitions(dupRDD, preservesPartitioning = true) { case (iter1, iter2) =>
      val leftBlocks = iter1.toList
      val temp = iter2.next()
      val rightBlocks = temp._2.toList

      val pid = temp._1

      val res = findResultRMMRight(pid, n, leftRowBlkNum, rightColBlkNum)
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
      res.map{ case (row, col) =>
        leftBlocks.filter(row == _._2._1._1).map{ case a =>
          rightBlocks.filter(col == _._1._2).filter(a._2._1._2 == _._1._1).map{ case b =>
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                DMatrixSerializer.deserialize(a._2._2),b._2))
            } else {
              tmp.put((row, col), Block.incrementalMultiply(DMatrixSerializer.deserialize(a._2._2),b._2, tmp.get((row, col)).get))
            }
          }
        }
      }
      tmp.iterator
    }.map{ row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = part.getPartition((rid, cid))
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, DMatrixSerializer.serialize(mat))
      res
    }
  }

  def rmmDuplicationLeft(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, rightColBlkNum: Int): RDD[InternalRow] = {
    val part = new ColumnPartitioner(n, rightColBlkNum)
    val rightRDD = repartitionWithTargetPartitioner(part, right)
    val dupRDD = BroadcastPartitions(left, n)

    dupRDD.zipPartitions(rightRDD, preservesPartitioning = true) { case (iter1, iter2) =>
      val temp = iter1.next()
      val leftBlocks = temp._2.toList
      val rightBlocks = iter2.toList

      val pid = temp._1

      val res = findResultRMMLeft(pid, n, leftRowBlkNum, rightColBlkNum)
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
      res.map{ case (row, col) =>
        leftBlocks.filter(row == _._1._1).map{ case a =>
          rightBlocks.filter(col == _._2._1._2).filter(a._1._2 == _._2._1._1).map{ case b =>
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                a._2, DMatrixSerializer.deserialize(b._2._2)))
            } else {
              tmp.put((row, col), Block.incrementalMultiply( a._2, DMatrixSerializer.deserialize(b._2._2), tmp.get((row, col)).get))
            }
          }
        }
      }
      tmp.iterator
    }.map{ row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = part.getPartition((rid, cid))
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, DMatrixSerializer.serialize(mat))
      res
    }
  }

  private def findResultRMMRight(pid: Int, n: Int, rows: Int, cols: Int): scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()

    if(pid == -1){
      tmp
    } else{
      val rowsInPartition = if(rows < n) rows.toDouble else ((rows*1.0)/(n * 1.0))
      val rowsBase = pid % n
      val rowIndesList = (0 to rows -1).filter(i => rowsBase == Math.floor(i*1.0/rowsInPartition*1.0).toInt)

      rowIndesList.flatMap{ case row =>
        (0 to cols-1).map{ case col =>
          tmp += ((row, col))
        }
      }
      tmp
    }
  }

  private def findResultRMMLeft(pid: Int, n: Int, rows: Int, cols: Int): scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()

    if(pid == -1){
      tmp
    } else{

      val colsInPartition = if(cols < n) cols.toDouble else ((cols*1.0)/(n * 1.0))
      val colsBase = pid % n
//      Math.floor(j * 1.0 /(colsInPartition*1.0)).toInt
      val colIndexList = (0 to rows -1).filter(i => colsBase == Math.floor(i *1.0/ colsInPartition*1.0).toInt)

      colIndexList.flatMap{ case col =>
        (0 to rows-1).map{ case row =>
          tmp += ((row, col))
        }
      }
      tmp
    }
  }

  private def findResultCPMM(rows: Int, cols: Int): scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()

    (0 to rows-1).map(row => (0 to cols-1).map(col => tmp += ((row, col))))

    tmp
  }

  private def findResultRI(pid:Int, p:Int, q:Int, rows:Int, cols:Int):scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()
    if(pid == -1){
      tmp.empty
    } else {
      val rowsInPartition = if(rows < p) rows.toDouble else ((rows*1.0)/(p*1.0))
      val colsInPartition = if(cols < q) cols.toDouble else ((cols*1.0)/(q*1.0))

      val colsBase = pid % q
      val rowsBase = pid / q

      (0 to cols - 1).filter(i => colsBase == Math.floor(i*1.0/colsInPartition*1.0).toInt).flatMap{ col =>
        (0 to rows -1).filter(j => rowsBase == Math.floor(j*1.0/rowsInPartition*1.0).toInt).map( row =>
          tmp += ((row, col))
        )
      }
    }
    tmp
  }
}
