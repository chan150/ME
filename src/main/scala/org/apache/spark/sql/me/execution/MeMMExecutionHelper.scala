package org.apache.spark.sql.me.execution

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.me.serializer.{DMatrixSerializer, MeSerializer}
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

    println("CubeMM")

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
////
//      println(s"key: $key, leftBlocks: ${leftBlocks.toMap.keys}")
//      println(s"key: $key, rightBlocks: ${rightBlocks.toMap.keys}")

//      var count = 0
//      var teststring =s"key: $key"
      res.map{ case (row, col) =>
        leftBlocks.filter(row == _._1._1).map{ case a =>
          rightBlocks.filter(col == _._1._2).filter(a._1._2 == _._1._1).map{ case b =>
//            teststring = teststring + s"a, b: {${a._1}, ${b._1}}\n"
//            println(s"key: $key, a: ${a._1}, b: ${b._1}")
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                DMatrixSerializer.deserialize(a._2),
                DMatrixSerializer.deserialize(b._2)
              ))
//              count = 1 + count
            }else {
              tmp.put((row, col), Block.incrementalMultiply(DMatrixSerializer.deserialize(a._2),DMatrixSerializer.deserialize(b._2), tmp.get((row, col)).get))
//              count = 1 + count
            }
          }
        }
      }
//      println(s"cnt: $count" + s" $teststring")
//      println(s"cnt: $count" + s" $teststring" +s" leftBlocks: ${leftBlocks.toMap.keys}" +s" rightBlocks: ${rightBlocks.toMap.keys}")
//      println(s"partition id: ${CubePart.getPartition(key)}, key: $key, temp: ${tmp.keys}")
      tmp.iterator
    }, true)

//    println(newBlocks.partitioner)


    if(k == 1) {
      val resultPart = new GridPartitioner(p, q, leftRowBlkNum, rightColBlkNum)

      new CubeToGridRDD[((Int, Int), DistributedMatrix)](sc, newBlocks, p, q, k, resultPart, master, slaves)
        .reduceByKey(resultPart, (a, b) => Block.add(a, b)).map { row =>
        val rid = row._1._1
        val cid = row._1._2

//        println(s"In reduce, $rid, $cid")
        val resultPart = new GridPartitioner(p, q, leftRowBlkNum, rightColBlkNum)

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
      else{

      val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

//      newBlocks.cartesian()
//      newBlocks.count()

      new CubeToGridRDD[((Int, Int), DistributedMatrix)](sc, newBlocks,p,q,k,resultPart,master,slaves)
        .reduceByKey(resultPart, (a, b) => Block.add(a, b)).map{ row =>
        val rid = row._1._1
        val cid = row._1._2

        //        println(s"In reduce, $rid, $cid")
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

  def sparseGPU(left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int, blksize: Int): RDD[InternalRow] ={
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



      val handle = new cusparseHandle
      val descra = new cusparseMatDescr

      JCusparse.setExceptionsEnabled(true)
      JCuda.setExceptionsEnabled(true)

      JCusparse.cusparseCreate(handle)
      JCusparse.cusparseCreateMatDescr(descra)
      JCusparse.cusparseSetMatType(descra, cusparseMatrixType.CUSPARSE_MATRIX_TYPE_GENERAL)
      JCusparse.cusparseSetMatIndexBase(descra, cusparseIndexBase.CUSPARSE_INDEX_BASE_ZERO)

      val sparse = DMatrixSerializer.deserialize(a).asInstanceOf[SparseMatrix]

      val (rowPtr, colIdx, values) = (sparse.colPtrs, sparse.rowIndices, sparse.values)
      val nnz = values.length

      val csrRowPtrA = new Pointer()
      val csrColIndA = new Pointer()
      val csrValA = new Pointer()

      JCuda.cudaMalloc(csrRowPtrA, rowPtr.length * Sizeof.INT)
      JCuda.cudaMalloc(csrColIndA, colIdx.length * Sizeof.INT)
      JCuda.cudaMalloc(csrValA, nnz * Sizeof.DOUBLE)

      JCuda.cudaMemcpy(csrRowPtrA, Pointer.to(rowPtr), rowPtr.length * Sizeof.INT, cudaMemcpyKind.cudaMemcpyHostToDevice)
      JCuda.cudaMemcpy(csrColIndA, Pointer.to(colIdx), colIdx.length * Sizeof.INT, cudaMemcpyKind.cudaMemcpyHostToDevice)
      JCuda.cudaMemcpy(csrValA, Pointer.to(values), values.length * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice)



      var numStream = 1


      val colByStream = new Array[Int](numStream)
      val GPUstream = new Array[cudaStream_t](numStream)

      val resultC = new Array[Pointer](numStream)

      (0 until numStream).map{i =>
        GPUstream(i) = new cudaStream_t
        JCuda.cudaStreamCreate(GPUstream(i))
        resultC(i) = new Pointer()
        val cudaStat = JCuda.cudaMalloc(resultC(i), blksize*blksize*Sizeof.DOUBLE)
        require(cudaStat == jcuda.runtime.cudaError.cudaSuccess, s"GPU memory allocation failed")

      }

      (0 until numStream).map { i =>
        JCuda.cudaMemset(resultC(i), 0, blksize * blksize * Sizeof.DOUBLE)
      }



      val B = DMatrixSerializer.deserialize(b).toArray

      val d_B = new Pointer()

      JCuda.cudaMalloc(d_B, blksize * blksize * Sizeof.DOUBLE)

      //      JCusparse.cusparseSetStream(handle, stream)



      val d_C = new Pointer()
      JCublas.cublasAlloc(blksize*blksize, Sizeof.DOUBLE, d_C)
      JCuda.cudaMemset(d_C, 0, blksize * blksize * Sizeof.DOUBLE)





      for(i <- 0 to 0) {
//        for(j <- 0 until 4){
//          JCublas.cublasSetVectorAsync(blksize*blksize, Sizeof.DOUBLE, Pointer.to(B), 1, d_B, 1, GPUstream(0))
//        }

        (0 until numStream).map { case i =>
          JCusparse.cusparseSetStream(handle, GPUstream(i))
          //        JCuda.cudaMemcpyAsync(d_B, Pointer.to(B), blksize * blksize * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice, stream)

          JCuda.cudaMemcpyAsync(d_B, Pointer.to(B), blksize * blksize * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice,GPUstream(i))
//          JCublas.cublasSetVectorAsync(n2, Sizeof.DOUBLE, Pointer.to(B), 1, d_B, 1, GPUstream(i))
          JCusparse.cusparseDcsrmm(handle,
            cusparseOperation.CUSPARSE_OPERATION_NON_TRANSPOSE,
            blksize, blksize, blksize, nnz,
            Pointer.to(Array[Double](1.0)), descra,
            csrValA, csrRowPtrA, csrColIndA,
            d_B, blksize,
            Pointer.to(Array[Double](1.0)),
            d_C, blksize)


        }

      }


      val resultBlock = DenseMatrix.zeros(blksize, blksize).values

      (0 until numStream).map{ case i =>
        JCuda.cudaMemcpy(Pointer.to(resultBlock), d_C, blksize * blksize * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyDeviceToHost)

        // Memory clean up

        JCuda.cudaFree(resultC(i))


      }
      JCuda.cudaFree(d_B)
//      JCuda.cudaFree(d_C)


      JCuda.cudaFree(csrColIndA)
      JCuda.cudaFree(csrRowPtrA)
      JCuda.cudaFree(csrValA)

      JCusparse.cusparseDestroyMatDescr(descra)
      JCusparse.cusparseDestroy(handle)


//      val d_B = new Array[Pointer](numStream)
//
//
//      (0 until numStream).map{i =>
//        d_B(i) = new Pointer()
//      }









      ((i, j),  DistributedMatrix.dense(blksize, blksize, resultBlock))
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

  def rmmWithoutPartitionGPU(left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int, blksize: Int): RDD[InternalRow] ={
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


      val A = DMatrixSerializer.deserialize(a).toArray
      val B = DMatrixSerializer.deserialize(b).toArray

      val N = blksize
      val n2 = N * N
      val d_A = new Pointer()

      val alpha = 1.0f
      val beta = 1.0f

      val Cublas = new jcublas.cublasHandle

      JCublas.cublasInit()

      val numStream = 2


      val colByStream = new Array[Int](numStream)
      val GPUstream = new Array[cudaStream_t](numStream)
      val d_B = new Array[Pointer](numStream)
      val resultC = new Array[Pointer](numStream)

      (0 until numStream).map{i =>
        GPUstream(i) = new cudaStream_t
        JCuda.cudaStreamCreate(GPUstream(i))
        resultC(i) = new Pointer()
        d_B(i) = new Pointer()
        JCuda.cudaMalloc(resultC(i), blksize*blksize*Sizeof.DOUBLE)
        JCuda.cudaMalloc(d_B(i), blksize * blksize * Sizeof.DOUBLE)
      }

      for (i <- (0 until numStream)) {
          JCuda.cudaMemset(resultC(i), 0, blksize * blksize * Sizeof.DOUBLE)
      }

      JCuda.cudaMalloc(d_A, blksize * blksize * Sizeof.DOUBLE)
      JCublas.cublasSetVector(n2, Sizeof.DOUBLE, Pointer.to(A), 1, d_A, 1)

      for(q <- 0 to 0) {

        for(j <- 0 until 4){
          if(q != 0)
            JCublas.cublasSetVectorAsync(n2, Sizeof.DOUBLE, Pointer.to(B), 1, d_A, 1, GPUstream(0))
        }

        for (i <- (0 until numStream)) {
          JCublas.cublasSetKernelStream(GPUstream(i))
          JCublas.cublasSetVectorAsync(n2, Sizeof.DOUBLE, Pointer.to(B), 1, d_B(i), 1, GPUstream(i))
          for(z <- 0 until 3) {
            JCublas.cublasDgemm('n', 'n', blksize, blksize, blksize, alpha, d_A, blksize, d_B(i), N, beta, resultC(i), blksize)
          }

        }

      }
      val resultBlock = Array.ofDim[Double](numStream, blksize * blksize)
      for (i <- (0 until numStream)) {
        JCublas.cublasGetVectorAsync(n2, Sizeof.DOUBLE, resultC(i), 1, Pointer.to(resultBlock(i)), 1, GPUstream(i))
        JCublas.cublasGetVectorAsync(n2, Sizeof.DOUBLE, resultC(i), 1, Pointer.to(resultBlock(i)), 1, GPUstream(i))
      }

      for (i <- (0 until numStream)) {
        JCublas.cublasFree(resultC(i))
        JCublas.cublasFree(d_B(i))
      }

      JCublas.cublasFree(d_A)
      JCublas.cublasShutdown()


      ((i, j),  DistributedMatrix.dense(blksize, blksize, resultBlock(0)))
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
  def CubeMMGPU(p:Int, q:Int, k:Int,
               left: RDD[InternalRow], right: RDD[InternalRow],
               leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int,
               blksize:Int,
               master:String, slaves:Array[String],
               sc: SparkContext): RDD[InternalRow] = {

    println("CubeMMGPU")

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
      }
      .mapPartitions( { case a =>
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

  //        var count = 0
          val d_C = new Pointer()
          var cudaStat = JCuda.cudaMalloc(d_C, blksize*blksize*Sizeof.DOUBLE)
          require(cudaStat == jcuda.runtime.cudaError.cudaSuccess, s"GPU memory allocation failed")


          leftBlocks.filter(row == _._1._1).map{ case a =>
            rightBlocks.filter(col == _._1._2).filter(a._1._2 == _._1._1).foreach{ case b =>
              //            println(s"key: $key, a: ${a._1}, b: ${b._1}")
              val A = DMatrixSerializer.deserialize(a._2).toArray
              val B = DMatrixSerializer.deserialize(b._2).toArray
              val d_A = new Pointer()
              val d_B = new Pointer()
              JCuda.cudaMalloc(d_A, blksize * blksize * Sizeof.DOUBLE)
              JCuda.cudaMalloc(d_B, blksize * blksize * Sizeof.DOUBLE)


              var stat = JCublas2.cublasSetMatrix(blksize, blksize, Sizeof.DOUBLE,Pointer.to(A),blksize, d_A, blksize)

              if(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS){
                JCuda.cudaFree(d_A)
                JCublas2.cublasDestroy(Cublas)
                throw new SparkException(s"data download failed")
              }
              stat = JCublas2.cublasSetMatrix(blksize, blksize, Sizeof.DOUBLE,Pointer.to(B),blksize, d_B, blksize)
              if(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS){
                JCuda.cudaFree(d_B)
                JCublas2.cublasDestroy(Cublas)
                throw new SparkException(s"data download failed")
              }

              val C = new Pointer()


              val cudaStat = JCuda.cudaMalloc(C, blksize*blksize*Sizeof.DOUBLE)
              JCuda.cudaMemset(C, 0, blksize * blksize * Sizeof.DOUBLE)

              stat= JCublas2.cublasDgemm(Cublas,
                jcublas.cublasOperation.CUBLAS_OP_N, jcublas.cublasOperation.CUBLAS_OP_N,
                blksize, blksize, blksize,
                Pointer.to(Array[Double](1.0)), d_A, blksize, d_B, blksize, Pointer.to(Array[Double](1.0)), C, blksize)
               println( stat)
  //            count = 1+count
  //              println(s"key:$row, $col, a: ${a._1}, b: ${b._1}, #GPUcall: $count")


              val resultBlock = DenseMatrix.zeros(blksize, blksize).values

              JCuda.cudaMemcpy(Pointer.to(resultBlock), d_C, blksize* blksize * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyDeviceToHost)
              JCuda.cudaFree(d_B)
              JCuda.cudaFree(d_A)

              JCuda.cudaFree(d_C)
              tmp.put((row, col), DistributedMatrix.dense(blksize, blksize, resultBlock))

              JCublas.cublasShutdown()

            }


          }
  //        count = 0






        }


  //      JCublas2.cublasDestroy(Cublas)
  //      println(s"partition id: ${CubePart.getPartition(key)}, key: $key, temp: ${tmp.keys}")
        tmp.iterator
    }, true)

//    println(newBlocks.count())
//    println(newBlocks.partitioner)


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

//        println(s"In reduce, $rid, $cid")
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


  def CubeMMStreamGPU(p:Int, q:Int, k:Int,
                left: RDD[InternalRow], right: RDD[InternalRow],
                leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int,
                blksize:Int,
                master:String, slaves:Array[String],
                sc: SparkContext): RDD[InternalRow] = {

    println("CubeMMStreamGPU")
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
      }
      .mapPartitions( { case a =>
        val partition = a.next()
        val (key, (leftBlocks, rightBlocks)) = (partition._1, (partition._2._1, partition._2._2))
        val res = findResultCube(key, CubePart, leftRowBlkNum, rightColBlkNum, leftRowsInPartition.toInt, rightColsInPartition.toInt)


        val (rowIdx, colIdx) = findResultCubeStream(key, CubePart, leftRowBlkNum, rightColBlkNum, leftRowsInPartition.toInt, rightColsInPartition.toInt)
//        colIdx.map(a => println(s"col key: $key, Idx: $a"))
//        rowIdx.map(a => println(s"row key: $key, Idx: $a"))
//        println(s"key:${key} \n rowIdx: ${rowIdx.toString()} \n colIdx: ${colIdx.toString()}")

        var numStream = 4


        val colByStream = new Array[Int](numStream)
        val GPUstream = new Array[cudaStream_t](numStream)



        val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()


        val Cublas = new jcublas.cublasHandle

        JCublas.cublasInit()
        val Cusparse = new cusparseHandle
        val descra = new cusparseMatDescr

        JCusparse.setExceptionsEnabled(true)
        JCuda.setExceptionsEnabled(true)

        JCusparse.cusparseCreate(Cusparse)
        JCusparse.cusparseCreateMatDescr(descra)
        JCusparse.cusparseSetMatType(descra, cusparseMatrixType.CUSPARSE_MATRIX_TYPE_GENERAL)
        JCusparse.cusparseSetMatIndexBase(descra, cusparseIndexBase.CUSPARSE_INDEX_BASE_ZERO)


        val resultC = new Array[Pointer](numStream)

        (0 until numStream).map{i =>
          GPUstream(i) = new cudaStream_t
          JCuda.cudaStreamCreate(GPUstream(i))
          resultC(i) = new Pointer()
          val cudaStat = JCuda.cudaMalloc(resultC(i), blksize*blksize*Sizeof.DOUBLE)
          require(cudaStat == jcuda.runtime.cudaError.cudaSuccess, s"GPU memory allocation failed")

        }

        rowIdx.map{ row =>
          val colIdxIter = colIdx.iterator
          while(colIdxIter.hasNext) {
            var count = 0
            (0 until numStream).map { i =>
              if (colIdxIter.hasNext) {
                JCuda.cudaMemset(resultC(i), 0, blksize * blksize * Sizeof.DOUBLE)
                colByStream(i) = colIdxIter.next()
                count = count + 1
              } else {
                JCuda.cudaFree(resultC(i))
              }
            }

            numStream = count

//            var test = 0
            leftBlocks.filter(row == _._1._1).map { a =>
//              val column = ""
//              colByStream.map(i => column + i.toString + ", ")

//              println(s"key:$key, current row: $row, in row: ${a._1}, col: ${column} numStream: $numStream")

//              test = test + 1
              CuBlock.JcuGEMMStream(a, rightBlocks, colByStream, numStream, resultC, Cublas, Cusparse, descra, GPUstream)

            }

            val resultBlock = Array.ofDim[Double](numStream, blksize * blksize)

            (0 until numStream).map { i =>

              resultBlock(i) = DenseMatrix.zeros(blksize, blksize).values

              JCuda.cudaMemcpyAsync(Pointer.to(resultBlock(i)), resultC(i), blksize * blksize * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyDeviceToHost, GPUstream(i))

              tmp.put((row, colByStream(i)), DistributedMatrix.dense(blksize, blksize, resultBlock(i)))

            }
          }

        }

        (0 until numStream).map{ i =>
          JCuda.cudaFree(resultC(i))
          JCuda.cudaStreamDestroy(GPUstream(i))
        }

        JCublas.cublasShutdown()
        JCusparse.cusparseDestroyMatDescr(descra)
        JCusparse.cusparseDestroy(Cusparse)

        tmp.iterator
      }, true)



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


      new CubeToGridRDD[((Int, Int), DistributedMatrix)](sc, newBlocks,p,q,k,resultPart,master,slaves)
        .reduceByKey(resultPart, (a, b) => Block.add(a, b)).map{ row =>
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
  }

  private abstract class MatrixPointer{
    val isTransposed: Boolean = false
    val isUploaded: Boolean = false
  }
  private class DenseMatrixPointer (val values:Pointer,
                                    override val isTransposed: Boolean,
                                    override val isUploaded: Boolean) extends MatrixPointer {

    def this(values: Pointer) = this(values, false, false)
    def this(values: Pointer, isTransposed:Boolean) = this(values, isTransposed, false)
  }

  private class SparseMatrixPointer (val colPtrs: Pointer,
                                     val rowIndices: Pointer,
                                     val values: Pointer,
                                     override val isTransposed: Boolean,
                                     override val isUploaded: Boolean) extends MatrixPointer{
    def this(colPtrs:Pointer, rowIndices:Pointer, values: Pointer) = this(colPtrs, rowIndices, values, false, false)
    def this(colPtrs:Pointer, rowIndices:Pointer, values: Pointer, isTransposed: Boolean) = this(colPtrs, rowIndices, values, isTransposed, false)
  }

  def selectExecutionGPU(dimI: Int, dimJ:Int, dimK:Int,
                         mem:Double, leftSparsity:Double, rightSparsity:Double, Streams: Int): (Int, Int, Int)={

    val leftSize = (leftSparsity * dimI * dimK * 8) / ((1024 * 1024 * 1024 * 1.0))
    val rightSize = (rightSparsity *  dimK * dimJ * 8) / ((1024 * 1024 * 1024 * 1.0))
    val resultSize = (1 * dimI * dimJ * 8) / (1024 * 1024 * 1024 * 1.0)


    println(s"Matrix A:$leftSize, B: $rightSize, C: $resultSize")

//    val clusterMemory = 600


    val Candidate = for {
      p <- (1 to dimI)
      q <- (1 to dimJ)
      r <- (1 to dimK)
      if((dimI % p == 0) && (dimJ % q == 0) && (dimK % r == 0))
    } yield (p, q, r)

    val prunCandidate = Candidate.filter{ case (p, q, r) => (leftSize/(p * r) + rightSize/(q * r)) < mem}

    val costs = prunCandidate.map{ case (p, q, r) =>
      ((p, q, r), q * leftSize + p * rightSize + r * resultSize)
    }.filter{ case ((p, q, r), cost) =>
      Streams >= Math.min(dimI/p, dimJ/q)
    }

    val argcost = costs.min(new Ordering[((Int, Int, Int), Double)]{
      override def compare(x: ((Int, Int, Int), Double), y: ((Int, Int, Int), Double)): Int = {
        if(x._2 == y._2){
          x._1._3 compare y._1._3
        }else {
          x._2 compare y._2
        }
      }
    })


    argcost._1
  }

  def CubeMMStreamGPUTest(p:Int, q:Int, k:Int,
                      left: RDD[InternalRow], right: RDD[InternalRow],
                      leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int,
                      blksize:Int,
                      master:String, slaves:Array[String],
                      sc: SparkContext): RDD[InternalRow] = {

    println("CubeMMStreamGPUw/olocality")
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


    val newBlocks = leftRDD.cogroup(rightRDD, CubePart)
      .mapPartitions{ case a =>
        val partition = a.next()
        val (key, (leftBlocks, rightBlocks)) = (partition._1, (partition._2._1, partition._2._2))


        val blocksOfA = new scala.collection.mutable.HashMap[(Int,Int), InternalRow]()
        leftBlocks.map(b => blocksOfA.put(b._1, b._2))

        val blocksOfB = new scala.collection.mutable.HashMap[(Int,Int), InternalRow]()
        rightBlocks.map(b => blocksOfB.put(b._1, b._2))

        val pt_list = (Math.ceil(key._1*leftRowsInPartition).toInt until Math.floor((key._1+1) * leftRowsInPartition).toInt)
        val qt_list = (Math.ceil(key._2*rightColsInPartition).toInt until Math.floor((key._2+1) * rightColsInPartition).toInt)
        val rt_list = (Math.ceil(key._3*leftColsInPartition).toInt until Math.floor((key._3+1) * leftColsInPartition).toInt)
        val (pSize, qSize, rSize) = (pt_list.size, qt_list.size, rt_list.size)

        val (rowIdx, colIdx) = (pt_list.toList, qt_list.toList)


        val GPUmem = 2.0
        val sparsityA = 1.0
        val sparsityB = 1.0
        val numStreams= 2

        val (pt, qt, rt) = selectExecutionGPU(pSize, qSize, rSize, GPUmem, sparsityA, sparsityB, numStreams)

        val PartitionInTask = new scala.collection.mutable.HashMap[(Int,Int,Int), collection.mutable.HashSet[(Int, Int, Int)]]()


        for(p<- 0 until pt; q<- 0 until qt; r <- 0 until rt) {
          PartitionInTask.put((p,q,r), (new collection.mutable.HashSet[(Int, Int, Int)]()))
        }


        val leftRowsInSubcube = if(pSize < pt) pSize.toDouble else ((pSize * 1.0) / (pt * 1.0))
        val leftColsInSubcube = if(rSize < rt) rSize.toDouble else ((rSize * 1.0) / (rt * 1.0))
//        val rightRowsInPTask = if(rSize < rt) rSize.toDouble else ((rSize * 1.0) / (rt * 1.0))
        val rightColsInSubcube = if(qSize < qt) qSize.toDouble else ((qSize * 1.0) / (qt * 1.0))


        for(i <- (0 until pSize);j <- (0 until qSize); k <- (0 until rSize)){
          val iIdx = Math.floor(i * 1.0 / leftRowsInSubcube).toInt
          val jIdx = Math.floor(j * 1.0/ rightColsInSubcube).toInt
          val kIdx = Math.floor(k * 1.0 / leftColsInSubcube).toInt
          val key = (iIdx, jIdx, kIdx)

          PartitionInTask.put(key, PartitionInTask.get(key).get.+((pt_list(i),qt_list(j), rt_list(k))))

        }

        val leftBlocksInPart = new collection.mutable.HashMap[(Int, Int), MatrixPointer]()
        val rightBlocksInPart = new collection.mutable.HashMap[(Int, Int), MatrixPointer]()
        val resultBlocksInPart = new collection.mutable.HashMap[(Int, Int), MatrixPointer]()

        for(p <- (0 until pt); q <- (0 until qt); r <- (0 until rt)) {

          val voxels = PartitionInTask.get((p,q,r)).get


          val numJ = rightColsInSubcube.toInt
          val jIdx = voxels.map(v => v._2)


          for ((i, j, k) <- voxels) {
            if(blocksOfA.contains((i,k))){
              DMatrixSerializer.deserialize(blocksOfA.get((i,k)).get) match{
                case dm: DenseMatrix =>
                  leftBlocksInPart.put((i,k), new DenseMatrixPointer(new Pointer(), dm.isTransposed))
                case sm: SparseMatrix =>
                  leftBlocksInPart.put((i,k), new SparseMatrixPointer(new Pointer(), new Pointer(), new Pointer(), sm.isTransposed))
                case _ =>
                  new SparkException(s"the class does not apply")
              }
            }
            if(blocksOfB.contains((k,j))){
              DMatrixSerializer.deserialize(blocksOfB.get((k,j)).get) match{
                case dm: DenseMatrix =>
                  rightBlocksInPart.put((k,j), new DenseMatrixPointer(new Pointer(), dm.isTransposed))
                case sm: SparseMatrix =>
                  rightBlocksInPart.put((k,j), new SparseMatrixPointer(new Pointer(), new Pointer(), new Pointer(), sm.isTransposed))
                case _ =>
                  new SparkException(s"the class does not apply")
              }
            }
            if(r == 0)
              resultBlocksInPart.put((i, j), new DenseMatrixPointer(new Pointer()))
          }


          //copy voxels
          leftBlocksInPart.foreach{case ((i,k), pointGPU) =>
            
          }

        }

        var numStream = 1


        val colByStream = new Array[Int](numStream)
        val GPUstream = new Array[cudaStream_t](numStream)



        val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()


        val Cublas = new jcublas.cublasHandle

        JCublas.cublasInit()
        val Cusparse = new cusparseHandle
        val descra = new cusparseMatDescr

        JCusparse.setExceptionsEnabled(true)
        JCuda.setExceptionsEnabled(true)

        JCusparse.cusparseCreate(Cusparse)
        JCusparse.cusparseCreateMatDescr(descra)
        JCusparse.cusparseSetMatType(descra, cusparseMatrixType.CUSPARSE_MATRIX_TYPE_GENERAL)
        JCusparse.cusparseSetMatIndexBase(descra, cusparseIndexBase.CUSPARSE_INDEX_BASE_ZERO)


        val resultC = new Array[Pointer](numStream)

        (0 until numStream).map{i =>
          GPUstream(i) = new cudaStream_t
          JCuda.cudaStreamCreate(GPUstream(i))
          resultC(i) = new Pointer()
          val cudaStat = JCuda.cudaMalloc(resultC(i), blksize*blksize*Sizeof.DOUBLE)
          require(cudaStat == jcuda.runtime.cudaError.cudaSuccess, s"GPU memory allocation failed")

        }

        rowIdx.map{ row =>
          val colIdxIter = colIdx.iterator
          while(colIdxIter.hasNext) {
            var count = 0
            (0 until numStream).map { i =>
              if (colIdxIter.hasNext) {
                JCuda.cudaMemset(resultC(i), 0, blksize * blksize * Sizeof.DOUBLE)
                colByStream(i) = colIdxIter.next()
                count = count + 1
              } else {
                JCuda.cudaFree(resultC(i))
              }
            }

            numStream = count

            //            var test = 0
            leftBlocks.filter(row == _._1._1).map { a =>
              //              val column = ""
              //              colByStream.map(i => column + i.toString + ", ")

              //              println(s"key:$key, current row: $row, in row: ${a._1}, col: ${column} numStream: $numStream")

              //              test = test + 1
              CuBlock.JcuGEMMStream(a, rightBlocks, colByStream, numStream, resultC, Cublas, Cusparse, descra, GPUstream)

            }

            val resultBlock = Array.ofDim[Double](numStream, blksize * blksize)

            (0 until numStream).map { i =>

              resultBlock(i) = DenseMatrix.zeros(blksize, blksize).values

              JCuda.cudaMemcpyAsync(Pointer.to(resultBlock(i)), resultC(i), blksize * blksize * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyDeviceToHost, GPUstream(i))

              tmp.put((row, colByStream(i)), DistributedMatrix.dense(blksize, blksize, resultBlock(i)))

            }
          }

        }

        (0 until numStream).map{ i =>
          JCuda.cudaFree(resultC(i))
          JCuda.cudaStreamDestroy(GPUstream(i))
        }

        JCublas.cublasShutdown()
        JCusparse.cusparseDestroyMatDescr(descra)
        JCusparse.cusparseDestroy(Cusparse)

        tmp.iterator
      }



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

//      val resultPart = new GridPartitioner(p,q,leftRowBlkNum, rightColBlkNum)

      val Npart = if(p*q < leftRowBlkNum * rightColBlkNum) (leftRowBlkNum * rightColBlkNum) else p*q

      newBlocks.reduceByKey((a, b) => Block.add(a, b)).map{ row =>
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

  private def findResultCubeStream(key:(Int, Int, Int), part:CubePartitioner, rows:Int, cols:Int, rowsInPartition:Int, colsInPartition:Int):(List[Int], List[Int]) = {
    val tmp = new mutable.HashSet[(Int, Int)]()
    val p = part.p
    val q = part.q
    val k = part.k

    val pid = (part.getPartition(key) / k)

    //    println(s"key: $key, pid : ${part.getPartition(key)}, result pid: $pid")

    val colsBase = pid % q
    val rowsBase = pid / q

    ((0 to rows -1).filter(j => rowsBase == Math.floor(j*1.0/rowsInPartition*1.0).toInt).toList, (0 to cols - 1).filter(i => colsBase == Math.floor(i*1.0/colsInPartition*1.0).toInt).toList)

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

  def rmmWithoutPartition(left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, leftColBlkNum: Int, rightRowBlkNum: Int, rightColBlkNum: Int, Numpartition: Int): RDD[InternalRow] ={
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

    leftRDD.join(rightRDD, Numpartition).map{ case ((i, j, k), (a, b)) =>
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
