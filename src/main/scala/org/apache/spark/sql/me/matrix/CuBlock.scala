package org.apache.spark.sql.me.matrix

import jcuda._
import jcuda.jcublas._
import jcuda.jcusparse._
import jcuda.driver.CUdevice_attribute._
import jcuda.driver.JCudaDriver._
import jcuda.driver._
import jcuda.runtime._
import jcuda.driver.CUmodule
import jcuda.driver.CUdevice_attribute
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.me.serializer.DMatrixSerializer

object CuBlock {
  def JcuGEMM(A: DistributedMatrix, B: DistributedMatrix, C: Pointer, Cublas:cublasHandle, Cuspasrs:cusparseHandle, descra:cusparseMatDescr) : Unit ={
    require(A.numCols == B.numRows , s"dimension mismatch a.numCols = ${A.numRows}, B.numRows = ${B.numRows}")

    (A, B) match {
      case (ma: DenseMatrix, mb: DenseMatrix) => JuMultiplyDenseDense(ma, mb, C, Cublas)
      case (ma: DenseMatrix, mb: SparseMatrix) => JuMultiplyDenseDense(ma, mb.toDense, C, Cublas)
      case (ma: SparseMatrix, mb: DenseMatrix) => JuMultiplySparseDense(ma, mb, C, Cuspasrs, descra)
      case (ma: SparseMatrix, mb: SparseMatrix) => JuMultiplySparseDense(ma, mb.toDense, C, Cuspasrs, descra)
      case _ =>
        new SparkException(s"incrementalMultiply does not apply to a: ${A.getClass}, b: ${B.getClass}")
    }
  }

  private def JuMultiplyDenseDense(A: DenseMatrix, B: DenseMatrix, C: Pointer, handle:cublasHandle): Unit ={
//    val handle = new jcublas.cublasHandle

    val d_A = new Pointer()
    val d_B = new Pointer()
    JCuda.cudaMalloc(d_A, A.numRows * A.numCols * Sizeof.DOUBLE)
    JCuda.cudaMalloc(d_B, B.numRows * B.numCols * Sizeof.DOUBLE)

    var stat = JCublas2.cublasSetMatrix(A.numRows, A.numCols, Sizeof.DOUBLE,Pointer.to(A.values),A.numRows, d_A, A.numRows)

    if(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS){
      JCuda.cudaFree(d_A)
      JCublas2.cublasDestroy(handle)
      throw new SparkException(s"data download failed")
    }
    stat = JCublas2.cublasSetMatrix(B.numRows, B.numCols, Sizeof.DOUBLE,Pointer.to(B.values),B.numRows, d_B, B.numRows)
    if(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS){
      JCuda.cudaFree(d_B)
      JCublas2.cublasDestroy(handle)
      throw new SparkException(s"data download failed")
    }
    if(!A.isTransposed && !B.isTransposed) {
      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_N, jcublas.cublasOperation.CUBLAS_OP_N,
        A.numRows, B.numCols, A.numCols,
        Pointer.to(Array[Double](1.0)), d_A, A.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, A.numRows)
    }else if(!A.isTransposed && B.isTransposed)

      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_N, jcublas.cublasOperation.CUBLAS_OP_T,
        A.numRows, B.numCols, A.numCols,
        Pointer.to(Array[Double](1.0)), d_A, A.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, A.numRows)
    else if(A.isTransposed && !B.isTransposed)

      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_T, jcublas.cublasOperation.CUBLAS_OP_N,
        A.numRows, B.numCols, A.numCols,
        Pointer.to(Array[Double](1.0)), d_A, A.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, A.numRows)
    else

      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_T, jcublas.cublasOperation.CUBLAS_OP_T,
        A.numRows, B.numCols, A.numCols,
        Pointer.to(Array[Double](1.0)), d_A, A.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, A.numRows)

    JCuda.cudaFree(d_A)
    JCuda.cudaFree(d_B)
  }

  private def JuMultiplySparseDense(A: SparseMatrix, B: DenseMatrix, C: Pointer, handle: cusparseHandle, descra:cusparseMatDescr): Unit ={

    if(!A.isTransposed) {
//      val (rowPtr, colIdx, values) = Block.csc2csr(A)



      val (rowPtr, colIdx, values) = (A.colPtrs, A.rowIndices, A.values)
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

      val d_B = new Pointer()
      JCuda.cudaMalloc(d_B, B.numRows * B.numCols * Sizeof.DOUBLE)
      JCuda.cudaMemcpy(d_B, Pointer.to(B.toArray), B.numRows * B.numCols * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice)


      JCusparse.cusparseDcsrmm(handle,
        cusparseOperation.CUSPARSE_OPERATION_NON_TRANSPOSE,
        A.numRows, B.numCols, B.numRows, nnz,
        Pointer.to(Array[Double](1.0)), descra,
        csrValA, csrRowPtrA, csrColIndA,
        d_B, B.numRows,
        Pointer.to(Array[Double](1.0)),
        C, A.numRows)

      JCuda.cudaFree(d_B)
      JCuda.cudaFree(csrColIndA)
      JCuda.cudaFree(csrRowPtrA)
      JCuda.cudaFree(csrValA)

    } else{
//      val (rowPtr, colIdx, values) = Block.csr2csc(A)
      val (rowPtr, colIdx, values) = (A.colPtrs, A.rowIndices, A.values)
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

      val d_B = new Pointer()
      JCuda.cudaMalloc(d_B, B.numRows * B.numCols * Sizeof.DOUBLE)
      JCuda.cudaMemcpy(d_B, Pointer.to(B.toArray), B.numRows * B.numCols * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice)



      JCusparse.cusparseDcsrmm(handle,
                                cusparseOperation.CUSPARSE_OPERATION_TRANSPOSE,
                                A.numRows, B.numCols, B.numRows, nnz,
                                Pointer.to(Array[Double](1.0)), descra,
                                csrValA, csrRowPtrA, csrColIndA,
                                d_B, B.numRows,
                                Pointer.to(Array[Double](1.0)),
                                C, A.numRows)

      JCuda.cudaFree(d_B)
      JCuda.cudaFree(csrColIndA)
      JCuda.cudaFree(csrRowPtrA)
      JCuda.cudaFree(csrValA)
    }
  }

  def JcuGEMMStream(a: ((Int, Int), InternalRow),
                    rightBlocks: scala.Iterable[((Int, Int), InternalRow)],
                    colByStream: Array[Int], numStream: Int, resultC: Array[Pointer],
                    Cublas:cublasHandle, Cusparse:cusparseHandle, descra:cusparseMatDescr,
                    GPUstream: Array[cudaStream_t]) : Unit ={
//    require(A.numCols == B.numRows , s"dimension mismatch a.numCols = ${A.numRows}, B.numRows = ${B.numRows}")

    val A = DMatrixSerializer.deserialize(a._2)
    A match {
      case dense: DenseMatrix =>
          val d_A = new Pointer()
          JCuda.cudaMalloc(d_A, A.numRows * A.numCols * Sizeof.DOUBLE)

          val d_B = new Array[Pointer](numStream)
          val cscColPtrB = new Array[Pointer](numStream)
          val cscRowIndB = new Array[Pointer](numStream)
          val cscValB = new Array[Pointer](numStream)

          (0 until numStream).map{i =>
            d_B(i) = new Pointer()

            cscColPtrB(i) = new Pointer()
            cscRowIndB(i) = new Pointer()
            cscValB(i) = new Pointer()
          }


          (0 until numStream).map(i => rightBlocks.filter(colByStream(i) == _._1._2).filter(a._1._2 == _._1._1).map{b =>

            DMatrixSerializer.deserialize(b._2) match{
              case mb: DenseMatrix =>



                val stat = JCublas2.cublasSetMatrix(A.numRows, A.numCols, Sizeof.DOUBLE, Pointer.to(dense.values), dense.numRows, d_A, dense.numRows)

                if(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS){
                  JCuda.cudaFree(d_A)
                  JCublas2.cublasDestroy(Cublas)
                  throw new SparkException(s"data download failed")
                }
                JuMultiplyDenseDenseStream(d_A, mb, resultC(i), Cublas, GPUstream(i), d_B(i), A.isTransposed)

                JCuda.cudaFree(d_B(i))
              case mb: SparseMatrix =>




                if(!A.isTransposed) {
                  JCuda.cudaMemcpy(d_A, Pointer.to(dense.values), A.numRows * A.numCols * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice)

                }else{
                  A.transpose
                }
                JuMultiplyDenseSparseStream(d_A, mb, resultC(i), Cusparse, GPUstream(i), cscColPtrB(i), cscRowIndB(i), cscValB(i))
                JCuda.cudaFree(cscColPtrB(i))
                JCuda.cudaFree(cscRowIndB(i))
                JCuda.cudaFree(cscValB(i))
            }

            JCuda.cudaStreamSynchronize(GPUstream(i))

          })

          JCuda.cudaFree(d_A)

      case sparse: SparseMatrix =>
//        JuMultiplySparseDenseStream(ma, mb, C, Cuspasrs, descra, stream)

        if(!sparse.isTransposed){

//          JCuda.cudaDeviceSynchronize()

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

          val d_B = new Array[Pointer](numStream)


          (0 until numStream).map{i =>
            d_B(i) = new Pointer()
          }
//          var test = 0
//          println(s"left block: ${a._1}, ${colByStream(0)}, ${colByStream(1)}")

          (0 until numStream).map(i => rightBlocks.filter(colByStream(i) == _._1._2).filter(a._1._2 == _._1._1).map{b =>
//            test = test + 1
//            println(s"row: ${a._1}, b:${b._1}, col by stream: ${colByStream(i)}")
            DMatrixSerializer.deserialize(b._2) match{
              case mb: DenseMatrix => JuMultiplySparseDenseStream(csrColIndA, csrRowPtrA, csrValA, A.numRows, A.numCols, nnz,
                mb, resultC(i), Cusparse, descra, GPUstream(i), d_B(i))

              case mb: SparseMatrix => JuMultiplySparseDenseStream(csrColIndA, csrRowPtrA, csrValA, A.numRows, A.numCols, nnz,
                mb.toDense, resultC(i), Cusparse, descra, GPUstream(i), d_B(i))
            }




            JCuda.cudaStreamSynchronize(GPUstream(i))
            JCuda.cudaFree(d_B(i))
          })


//          println(s"inner count: ${test}")
          JCuda.cudaFree(csrColIndA)
          JCuda.cudaFree(csrRowPtrA)
          JCuda.cudaFree(csrValA)

        }else{

        }


      case _ =>
        new SparkException(s"incrementalMultiply does not apply to a: ${A.getClass}")
    }
  }

  private def JuMultiplyDenseSparseStream(d_A: Pointer, B: SparseMatrix, C: Pointer, handle:cusparseHandle, stream:cudaStream_t, cscColPtrB: Pointer, cscRowIndB: Pointer, cscValB: Pointer): Unit ={

    JCusparse.cusparseSetStream(handle, stream)

    val (rowPtr, colIdx, values) = (B.colPtrs, B.rowIndices, B.values)
    val nnz = values.length


    JCuda.cudaMalloc(cscColPtrB, rowPtr.length * Sizeof.INT)
    JCuda.cudaMalloc(cscRowIndB, colIdx.length * Sizeof.INT)
    JCuda.cudaMalloc(cscValB, nnz * Sizeof.DOUBLE)

    JCuda.cudaMemcpy(cscColPtrB, Pointer.to(rowPtr), rowPtr.length * Sizeof.INT, cudaMemcpyKind.cudaMemcpyHostToDevice)
    JCuda.cudaMemcpy(cscRowIndB, Pointer.to(colIdx), colIdx.length * Sizeof.INT, cudaMemcpyKind.cudaMemcpyHostToDevice)
    JCuda.cudaMemcpy(cscValB, Pointer.to(values), values.length * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice)

      JCusparse.cusparseDgemmi(handle, B.numRows,B.numCols,B.numRows,nnz,Pointer.to(Array[Double](1.0)), d_A,
        B.numRows, cscValB, cscColPtrB, cscRowIndB, Pointer.to(Array[Double](1.0)), C, B.numRows)


  }

  private def JuMultiplySparseDenseStream(csrColIndA:Pointer, csrRowPtrA:Pointer, csrValA:Pointer, numRows:Int, numCols:Int, nnz: Int,
                                          B: DenseMatrix, C: Pointer, handle: cusparseHandle, descra:cusparseMatDescr, stream:cudaStream_t, d_B: Pointer): Unit ={


      JCuda.cudaMalloc(d_B, numRows * numCols * Sizeof.DOUBLE)

      JCusparse.cusparseSetStream(handle, stream)

      JCuda.cudaMemcpyAsync(d_B, Pointer.to(B.values), B.numRows * B.numCols * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice, stream)



      JCusparse.cusparseDcsrmm(handle,
        cusparseOperation.CUSPARSE_OPERATION_NON_TRANSPOSE,
        numRows, B.numCols, B.numRows, nnz,
        Pointer.to(Array[Double](1.0)), descra,
        csrValA, csrRowPtrA, csrColIndA,
        d_B, B.numRows,
        Pointer.to(Array[Double](1.0)),
        C, numRows)



  }

  private def JuMultiplyDenseDenseStream(d_A: Pointer, B: DenseMatrix, C: Pointer, handle:cublasHandle, stream:cudaStream_t, d_B: Pointer, Atrans:Boolean): Unit ={
    //    val handle = new jcublas.cublasHandle

    JCuda.cudaMalloc(d_B, B.numRows * B.numCols * Sizeof.DOUBLE)

    JCublas2.cublasSetStream(handle, stream)

    JCuda.cudaMemcpyAsync(d_B, Pointer.to(B.values), B.numRows * B.numCols * Sizeof.DOUBLE, cudaMemcpyKind.cudaMemcpyHostToDevice, stream)

    if(!Atrans && !B.isTransposed) {
      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_N, jcublas.cublasOperation.CUBLAS_OP_N,
        B.numRows, B.numCols, B.numCols,
        Pointer.to(Array[Double](1.0)), d_A, B.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, B.numRows)
    }else if(!Atrans && B.isTransposed)

      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_N, jcublas.cublasOperation.CUBLAS_OP_T,
        B.numRows, B.numCols, B.numCols,
        Pointer.to(Array[Double](1.0)), d_A, B.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, B.numRows)
    else if(Atrans && !B.isTransposed)

      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_T, jcublas.cublasOperation.CUBLAS_OP_N,
        B.numRows, B.numCols, B.numCols,
        Pointer.to(Array[Double](1.0)), d_A, B.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, B.numRows)
    else

      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_T, jcublas.cublasOperation.CUBLAS_OP_T,
        B.numRows, B.numCols, B.numCols,
        Pointer.to(Array[Double](1.0)), d_A, B.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, B.numRows)

  }
}
