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

    var stat = JCublas2.cublasSetMatrix(A.numRows, A.numCols, Sizeof.DOUBLE,Pointer.to(A.toArray),A.numRows, d_A, A.numRows)
    if(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS){
      JCuda.cudaFree(d_A)
      JCublas2.cublasDestroy(handle)
      throw new SparkException(s"data download failed")
    }

    stat = JCublas2.cublasSetMatrix(B.numRows, B.numCols, Sizeof.DOUBLE,Pointer.to(B.toArray),B.numRows, d_B, B.numRows)
    if(stat != jcublas.cublasStatus.CUBLAS_STATUS_SUCCESS){
      JCuda.cudaFree(d_B)
      JCublas2.cublasDestroy(handle)
      throw new SparkException(s"data download failed")
    }
    if(!A.isTransposed && !B.isTransposed)
      JCublas2.cublasDgemm(handle,
        jcublas.cublasOperation.CUBLAS_OP_N, jcublas.cublasOperation.CUBLAS_OP_N,
        A.numRows, B.numCols, A.numCols,
        Pointer.to(Array[Double](1.0)), d_A, A.numRows, d_B, B.numRows, Pointer.to(Array[Double](1.0)), C, A.numRows)
    else if(!A.isTransposed && B.isTransposed)

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
}
