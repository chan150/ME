package org.apache.spark.sql.me.matrix

import com.github.fommil.netlib.{F2jBLAS, BLAS => NetlibBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}


object  BLAS extends Serializable with Logging{
  @transient
  private var _f2jBLAS: NetlibBLAS = _
  @transient
  private var _nativeBLAS: NetlibBLAS = _

  private def f2jBLAS: NetlibBLAS ={
    if(_f2jBLAS == null) _f2jBLAS = new F2jBLAS

    _f2jBLAS
  }


  /*
  y += a * x
   */
  def axpy(a: Double, x: Vector, y: Vector): Unit ={
    require(x.size == y.size)
    y match{
      case dy: DenseVector =>
        x match{
          case sx: SparseVector =>
            axpysd(a, sx, dy)
          case dx: DenseVector =>
            axpydd(a, dx, dy)
          case _ =>
            throw new UnsupportedOperationException(s"axpy doesn't support x type ${x.getClass}")
        }
      case _ =>
        throw new IllegalArgumentException(s"axpy only supports adding to a dense vector but got type ${y.getClass}")
    }
  }

  private def axpydd(a: Double, x: DenseVector, y:DenseVector): Unit ={
    val n = x.size
    f2jBLAS.daxpy(n, a, x.values, 1, y.values, 1)
  }

  private def axpysd(a: Double, x: SparseVector, y: DenseVector): Unit ={
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    if(a == 1.0){
      var k = 0
      while(k < nnz){
        yValues(xIndices(k)) += xValues(k)
        k +=1
      }
    } else{
      var k = 0
      while (k < nnz){
        yValues(xIndices(k)) += a * xValues(k)
        k += 1
      }
    }
  }

  /*
  dot(x, y)
   */

  def dot(x: Vector, y:Vector): Double ={
    require(x.size == y.size, s"dot(x: Vector, y: Vector) was given Vectors with non-matching sizes: x.size = ${x.size}, y.size = ${y.size}.")
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) => dotdd(dx, dy)
      case (sx: SparseVector, dy: DenseVector) => dotsd(sx, dy)
      case (dx: DenseVector, sy: SparseVector) => dotsd(sy, dx)
      case (sx: SparseVector, sy: SparseVector) => dotss(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  private def dotdd(x: DenseVector, y: DenseVector): Double ={
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  private def dotsd(x: SparseVector, y: DenseVector): Double ={
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    var sum = 0.0
    var k = 0
    while(k < nnz){
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  private def dotss(x: SparseVector, y:SparseVector): Double ={
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.size
    val nnzy = yIndices.size

    var kx = 0
    var ky = 0
    var sum = 0.0

    while(kx < nnzx && ky < nnzy){
      val ix = xIndices(kx)
      while(ky< nnzy && yIndices(ky) < ix){
        ky += 1
      }
      if(ky < nnzy && yIndices(ky) == ix){
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }

  def copy(x: Vector, y: Vector): Unit = {
    val n = y.size
    require(x.size == n)
    y match{
      case dy: DenseVector =>
        x match{
          case sx: SparseVector =>
            val sxIndices = sx.indices
            val sxValues = sx.values
            val dyValues = dy.values
            val nnz = sxIndices.size

            var i = 0
            var k = 0
            while(k < nnz){
              val j = sxIndices(k)
              while(i < j){
                dyValues(i) = 0.0
                i += 1
              }
              dyValues(i) = sxValues(k)
              i += 1
              k += 1
            }
            while(i < n){
              dyValues(i) = 0.0
              i += 1
            }
          case dx: DenseVector =>
            Array.copy(dx.values, 0, dy.values, 0, n)
        }
      case _ =>
        throw new IllegalArgumentException(s"y must be dense in copy but got ${y.getClass}")
    }
  }

  /*
  x = a * x
   */
  def scal(a: Double, x: Vector): Unit ={
    x match{
      case sx: SparseVector => f2jBLAS.dscal(sx.values.size, a, sx.values, 1)
      case dx: DenseVector => f2jBLAS.dscal(dx.values.size, a, dx.values, 1)
      case _ =>
        throw new IllegalArgumentException(s"scal doesn't support vector type ${x.getClass}.")
    }
  }

  private def nativeBLAS: NetlibBLAS ={
    if(_nativeBLAS == null)
      _nativeBLAS = NativeBLAS
    _nativeBLAS
  }

  /*
  A := alpha * x * x^T + A
   */
  def syr(alpha: Double, x: Vector, A: DenseMatrix):Unit ={
    val mA = A.numRows
    val nA = A.numCols
    require(mA == nA, s"A is not a square matrix (and hence is not symmetric). A: $mA x $nA")
    require(mA == x.size, s"The size of x doesn't match the rank of A. A: $mA x $nA, x: ${x.size}")

    x match{
      case dv: DenseVector => syrd(alpha, dv, A)
      case sv: SparseVector => syrs(alpha, sv, A)
      case _ =>
        throw new IllegalArgumentException(s"syr doesn't support vector type ${x.getClass}.")
    }
  }

  private def syrd(alpha: Double, x: DenseVector, A: DenseMatrix): Unit ={
    val nA = A.numRows
    val mA = A.numCols

    nativeBLAS.dsyr("U", x.size, alpha, x.values, 1, A.values, nA)

    var i = 0
    while(i < mA) {
      var j = i + 1
      while(j < nA){
        A(j, i) = A(i, j)
        j += 1
      }
      i += 1
    }
  }

  private def syrs(alpha: Double, x: SparseVector, A: DenseMatrix): Unit ={
    val mA = A.numCols
    val xIndices = x.indices
    val xValues = x.values
    val nnz = xValues.length
    val Avalues = A.values

    var i = 0
    while(i < nnz) {
      val multiplier = alpha * xValues(i)
      val offset = xIndices(i) * mA
      var j = 0
      while(j < nnz){
        Avalues(xIndices(j) + offset) += multiplier * xValues(j)
        j += 1
      }
      i += 1
    }
  }

  /*
  C := alpha * A * B + beta * C
   */

  def gemm( alpha: Double, A: DistributedMatrix, B: DenseMatrix, beta:Double, C: DenseMatrix): Unit ={
    require(!C.isTransposed, "The matrix C cannot be the product of a transpose() call. C.isTransposed must be false.")
    if(alpha == 0.0 && beta == 1.0) {
      logDebug("gemm: alpha is equal to 0 and beta is equal to 1. Returning C.")
    } else if (alpha == 0.0){
      f2jBLAS.dscal(C.values.length, beta, C.values, 1)
    } else{
      A match{
        case sparse: SparseMatrix => gemmsdd(alpha, sparse, B, beta, C)
        case dense: DenseMatrix => gemmddd(alpha, dense, B, beta, C)
        case _ =>
          throw new IllegalArgumentException(s"gemm doesn't support matrix type ${A.getClass}")
      }
    }
  }

  /*
  C:= alpha * A * B + beta * C
  For 'DenseMatrix' A.
   */
  private def gemmddd(alpha: Double, A: DenseMatrix, B: DenseMatrix, beta: Double, C:DenseMatrix): Unit ={
    val tAstr = if (A.isTransposed) "T" else "N"
    val tBstr = if (B.isTransposed) "T" else "N"
    val lda = if (!A.isTransposed) A.numRows else A.numCols
    val ldb = if (!B.isTransposed) B.numRows else B.numCols

    require(A.numCols == B.numRows, s"The columns of A don't match the rows of B. A: ${A.numCols}, B: ${B.numRows}")
    require(A.numRows == C.numRows, s"The rows of C don't match the rows of A. C: ${C.numRows}, A: ${A.numRows}")
    require(B.numCols == C.numCols, s"The columns of C don't match the columns of B. C: ${C.numCols}, A: ${B.numCols}")

    nativeBLAS.dgemm(tAstr, tBstr, A.numRows, B.numCols, A.numCols, alpha, A.values, lda, B.values, ldb, beta, C.values, C.numRows)
  }

  /*
  C:= alpha * A * B + beta * C
  For 'SparseMatrix' A.
   */

  private def gemmsdd(alpha: Double, A: SparseMatrix, B: DenseMatrix, beta: Double, C: DenseMatrix): Unit ={
    val mA: Int = A.numRows
    val nB: Int = B.numCols
    val kA: Int = A.numCols
    val kB: Int = B.numRows

    require(kA == kB, s"The columns of A don't match the rows of B. A: $kA, B: $kB")
    require(mA == C.numRows, s"The rows of C don't match the rows of A. C: ${C.numRows}, A: $mA")
    require(nB == C.numCols, s"The columns of C don't match the columns of B. C: ${C.numCols}, A: $nB")

    val Avals = A.values
    val Bvals = B.values
    val Cvals = C.values
    val ArowIndices = A.rowIndices
    val AcolPtrs = A.colPtrs

    if(A.isTransposed){
      var colCounterForB = 0
      if(!B.isTransposed) {
        while(colCounterForB < nB){
          var rowCounterForA = 0
          val Cstart = colCounterForB * mA
          val Bstart = colCounterForB * kA

          while(rowCounterForA < mA){
            var i = AcolPtrs(rowCounterForA)
            val idxEnd = AcolPtrs(rowCounterForA + 1)
            var sum = 0.0

            while(i < idxEnd){
              sum += Avals(i) * Bvals(Bstart + ArowIndices(i))
              i += 1
            }
            val Cindex = Cstart + rowCounterForA
            Cvals(Cindex) = beta * Cvals(Cindex) + sum * alpha
            rowCounterForA += 1
          }
          colCounterForB += 1
        }
      } else{
        while(colCounterForB < nB){
          var rowCounterForA = 0
          val Cstart = colCounterForB * mA

          while(rowCounterForA < mA){
            var i = AcolPtrs(rowCounterForA)
            val idxEnd = AcolPtrs(rowCounterForA + 1)
            var sum = 0.0

            while(i < idxEnd){
              sum += Avals(i) * B(ArowIndices(i), colCounterForB)
              i += 1
            }
            val Cindex = Cstart + rowCounterForA
            Cvals(Cindex) = beta * Cvals(Cindex) + sum * alpha
            rowCounterForA += 1
          }
          colCounterForB += 1
        }
      }
    } else{
      // Scala matrix first if 'beta' is not equal to 1.0
      if(beta != 1.0){
        f2jBLAS.dscal(C.values.length, beta, C.values, 1)
      }

      // Perform matrix multiplication and add to C. The rows of A are multiplied by the columns of B, and added to C.
      var colCounterForB = 0 // the column to be updated in C

      if(!B.isTransposed) { //Expensive to put the check inside the loop
        while (colCounterForB < nB) {
          var colCounterForA = 0
          val Bstart = colCounterForB * kB
          val Cstart = colCounterForB * mA

          while (colCounterForA < kA) {
            var i = AcolPtrs(colCounterForA)
            val idxEnd = AcolPtrs(colCounterForA + 1)
            val Bval = Bvals(Bstart + colCounterForA) * alpha

            while (i < idxEnd) {
              Cvals(Cstart + ArowIndices(i)) += Avals(i) * Bval
              i += 1
            }
            colCounterForA += 1
          }
          colCounterForB += 1
        }
      } else{
        while(colCounterForB < nB){
          var colCounterForA = 0 // The column of A to multiply with the row of B
          val Cstart = colCounterForB * mA

          while(colCounterForA < kA){
            var i = AcolPtrs(colCounterForA)
            val idxEnd = AcolPtrs(colCounterForA + 1)
            val Bval = B(colCounterForA, colCounterForB) * alpha

            while(i < idxEnd){
              Cvals(Cstart + ArowIndices(i)) += Avals(i) * Bval
              i += 1
            }
            colCounterForA += 1
          }
          colCounterForB += 1
        }
      }
    }
  }

  /*
  y := alpha * A * x + beta * y
   */
  def gemv(alpha: Double, A: DistributedMatrix, x: Vector, beta: Double, y: DenseVector): Unit ={
    require(A.numCols == x.size, s"The columns of A don't match the number of elements of x. A: ${A.numCols}, x: ${x.size}")
    require(A.numRows == y.size, s"The rows of A don't match the number of elements of y. A: ${A.numRows}, y:${y.size}")

    if(alpha == 0.0 && beta == 1.0){
      logDebug("gemv: alpha is equal to 0 and beta is equal to 1. Retrurning y.")
    } else if (alpha == 0.0){
      scal(beta, y)
    } else{
      (A, x) match {
        case (smA: SparseMatrix, dvx: DenseVector) =>
          gemvsdd(alpha, smA, dvx, beta, y)
        case (smA: SparseMatrix, svx: SparseVector) =>
          gemvssd(alpha, smA, svx, beta, y)
        case (dmA: DenseMatrix, dvx: DenseVector) =>
          gemvddd(alpha, dmA, dvx, beta, y)
        case (dmA: DenseMatrix, svx: SparseVector) =>
          gemvdsd(alpha, dmA, svx, beta, y)
        case _ =>
          throw new IllegalArgumentException(s"gemv dosen't support running on matrix type ${A.getClass} and vector type ${x.getClass}")
      }
    }
  }

  /*
  y := alpha * A * x + beta * y
  For 'DenseMatrix' A and 'DenseVector' x.
   */
  private def gemvddd(alpha: Double, A: DenseMatrix, x: DenseVector, beta: Double, y: DenseVector): Unit ={
    val tStrA = if(A.isTransposed) "T" else "N"
    val mA = if (!A.isTransposed) A.numRows else A.numCols
    val nA = if (!A.isTransposed) A.numCols else A.numRows
    nativeBLAS.dgemv(tStrA, mA, nA, alpha, A.values, mA, x.values, 1, beta, y.values, 1)
  }

  private def gemvdsd(alpha: Double, A: DenseMatrix, x: SparseVector, beta: Double, y:DenseVector): Unit ={
    val mA: Int = A.numRows
    val nA: Int = A.numCols

    val Avals = A.values

    val xIndices = x.indices
    val xNnz = xIndices.length
    val xValues = x.values
    val yValues = y.values

    if(A.isTransposed){
      var rowCounterForA = 0
      while (rowCounterForA < mA){
        var sum = 0.0
        var k = 0
        while (k < xNnz){
          sum += xValues(k) * Avals(xIndices(k) + rowCounterForA * nA)
          k += 1
        }
        yValues(rowCounterForA) = sum * alpha + beta * yValues(rowCounterForA)
        rowCounterForA += 1
      }
    } else {
      var rowCounterForA = 0
      while (rowCounterForA < mA) {
        var sum = 0.0
        var k = 0
        while (k < xNnz) {
          sum += xValues(k) * Avals(xIndices(k) * mA + rowCounterForA)
          k += 1
        }
        yValues(rowCounterForA) = sum * alpha + beta * yValues(rowCounterForA)
        rowCounterForA += 1
      }
    }
  }

  /*
  y := alpha * A * x + beta * y
  For 'SparseMatrix' A and 'SparseVector' x.
   */
  private def gemvssd(alpha: Double, A: SparseMatrix, x: SparseVector, beta: Double, y: DenseVector): Unit ={
    val xValues = x.values
    val xIndices = x.indices
    val xNnz = xIndices.length

    val yValues = y.values

    val mA: Int = A.numRows
    val nA: Int = A.numCols

    val Avals = A.values
    val Arows = if (!A.isTransposed) A.rowIndices else A.colPtrs
    val Acols = if (!A.isTransposed) A.colPtrs else A.rowIndices

    if(A.isTransposed) {
      var rowCounter = 0
      while(rowCounter < mA){
        var i = Arows(rowCounter)
        val idxEnd = Arows(rowCounter + 1)
        var sum = 0.0
        var k = 0
        while(k < xNnz && i < idxEnd) {
          if(xIndices(k) == Acols(i)) {
            sum += Avals(i) * xValues(k)
            i += 1
          }
          k += 1
        }
        yValues(rowCounter) = sum * alpha + beta * yValues(rowCounter)
        rowCounter += 1
      }
    } else {
      if (beta != 1.0) scal(beta, y)

      var colCounterForA = 0
      var k = 0
      while(colCounterForA < nA && k < xNnz) {
        if (xIndices(k) == colCounterForA){
          var i = Acols(colCounterForA)
          val idxEnd = Acols(colCounterForA + 1)

          val xTemp = xValues(k) * alpha
          while(i < idxEnd){
            val rowIndex = Arows(i)
            yValues(Arows(i)) += Avals(i) * xTemp
            i += 1
          }
          k += 1
        }
        colCounterForA += 1
      }
    }
  }

  /*
  y := alpha * A * x + beta * y
  For 'SparseMatrix' A and 'DenseVector' x.
   */
  private def gemvsdd(alpha: Double, A: SparseMatrix, x: DenseVector, beta: Double, y: DenseVector): Unit = {
    val xValues = x.values
    val yValues = y.values
    val mA: Int = A.numRows
    val nA: Int = A.numCols

    val Avals = A.values
    val Arows = if (!A.isTransposed) A.rowIndices else A.colPtrs
    val Acols = if (!A.isTransposed) A.colPtrs else A.rowIndices

    if(A.isTransposed){
      var rowCounter = 0
      while(rowCounter < mA){
        var i = Arows(rowCounter)
        val idxEnd = Arows(rowCounter + 1)
        var sum = 0.0
        while(i < idxEnd){
          sum += Avals(i) * xValues(Acols(i))
          i += 1
        }
        yValues(rowCounter) = beta * yValues(rowCounter) + sum * alpha
        rowCounter += 1
      }
    } else {
      if (beta != 1.0) scal(beta, y)

      var colCounterForA = 0
      while(colCounterForA < nA){
        var i = Acols(colCounterForA)
        val idxEnd = Acols(colCounterForA + 1)
        val xVal = xValues(colCounterForA) * alpha

        while(i < idxEnd){
          val rowIndex = Arows(i)
          yValues(rowIndex) += Avals(i) * xVal
          i += 1
        }
        colCounterForA += 1
      }
    }
  }
}
