package org.apache.spark.sql.me.matrix

import org.apache.spark.SparkException

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import breeze.linalg.{DenseMatrix => BDM, CSCMatrix => BSM,Matrix => BM}

object Block{

  def aggregate(mat: DistributedMatrix, agg: (Double, Double) => Double): Double ={
    if(mat == null){
      throw new SparkException("matrix cannot be null for aggregation")
    } else{
      mat match {
        case dm: DenseMatrix => dm.values.reduce(agg)
        case sm: SparseMatrix => sm.values.reduce(agg)
        case _ => throw new SparkException("Illegal matrix type")
      }
    }
  }

  def compute(a: DistributedMatrix, b: DistributedMatrix, f: (Double, Double) => Double): DistributedMatrix ={
    if(a != null && b != null){
      (a, b) match {
        case (ma: DenseMatrix, mb: DenseMatrix) => computeDense(ma, mb, f)
        case (ma: DenseMatrix, mb: SparseMatrix) => computeDenseSparse(ma, mb, f)
        case (ma: SparseMatrix, mb: DenseMatrix) => computeDenseSparse(mb, ma, f)
        case (ma: SparseMatrix, mb: SparseMatrix) => computeSparseSparse(ma, mb, f)
      }
    } else {
      null
    }
  }

  def add(a: DistributedMatrix, b: DistributedMatrix): DistributedMatrix ={
    if (a != null && b != null){
      require(a.numRows == b.numRows, s"Matrix A and B must have the same number of rows. But found A.numRows = ${a.numRows}, B.numRows = ${b.numRows}")
      require(a.numCols == b.numCols, s"Matrix A and B must have the same number of cols. but found A.numCols = ${a.numCols}, B.numCols = ${b.numCols}")

      (a, b) match {
        case (ma: DenseMatrix, mb: DenseMatrix) => addDense(ma, mb)
        case (ma: DenseMatrix, mb: SparseMatrix) => addDenseSparse(ma, mb)
        case (ma: SparseMatrix, mb: DenseMatrix) => addDenseSparse(mb, ma)
        case (ma: SparseMatrix, mb: SparseMatrix) => addSparseSparse(ma, mb)
      }
    } else{
      if (a != null && b == null) a
      else if (a == null && b != null) b
      else null
    }
  }

  private def computeDense(ma: DenseMatrix, mb: DenseMatrix, f: (Double, Double) => Double): DistributedMatrix ={
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(math.min(arr1.length, arr2.length))(0.0)

    for(i <- 0 until arr.length){
      arr(i) = f(arr1(i), arr2(i))
    }
    new DenseMatrix(math.min(ma.numRows, mb.numRows), math.min(ma.numCols, mb.numCols), arr)
  }

  private def addDense(ma: DenseMatrix, mb: DenseMatrix): DistributedMatrix ={
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)
    for(i <- 0 until arr.length) {
      arr(i) = arr1(i) + arr2(i)
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  private def computeDenseSparse(ma: DenseMatrix, mb: SparseMatrix, f: (Double, Double) => Double): DistributedMatrix ={
    val rand = new scala.util.Random()
    if(math.abs(f(rand.nextDouble(), 0.0)) > 1e-6){
      val (arr1, arr2) = (ma.toArray, mb.toArray)
      val arr = Array.fill(math.min(arr1.length, arr2.length))(0.0)
      for (i <- 0 until arr.length) {
        arr(i) = f(arr1(i), arr2(i))
      }
      new DenseMatrix(math.min(ma.numRows, mb.numRows), math.min(ma.numCols, mb.numCols), arr)
    } else {
      if (ma.numRows * ma.numCols >= mb.numRows * mb.numCols) {
        val arr = Array.fill(mb.values.length)(0.0)
        if(!mb.isTransposed){
          for(k <- 0 until mb.numCols){
            val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
            for (j <- 0 until cnt) {
              val idx = mb.colPtrs(k) + j
              val rid = mb.rowIndices(idx)
              arr(idx) = f(ma(rid, k), mb.values(idx))
            }
          }
        } else{
          for (k <- 0 until mb.numRows) {
            val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
            for(i <- 0 until cnt) {
              val idx = mb.colPtrs(k) + i
              val cid = mb.rowIndices(idx)
              arr(idx) = f(ma(k, cid), mb.values(idx))
            }
          }
        }
        new SparseMatrix(mb.numRows, mb.numCols, mb.colPtrs, mb.rowIndices, arr, mb.isTransposed)
      } else{
        val arr = Array.fill(ma.values.length)(0.0)
        val (arr1, arr2) = (ma.toArray, mb.toArray)
        for(i <- 0 until arr.length){
          if(arr2(i) > 0.0)
            arr(i) = f(arr1(i), arr2(i))
        }
        new DenseMatrix(ma.numRows, ma.numCols, arr)
      }
    }
  }

  private def addDenseSparse(ma: DenseMatrix, mb: SparseMatrix): DistributedMatrix ={
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)
    for(i <- 0 until arr.length){
      arr(i) = arr1(i) + arr2(i)
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  private def computeSparseSparse(ma: SparseMatrix, mb: SparseMatrix, f:(Double, Double) => Double): DistributedMatrix={
    val rand = new scala.util.Random()
    if(math.abs(f(rand.nextDouble(), 0.0)) > 1e-6 && math.abs(f(0.0, rand.nextDouble())) > 1e-6 ){
      val (arr1, arr2) =  (ma.toArray, mb.toArray)
      val arr = Array.fill(math.min(arr1.length, arr2.length))(0.0)
      var nnz = 0
      for (i <- 0 until arr.length){
        arr(i) = f(arr1(i), arr2(i))
        if(arr(i) != 0) nnz += 1
      }
      val c = new DenseMatrix(math.min(ma.numRows, mb.numRows), math.min(ma.numCols, mb.numCols), arr)

      if(c.numRows * c.numCols > nnz * 2 + c.numCols + 1){
        c.toSparse
      } else{
        c
      }
    } else{
      if(math.abs(f(0.0, rand.nextDouble())) > 1e-6){
        if(ma.numRows * ma.numCols <= mb.numRows * mb.numCols){
          val arr = Array.fill(ma.values.length)(0.0)
          if(!ma.isTransposed) {
            for(k <- 0 until ma.numCols) {
              val cnt = ma.colPtrs(k + 1) - ma.colPtrs(k)
              for(j <- 0 until cnt) {
                val idx = ma.colPtrs(k) + j
                val rid = ma.rowIndices(idx)
                arr(idx) = f(ma.values(idx), mb(rid, k))
              }
            }
          } else {
            for (k <- 0 until ma.numRows) {
              val cnt = ma.colPtrs(k + 1) - ma.colPtrs(k)
              for (i <- 0 until cnt) {
                val idx = ma.colPtrs(k) + i
                val cid = ma.rowIndices(idx)
                arr(idx) = f(ma.values(idx), mb(k, cid))
              }
            }
          }
          new SparseMatrix(ma.numRows, ma.numCols, ma.colPtrs, ma.rowIndices, arr, ma.isTransposed)
        } else {
          val arr = Array.fill(mb.values.length)(0.0)
          if (!mb.isTransposed) {
            for (k <- 0 until mb.numCols){
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (j <- 0 until cnt){
                val idx = mb.colPtrs(k) + j
                val rid = mb.rowIndices(idx)
                val mav = ma(rid, k)
                if(mav > 0.0){
                  arr(idx) = f(mav, mb.values(idx))
                }
              }
            }
          } else {
            for (k <- 0 until mb.numRows){
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (i <- 0 until cnt) {
                val idx = mb.colPtrs(k) + i
                val cid = mb.rowIndices(idx)
                val mav = ma(k, cid)
                if (mav > 0.0) {
                  arr(idx) = f(mav, mb.values(idx))
                }
              }
            }
          }
          new SparseMatrix(mb.numRows, mb.numCols, mb.colPtrs, mb.rowIndices, arr, mb.isTransposed)
        }
      } else {
        if (ma.numRows * ma.numCols >= mb.numRows * mb.numCols){
          val arr = Array.fill(mb.values.length)(0.0)
          if (!mb.isTransposed) {
            for (k <- 0 until mb.numCols) {
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (j <- 0 until cnt) {
                val idx = mb.colPtrs(k) + j
                val rid = mb.rowIndices(idx)
                arr(idx) = f(ma(rid, k), mb.values(idx))
              }
            }
          } else {
            for (k <- 0 until mb.numRows){
              val cnt = mb.colPtrs(k + 1) - mb.colPtrs(k)
              for (i <- 0 until cnt) {
                val idx = mb.colPtrs(k) + i
                val cid = mb.rowIndices(idx)
                arr(idx) = f(ma(k, cid), mb.values(idx))
              }
            }
          }
          new SparseMatrix(mb.numRows, mb.numCols, mb.colPtrs, mb.rowIndices, arr, mb.isTransposed)
        } else {
          val arr = Array.fill(ma.values.length)(0.0)
          if (!ma.isTransposed){
            for (k <- 0 until ma.numCols) {
              val cnt = ma.colPtrs(k+1) - ma.colPtrs(k)
              for (j <- 0 until cnt){
                val idx = ma.colPtrs(k) + j
                val rid = ma.rowIndices(idx)
                val mbv = mb(rid, k)
                if(mbv > 0.0) {
                  arr(idx) = f(ma.values(idx), mbv)
                }
              }
            }
          } else {
            for (k <- 0 until ma.numRows) {
              val cnt = ma.colPtrs(k + 1) - ma.colPtrs(k)
              for (i <- 0 until cnt) {
                val idx = ma.colPtrs(k) + i
                val cid = ma.rowIndices(idx)
                val mbv = mb(k, cid)
                if(mbv > 0.0){
                  arr(idx) = f(ma.values(idx), mbv)
                }
              }
            }
          }
          new SparseMatrix(ma.numRows, ma.numCols, ma.colPtrs, ma.rowIndices, arr, ma.isTransposed)
        }
      }
    }
  }

  private def addSparseSparse(ma: SparseMatrix, mb: SparseMatrix): DistributedMatrix ={
    if(ma.isTransposed || mb.isTransposed){
      val (arr1, arr2) = (ma.toArray, mb.toArray)
      val arr = Array.fill(arr1.length)(0.0)
      var nnz = 0
      for (i <- 0 until arr.length){
        arr(i) = arr1(i) + arr2(i)
        if(arr(i) != 0) nnz += 1
      }
      val c = new DenseMatrix(ma.numRows, ma.numCols, arr)
      if(c.numRows * c.numCols > nnz * 2 + c.numCols + 1){
        c.toSparse
      } else{
        c
      }
    } else{
      addSparseSparseNative(ma, mb)
    }
  }


  private def arrayClear(arr: Array[Double]): Unit = {
    for (i <- 0 until arr.length) {
      arr(i) = 0.0
    }
  }

  private def addSparseSparseNative(ma: SparseMatrix, mb: SparseMatrix): DistributedMatrix ={
    require(ma.numRows == mb.numRows, s"The number of rows for Matrix A must be equal to the number of rows for matrix B, but found A.numRows = ${ma.numRows}, B.numRows = ${mb.numRows}")
    require(ma.numCols == mb.numCols, s"The number of cols for Matrix A must be equal to the number of cols for matrix B, but found A.numCols = ${ma.numCols}, B.numCols = ${mb.numCols}")

    val va = ma.values
    val rowIdxA = ma.rowIndices
    val colPtrA = ma.colPtrs

    val vb = mb.values
    val rowIdxB = mb.rowIndices
    val colPtrB = mb.colPtrs

    val vc = ArrayBuffer[Double]()
    val rowIdxC = ArrayBuffer[Int]()
    val colPtrC = new Array[Int](ma.numCols + 1)
    val coltmp = new Array[Double](ma.numRows)

    for(jc <- 0 until ma.numCols) {
      val numPerColB = colPtrB(jc + 1) - colPtrB(jc)
      val numperColA = colPtrA(jc + 1) - colPtrA(jc)

      arrayClear(coltmp)

      for (ia <- colPtrA(jc) until colPtrA(jc) + numperColA){
        coltmp(rowIdxA(ia)) += va(ia)
      }
      for(ib <- colPtrB(jc) until colPtrB(jc) + numPerColB) {
        coltmp(rowIdxB(ib)) += vb(ib)
      }

      var count = 0
      for(i <- 0 until coltmp.length) {
        if(coltmp(i) != 0.0){
          rowIdxC += i
          vc += coltmp(i)
          count += 1
        }
      }

      colPtrC(jc + 1) = colPtrC(jc) + count
    }
    if(ma.numRows * ma.numCols > 2 * colPtrC(colPtrC.length - 1) + colPtrC.length){
      new SparseMatrix(ma.numRows, ma.numCols, colPtrC, rowIdxC.toArray, vc.toArray)
    } else{
      new SparseMatrix(ma.numRows, ma.numCols, colPtrC, rowIdxC.toArray, vc.toArray).toDense
    }
  }


  def multiplySparseSparse(ma: SparseMatrix, mb: SparseMatrix): DistributedMatrix ={
    require(ma.numCols == mb.numRows, s"Matrix A.numCols must be equals to B.numRows, but found A.numCols = ${ma.numCols}, B.numRows = ${mb.numRows}")

    (ma.isTransposed, mb.isTransposed) match {
      case (false, false) => multiplyCSC_CSC(ma, mb)
      case (true, true) => multiplyCSR_CSR(ma, mb)
      case (true, false) => multiplyCSR_CSC(ma, mb)
      case (false, true) => multiplyCSC_CSR(ma, mb)
    }
  }

  private def multiplyCSC_CSC(ma: SparseMatrix, mb: SparseMatrix): DistributedMatrix ={
    val va = ma.values
    val rowIdxA = ma.rowIndices
    val colPtrA = ma.colPtrs

    val vb = mb.values
    val rowIdxB = mb.rowIndices
    val colPtrB = mb.colPtrs

    val vc = new ArrayBuffer[Double]()
    val rowIdxC = new ArrayBuffer[Int]()
    val colPtrC = new Array[Int](mb.numCols + 1)
    val coltmp = new Array[Double](ma.numRows)

    for(jb <- 0 until mb.numCols) {
      val numPerColB = colPtrB(jb + 1) - colPtrB(jb)
      arrayClear(coltmp)
      for(ib <- colPtrB(jb) until colPtrB(jb) + numPerColB) {
        val alpha = vb(ib)
        val ja = rowIdxB(ib)
        val numPerColA = colPtrA(ja + 1) - colPtrA(ja)
        for (ia <- colPtrA(ja) until colPtrA(ja) + numPerColA) {
          val idx = rowIdxA(ia)
          coltmp(idx) += va(ia) * alpha
        }
      }
      var count = 0
      for(i <- 0 until coltmp.length) {
        if(coltmp(i) != 0.0){
          rowIdxC += i
          vc += coltmp(i)
          count += 1
        }
      }
      colPtrC(jb + 1) = colPtrC(jb) + count
    }
    if(ma.numRows * mb.numCols > 2 * colPtrC(colPtrC.length-1) + colPtrC.length){
      new SparseMatrix(ma.numRows, mb.numCols, colPtrC, rowIdxC.toArray, vc.toArray)
    } else{
      new SparseMatrix(ma.numRows, mb.numCols, colPtrC, rowIdxC.toArray, vc.toArray).toDense
    }
  }

  private def multiplyCSR_CSR(ma: SparseMatrix, mb: SparseMatrix): DistributedMatrix ={
    val va = ma.values
    val colIdxA = ma.rowIndices
    val rowPtrA = ma.colPtrs

    val vb = mb.values
    val colIdxB = mb.rowIndices
    val rowPtrB = mb.colPtrs

    val vc = new ArrayBuffer[Double]()
    val colIdxC = new ArrayBuffer[Int]()
    val rowPtrC = new Array[Int](ma.numRows + 1)
    val rowtmp = new Array[Double](mb.numCols)

    for(ia <- 0 until ma.numRows){
      val numPerRowA = rowPtrA(ia + 1) - rowPtrA(ia)
      arrayClear(rowtmp)
      for(ja <- rowPtrA(ia) until rowPtrA(ia) + numPerRowA){
        val alpha = va(ja)
        val ib = colIdxA(ja)
        val numPerRowB = rowPtrB(ib + 1) - rowPtrB(ib)

        for(jb <- rowPtrB(ib) until rowPtrB(ib) + numPerRowB) {
          val idx = colIdxB(jb)
          rowtmp(idx) += vb(jb) * alpha
        }
      }
      var count = 0
      for (i <- 0 until rowtmp.length){
        if(rowtmp(i) != 0.0){
          colIdxC += i
          vc += rowtmp(i)
          count += 1
        }
      }
      rowPtrC(ia + 1) = rowPtrC(ia) + count
    }
    if(ma.numRows * mb.numCols > 2 * rowPtrC(rowPtrC.length -1) + rowPtrC.length){
      new SparseMatrix(ma.numRows, mb.numCols, rowPtrC, colIdxC.toArray, vc.toArray, true)
    } else{
      new SparseMatrix(ma.numRows, mb.numCols, rowPtrC, colIdxC.toArray, vc.toArray, true).toDense
    }
  }

  private def multiplyCSR_CSC(ma: SparseMatrix, mb: SparseMatrix): DistributedMatrix ={
    val va = ma.values
    val colIdxA = ma.rowIndices
    val rowPtrA = ma.colPtrs

    val vb = mb.values
    val rowIdxB = mb.rowIndices
    val colPtrB = mb.colPtrs

    val vc = new ArrayBuffer[Double]()
    val rowIdxC = new ArrayBuffer[Int]()
    val colPtrC = new Array[Int](mb.numCols + 1)
    val coltmp = new Array[Double](ma.numRows)

    for (jb <- 0 until colPtrB.length -1){
      val num = colPtrB(jb + 1) - colPtrB(jb)
      val entryColB = new scala.collection.mutable.HashMap[Int, Double]()
      for(ib <- colPtrB(jb) until colPtrB(jb) + num){
        entryColB.put(rowIdxB(ib), vb(ib))
      }
      arrayClear(coltmp)

      for(ia <- 0 until rowPtrA.length - 1) {
        val count = rowPtrA(ia + 1) - rowPtrA(ia)
        for(ja <- rowPtrA(ia) until rowPtrA(ia) + count) {
          if (entryColB.contains(colIdxA(ja))) {
            coltmp(ia) += va(ja) * entryColB.get(colIdxA(ja)).get
          }
        }
      }
      var count = 0
      for (i <- 0 until coltmp.length){
        if (coltmp(i) != 0.0) {
          count += 1
          vc += coltmp(i)
          rowIdxC += i
        }
      }

      colPtrC(jb + 1) = colPtrC(jb) + count
    }
    if(ma.numRows * mb.numCols > 2 * colPtrC(colPtrC.length - 1) + colPtrC.length){
      new SparseMatrix(ma.numRows, mb.numCols, colPtrC, rowIdxC.toArray, vc.toArray)
    } else{
      new SparseMatrix(ma.numRows, mb.numCols, colPtrC, rowIdxC.toArray, vc.toArray).toDense
    }
  }

  private def multiplyCSC_CSR(ma: SparseMatrix, mb: SparseMatrix): DistributedMatrix = {
    val buf = new Array[Double](ma.numRows * mb.numCols)
    val n = ma.numRows
    val va = ma.values
    val rowIdxA = ma.rowIndices
    val colPtrA = ma.colPtrs

    val vb = mb.values
    val colIdxB = mb.rowIndices
    val rowPtrB = mb.colPtrs

    for(ja <- 0 until colPtrA.length -1){
      val countA = colPtrA(ja + 1) - colPtrA(ja)
      val rowValMap = new mutable.HashMap[Int, Double]()
      for (ia <- colPtrA(ja) until colPtrA(ja) + countA){
        rowValMap.put(rowIdxA(ia), va(ia))
      }

      val countB = rowPtrB(ja + 1) - rowPtrB(ja)
      for(jb <- rowPtrB(ja) until rowPtrB(ja) + countB) {
        for (elem <- rowValMap) {
          val i = elem._1
          val j = colIdxB(jb)
          buf(i + j * n) += elem._2 * vb(jb)
        }
      }
    }
    val nnz = buf.count(x => x != 0.0)
    if(ma.numRows * mb.numCols <= 2 * nnz + mb.numCols) {
      new DenseMatrix(ma.numRows, mb.numCols, buf)
    } else{
      new DenseMatrix(ma.numRows, mb.numCols, buf).toSparse
    }
  }

  private def binSearch(arr: Array[Int], target: Int): Int = {
    var (lo, hi) = (0, arr.length - 2)
    while (lo <= hi) {
      val mid = lo + (hi - lo) / 2
      if (arr(mid) == target || (arr(mid) < target && target < arr(mid + 1))) {
        return mid
      }
      else if (arr(mid) > target) {
        hi = mid - 1
      }
      else {
        lo = mid + 1
      }
    }
    throw new SparkException("binary search failed for CSR2CSC/CSC2CSR conversion!")
  }

  def csr2csc(sp: SparseMatrix): (Array[Int], Array[Int], Array[Double]) = {
    require(sp.isTransposed, s" Sparse matrix needs to set true for isTransposed bit, but found sp.isTransposed = ${sp.isTransposed}")

    val vcsr = sp.values
    val colIdxCSR = sp.rowIndices
    val rowPtrCSR = sp.colPtrs

    val values = new Array[Double](sp.values.length)
    val rowIdx = new Array[Int](sp.values.length)
    val colPtr = new Array[Int](sp.numCols + 1)

    val colMap = scala.collection.mutable.LinkedHashMap[Int, ArrayBuffer[Int]]()

    for(i <- 0 until colIdxCSR.length) {
      val elem = colMap.getOrElse(colIdxCSR(i), new ArrayBuffer[Int]())
      elem += i
      colMap(colIdxCSR(i)) = elem
    }

    var idx = 0
    for (c <- 0 until colPtr.length -1) {
      colPtr(c+1) = colPtr(c) + colMap(c).length
      for(id <- colMap(c)){
        values(idx) = vcsr(id)
        rowIdx(idx) = binSearch(rowPtrCSR, id)
        idx += 1
      }
    }
    (rowIdx, colPtr, values)
  }

  def csc2csr(sp: SparseMatrix): (Array[Int], Array[Int], Array[Double]) = {
    require(!sp.isTransposed, s"Sparse matrix needs to set false for isTransposed bit, but found sp.isTransposed = ${sp.isTransposed}")

    val vcsc = sp.values
    val rowIdxCSC = sp.rowIndices
    val colPtrCSC = sp.colPtrs

    val values = new Array[Double](vcsc.length)
    val colIdx = new Array[Int](vcsc.length)
    val rowPtr = new Array[Int](sp.numRows + 1)

    val rowMap = scala.collection.mutable.LinkedHashMap[Int, ArrayBuffer[Int]]()

    for(i <- 0 until rowIdxCSC.length) {
      val elem = rowMap.getOrElse(rowIdxCSC(i), new ArrayBuffer[Int]())
      elem += i
      rowMap(rowIdxCSC(i)) = elem
    }

    var idx = 0
    for (r <- 0 until rowPtr.length - 1) {
      rowPtr(r + 1) = rowPtr(r) + rowMap(r).length
      for (id <- rowMap(r)) {
        values(idx) = vcsc(id)
        colIdx(idx) = binSearch(colPtrCSC, id)
        idx += 1
      }
    }
    (rowPtr, colIdx, values)
  }

  def multiplyScalar(alpha: Double, a: DistributedMatrix): DistributedMatrix ={
    a match{
      case ma: DenseMatrix => multiplyScalarDense(alpha, ma)
      case ma: SparseMatrix => multiplyScalarSparse(alpha, ma)
    }
  }

  private def multiplyScalarDense(alpha: Double, ma: DenseMatrix): DistributedMatrix ={
    val arr = for (elem <- ma.values) yield alpha * elem
    new DenseMatrix(ma.numRows, ma.numCols, arr, ma.isTransposed)
  }

  private def multiplyScalarSparse(alpha: Double, ma: SparseMatrix): DistributedMatrix ={
    val arr = for (elem <- ma.values) yield  alpha * elem
    new SparseMatrix(ma.numRows, ma.numCols, ma.colPtrs, ma.rowIndices, arr, ma.isTransposed)
  }

  def toBreeze(mat: DistributedMatrix): BM[Double] ={
    mat match {
      case dm: DenseMatrix => denseToBreeze(dm)
      case sm: SparseMatrix => sparseToBreeze(sm)
    }
  }

  private def denseToBreeze(mat: DenseMatrix): BM[Double]={
    if(!mat.isTransposed){
      new BDM[Double](mat.numRows, mat.numCols, mat.values)
    } else{
      val breezeMatrix = new BDM[Double](mat.numRows, mat.numCols, mat.values)
      breezeMatrix.t
    }
  }

  private def sparseToBreeze(mat: SparseMatrix): BM[Double] ={
    if(!mat.isTransposed){
      new BSM[Double](mat.values, mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices)
    } else{
      val breezeMatrix = new BSM[Double](mat.values, mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices)
      breezeMatrix.t
    }
  }

  def fromBreeze(breeze: BM[Double]): DistributedMatrix ={
    breeze match{
      case dm: BDM[Double] => new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sm: BSM[Double] => new SparseMatrix(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
      case _ => throw new UnsupportedOperationException(s"Don't support conversion from type ${breeze.getClass.getName}")
    }
  }

  def elementWiseMultiply(a: DistributedMatrix, b: DistributedMatrix): DistributedMatrix ={
    require(a.numRows == b.numRows, s"a.numRows= ${a.numRows}, b.numRows = ${b.numRows}")
    require(a.numCols == b.numCols, s"a.numCols= ${a.numCols}, b.numCols = ${b.numCols}")

    (a, b) match {
      case (ma: DenseMatrix, mb: DenseMatrix) => elementWiseOpDenseDense(ma, mb)
      case (ma: DenseMatrix, mb: SparseMatrix) => elementWiseOpDenseSparse(ma, mb)
      case (ma: SparseMatrix, mb: DenseMatrix) => elementWiseOpDenseSparse(mb, ma)
      case (ma: SparseMatrix, mb: SparseMatrix) => elementWiseOpSparseSparse(ma, mb)
    }
  }

  def elementWiseDivide(a: DistributedMatrix, b: DistributedMatrix): DistributedMatrix ={
    require(a.numRows == b.numRows, s"a.numRows= ${a.numRows}, b.numRows = ${b.numRows}")
    require(a.numCols == b.numCols, s"a.numCols= ${a.numCols}, b.numCols = ${b.numCols}")

    (a, b) match {
      case (ma: DenseMatrix, mb: DenseMatrix) => elementWiseOpDenseDense(ma, mb, 1)
      case (ma: DenseMatrix, mb: SparseMatrix) => elementWiseOpDenseSparse(ma, mb, 1)
      case (ma: SparseMatrix, mb: DenseMatrix) => elementWiseOpDenseSparse(mb, ma, 1)
      case (ma: SparseMatrix, mb: SparseMatrix) => elementWiseOpSparseSparse(ma, mb, 1)
    }
  }

  // op = 0 -- multiplication, op = 1 -- division
  private def elementWiseOpDenseDense(ma: DenseMatrix, mb: DenseMatrix, op: Int = 0) ={
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)

    for(i <- 0 until arr.length){
      if(op == 0)
        arr(i) = arr1(i) * arr2(i)
      else
        arr(i) = arr1(i) / arr2(i)
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  private def elementWiseOpDenseSparse(ma: DenseMatrix, mb: SparseMatrix, op: Int = 0) ={
    val (arr1, arr2) = (ma.toArray, mb.toArray)
    val arr = Array.fill(arr1.length)(0.0)
    for(i <- 0 until arr.length){
      if(op == 0)
        arr(i) = arr1(i) * arr2(i)
      else
        arr(i) = arr1(i) / arr2(i)
    }
    new DenseMatrix(ma.numRows, ma.numCols, arr)
  }

  private def elementWiseOpSparseSparse(ma: SparseMatrix, mb: SparseMatrix, op: Int = 0) ={
    if (ma.isTransposed || mb.isTransposed){
      val (arr1, arr2) = (ma.toArray, mb.toArray)
      val arr = Array.fill(arr1.length)(0.0)
      var nnz = 0

      for (i <- 0 until arr.length){
        if(op == 0)
          arr(i) = arr1(i) * arr2(i)
        else
          arr(i) = arr1(i) / arr2(i)
        if(arr(i) != 0) nnz += 1
      }

      val c = new DenseMatrix(ma.numRows, ma.numCols, arr)
      if (c.numRows * c.numCols > nnz * 2 + c.numCols + 1){
        c.toSparse
      } else {
        c
      }
    } else {
      elementWiseOpSparseSparseNative(ma, mb, op)
    }
  }

  def elementWiseOpSparseSparseNative(a: SparseMatrix, b: SparseMatrix, op: Int = 0) : DistributedMatrix ={
    require(a.numRows == b.numRows, s"a.numRows= ${a.numRows}, b.numRows = ${b.numRows}")
    require(a.numCols == b.numCols, s"a.numCols= ${a.numCols}, b.numCols = ${b.numCols}")

    val va = a.values
    val rowIdxA = a.rowIndices
    val colPtrA = a.colPtrs

    val vb = b.values
    val rowIdxB = b.rowIndices
    val colPtrB = b.colPtrs

    val vc = ArrayBuffer[Double]()
    val rowIdxC = ArrayBuffer[Int]()
    val colPtrC = new Array[Int](a.numCols + 1)
    val coltmp1 = new Array[Double](a.numRows)
    val coltmp2 = new Array[Double](b.numRows)

    for(jc <- 0 until a.numCols){
      val numPerColB = colPtrB(jc + 1) - colPtrB(jc)
      val numPerColA = colPtrA(jc + 1) - colPtrA(jc)

      arrayClear(coltmp1)
      arrayClear(coltmp2)

      for (ia <- colPtrA(jc) until colPtrA(jc) + numPerColA){
        coltmp1(rowIdxA(ia)) += va(ia)
      }
      for (ib <- colPtrB(jc) until colPtrB(jc) + numPerColB){
        coltmp2(rowIdxB(ib)) += vb(ib)
      }

      for (ix <- 0 until coltmp1.length){
        if(op == 0)
          coltmp1(ix) *= coltmp2(ix)
        else
          coltmp1(ix) /= coltmp2(ix)
      }
      var count = 0
      for (i <- 0 until coltmp1.length) {
        if (coltmp1(i) != 0.0){
          rowIdxC += i
          vc += coltmp1(i)
          count += 1
        }
      }
      colPtrC(jc + 1) = colPtrC(jc) + count
    }
    if(a.numRows * a.numCols > 2 * colPtrC(colPtrC.length - 1) + colPtrC.length){
      new SparseMatrix(a.numRows, a.numCols, colPtrC, rowIdxC.toArray, vc.toArray)
    } else{
      new SparseMatrix(a.numRows, a.numCols, colPtrC, rowIdxC.toArray, vc.toArray).toDense
    }
  }

  // C = C + A * B, and C is pre-allocated already
  // this method should save memory when C is reused for later iterations
  // cannot handle the case when C is a sparse matrix and holding a non-exsitent
  // entry for the product

  def incrementalMultiply(a: DistributedMatrix, b:DistributedMatrix, c: DistributedMatrix): DistributedMatrix ={
    require(a.numRows == c.numRows && b.numCols == c.numCols, s"dimension mismatch a.numRows = ${a.numRows}, C.numRows = ${c.numRows}, b.numCols = ${b.numCols}, c.numCols = ${c.numCols}")

    (a, b, c) match {
      case (ma: DenseMatrix, mb: DenseMatrix, mc: DenseMatrix) => ma.inplacemultiply(mb, mc)
      case (ma: DenseMatrix, mb: SparseMatrix, mc: DenseMatrix) => incrementalMultiplyDenseSparse(ma, mb, mc)
      case (ma: SparseMatrix, mb: DenseMatrix, mc: DenseMatrix) => incrementalMultiplySparseDense(ma, mb, mc)
      case (ma: SparseMatrix, mb: SparseMatrix, mc: DenseMatrix) => incrementalMultiplySparseSparse(ma, mb, mc)
      case _ =>
        new SparkException(s"incrementalMultiply does not apply to a: ${a.getClass}, b: ${b.getClass}, c: ${c.getClass}")
    }
    c
  }

  private def incrementalMultiplyDenseDense(a: DenseMatrix, b: DenseMatrix, c: DenseMatrix) ={
    for (i <- 0 until a.numRows){
      for (j <- 0 until b.numCols) {
        for(k <- 0 until a.numCols) {
          var v1 = 0.0
          if (a.isTransposed) v1 = a(k, i)
          else v1 = a(i, k)
          var v2 = 0.0
          if (b.isTransposed) v2 = b(j, k)
          else v2 = b(k, j)
          if(!c.isTransposed) c(i, j) += v1 * v2
          else c(j, i) += v1 * v2
        }
      }
    }
  }

  private def incrementalMultiplyDenseSparse(a: DenseMatrix, b: SparseMatrix, c: DenseMatrix) = {
    if(b.isTransposed){
      val res = csr2csc(b)
      val rowIdx = res._1
      val colPtr = res._2
      val values = res._3

      for(ic <- 0 until b.numCols) {
        val count = colPtr(ic + 1) - colPtr(ic)
        val cstart = colPtr(ic)
        for (ridx <- cstart until cstart + count){
          val v = values(ridx)
          val r = rowIdx(ridx)

          for (i <- 0 until a.numRows) {
            var va = 0.0
            if(a.isTransposed) va = a(r, i)
            else va = a(i, r)
            if(!c.isTransposed) c(i, ic) += va * v
            else c(ic, i) += va * v
          }

        }
      }
    }else {
      val values: Array[Double] = b.values
      val rowIdx: Array[Int] = b.rowIndices
      val colPtr: Array[Int] = b.colPtrs

      for(ic <- 0 until b.numCols) {
        val count = colPtr(ic + 1) - colPtr(ic)
        val cstart = colPtr(ic)
        for (ridx <- cstart until cstart + count){
          val v = values(ridx)
          val r = rowIdx(ridx)

          for (i <- 0 until a.numRows) {
            var va = 0.0
            if (a.isTransposed) va = a(r, i)
            else va = a(i, r)
            if(!c.isTransposed) c(i, ic) += va * v
            else c(ic, i) += va * v
          }

        }
      }
    }
  }

  private def incrementalMultiplySparseDense(a: SparseMatrix, b: DenseMatrix, c: DenseMatrix) ={
    if(!a.isTransposed){
      val res = csc2csr(a)
      val rowPtr = res._1
      val colIdx = res._2
      val values = res._3

      for(r <- 0 until a.numRows){
        val count = rowPtr(r + 1) - rowPtr(r)
        val rstart = rowPtr(r)
        for (cidx <- rstart until rstart + count){
          val v = values(cidx)
          val ci = colIdx(cidx)

          for (j <- 0 until b.numCols) {
            var vb = 0.0
            if (b.isTransposed) vb = b(j, ci)
            else vb = b(ci, j)
            if (!c.isTransposed) c(r, j) += v * vb
            else c(j, ci) += v * vb
          }
        }
      }
    } else {
      val values: Array[Double] = a.values
      val colIdx: Array[Int] = a.rowIndices
      val rowPtr: Array[Int] = a.colPtrs

      for(r <- 0 until a.numRows){
        val count = rowPtr(r + 1) - rowPtr(r)
        val rstart = rowPtr(r)
        for (cidx <- rstart until rstart + count){
          val v = values(cidx)
          val ci = colIdx(cidx)

          for (j <- 0 until b.numCols) {
            var vb = 0.0
            if (b.isTransposed) vb = b(j, ci)
            else vb = b(ci, j)
            if (!c.isTransposed) c(r, j) += v * vb
            else c(j, ci) += v * vb
          }
        }
      }
    }
  }

  private def incrementalMultiplySparseSparse(a: SparseMatrix, b: SparseMatrix, c: DenseMatrix) ={
    if(!a.isTransposed && !b.isTransposed) {
      val valuesB = b.values
      val rowIdxB = b.rowIndices
      val colPtrB = b.colPtrs

      val valuesA = a.values
      val rowIdxA = a.rowIndices
      val colPtrA = a.colPtrs

      for (cb <- 0 until b.numCols) {
        val countB = colPtrB(cb + 1) - colPtrB(cb)
        val startB = colPtrB(cb)

        for(ridxb <- startB until startB + countB){
          val vb = valuesB(ridxb)
          val rowB = rowIdxB(ridxb)

          val countA = colPtrA(rowB + 1) - colPtrA(rowB)
          val startA = colPtrA(rowB)
          for (ridxa <- startA until startA + countA){
            val va = valuesA(ridxa)
            val rowA = rowIdxA(ridxa)
            if(!c.isTransposed) c(rowA, cb) += va * vb
            else c(cb, rowA) += va * vb
          }
        }
      }
    } else if (a.isTransposed && b.isTransposed){
      val valuesA = a.values
      val colIdxA = a.rowIndices
      val rowPtrA = a.colPtrs

      val valuesB = b.values
      val colIdxB = b.rowIndices
      val rowPtrB = b.colPtrs

      for(ra <- 0 until a.numRows){
        val countA = rowPtrA(ra + 1) - rowPtrA(ra)
        val startA = rowPtrA(ra)
        for(cidxa <- startA until startA + countA){
          val va = valuesA(cidxa)
          val colA = colIdxA(cidxa)

          val countB = rowPtrA(colA + 1) - rowPtrB(colA)
          val startB = rowPtrB(colA)

          for(cidxb <- startB until startB + countB) {
            val vb = valuesB(cidxb)
            val colB = colIdxB(cidxb)

            if(!c.isTransposed) c(ra, colB) += va * vb
            else c(colB, ra) += va * vb
          }
        }
      }
    } else if (a.isTransposed && !b.isTransposed){
      val valuesA = a.values
      val colIdxA = a.rowIndices
      val rowPtrA = a.colPtrs

      val valuesB = b.values
      val rowIdxB = b.rowIndices
      val colPtrB = b.colPtrs

      for(ra <- 0 until a.numRows){
        val countA = rowPtrA(ra + 1) - rowPtrA(ra)
        val startA = rowPtrA(ra)
        val entryA = ArrayBuffer[(Int, Double)]()

        for(cidxA <- startA until startA + countA) {
          entryA.append((colIdxA(cidxA), valuesA(cidxA)))
        }
        for(cb <- 0 until b.numCols){
          val countB = colPtrB(cb + 1) - colPtrB(cb)
          val startB = colPtrB(cb)
          val entryB = ArrayBuffer[(Int, Double)]()

          for(ridxB <- startB until startB + countB){
            entryB.append((rowIdxB(ridxB), valuesB(ridxB)))
          }

          var (i, j) = (0, 0)
          while(i < entryA.length && j < entryB.length) {
            if(entryA(i)._1 == entryB(j)._1){
              if(!c.isTransposed) {
                c(ra, cb) += entryA(i)._2 * entryB(j)._2
              } else {
                c(cb, ra) += entryA(i)._2 * entryB(j)._2
              }
              i += 1
              j += 1
            }
            else if (entryA(i)._1 > entryB(j)._1) j += 1
            else i += 1
          }
        }

      }
    } else {
      val valuesA = a.values
      val rowIdxA = a.rowIndices
      val colPtrA = a.colPtrs

      val valuesB = b.values
      val colIdxB = b.rowIndices
      val rowPtrB = b.colPtrs

      for (idx <- 0 until a.numCols){
        val countA = colPtrA(idx + 1) - colPtrA(idx)
        val startA = colPtrA(idx)
        val entryA = ArrayBuffer[(Int, Double)]()
        for(ridx <- startA until startA + countA){
          entryA.append((rowIdxA(ridx), valuesA(ridx)))
        }

        val countB = rowPtrB(idx + 1) - rowPtrB(idx)
        val startB = rowPtrB(idx)
        val entryB = ArrayBuffer[(Int, Double)]()
        for(cidx <- startB until startB + countB){
          entryB.append((colIdxB(cidx), valuesB(cidx)))
        }
        for(t1 <- entryA)
          for(t2 <- entryB)
            if(!c.isTransposed) c(t1._1, t2._1) += t1._2 * t2._2
            else  c(t2._1, t1._1) += t1._2 * t2._2

      }
    }
  }

  def multiplySparseMatDenseVec(sp: SparseMatrix, dv: DenseMatrix): DenseMatrix ={
    require(dv.numCols == 1, s"vector with more than 1 columns, dv.numCols = ${dv.numCols}")
    require(sp.numCols == dv.numRows, s"Sparse Matrix - Vector dimensions don't match, Matrix columns = ${sp.numCols}, Vector rows = ${dv.numRows}")

    val arr = new Array[Double](sp.numRows)
    if(sp.isTransposed){
      val values = sp.values
      val colIdx = sp.rowIndices
      val rowPtr = sp.colPtrs
      for (ridx <- 0 until arr.length){
        val count = rowPtr(ridx +1) - rowPtr(ridx)
        val start = rowPtr(ridx)
        for (cidx <- start until start + count){
          arr(ridx) += values(cidx) * dv(colIdx(cidx), 0)
        }
      }
      new DenseMatrix(sp.numRows, 1, arr)
    }
    else {
      val values = sp.values
      val rowIdx = sp.rowIndices
      val colPtr = sp.colPtrs
      for (cidx <- 0 until sp.numCols){
        val count = colPtr(cidx + 1) - colPtr(cidx)
        val start = colPtr(cidx)
        for(ridx <- start until start + count){
          arr(rowIdx(ridx)) += values(ridx) * dv(cidx, 0)
        }
      }
      new DenseMatrix(sp.numRows, 1, arr)
    }
  }

  def matrixMultiplication(a: DistributedMatrix, b: DistributedMatrix): DistributedMatrix ={
    (a, b) match {
      case (ma: DenseMatrix, mb: DenseMatrix) => ma.multiply(mb)
      case (ma: DenseMatrix, mb: SparseMatrix) => ma.multiply(mb.toDense)
      case (ma: SparseMatrix, mb: DenseMatrix) =>
        if(mb.numCols == 1){
          Block.multiplySparseMatDenseVec(ma, mb)
        } else {
          ma.multiply(mb)
        }
      case (ma: SparseMatrix, mb: SparseMatrix) =>
        val sparsityA = ma.values.length * 1.0 / (ma.numRows * ma.numCols)
        val sparsityB = mb.values.length * 1.0 / (mb.numRows * mb.numCols)

        if(sparsityA > 0.1){
          ma.toDense.multiply(mb.toDense)
        } else if (sparsityB > 0.1) {
          ma.multiply(mb.toDense)
        } else{
          Block.multiplySparseSparse(ma, mb)
        }
      case _ => throw new SparkException(s"Unsupported matrix type ${a.getClass.getName}")

    }
  }

  def addScalar(mat: DistributedMatrix, alpha: Double): DistributedMatrix ={
    mat match {
      case ds: DenseMatrix =>
        val arr = ds.values.clone()
        for (i <- 0 until arr.length){
          arr(i) = ds.values(i) + alpha
        }
        new DenseMatrix(ds.numRows, ds.numCols, arr, ds.isTransposed)
      case sp: SparseMatrix =>
        val arr = sp.values.clone()
        for(i <- 0 until arr.length){
          arr(i) = sp.values(i) + alpha
        }
        new SparseMatrix(sp.numRows, sp.numCols, sp.colPtrs, sp.rowIndices, arr, sp.isTransposed)
    }
  }

  def matrixDivideVector(mat: DistributedMatrix, vec: DistributedMatrix): DistributedMatrix = {
    require(mat.numCols == vec.numRows, s"Dimension error for matrix divide vector, mat.numCols = ${mat.numCols}, vec.numRows = ${vec.numRows}")
    mat match {
      case dm: DenseMatrix =>
        val arr = dm.values.clone()
        val div = vec.toArray
        val n = dm.numRows
        for (i <- 0 until arr.length) {
          if (div(i/n) != 0){
            arr(i) = arr(i) / div(i/n)
          }else{
            arr(i) = 0.0
          }
        }
        new DenseMatrix(mat.numRows, mat.numCols, arr)
      case sm: SparseMatrix =>
        val arr = sm.values.clone()
        val div = vec.toArray
        val rowIdx = sm.rowIndices
        val colPtr = sm.colPtrs
        for(cid <- 0 until colPtr.length -1) {
          val count = colPtr(cid + 1) - colPtr(cid)
          for(i <- 0 until count) {
            val idx = colPtr(cid) + i
            if (div(cid) != 0) arr(idx) = arr(idx) / div(cid)
            else arr(idx) = 0.0
          }
        }
        new SparseMatrix(sm.numRows, sm.numCols, colPtr, rowIdx, arr)
      case _ => throw new SparkException("matrix format not recognized!")
    }
  }
}

object TestSparse{
  def main (args: Array[String]): Unit = {
    val va = Array[Double](1, 2, 3, 4, 5, 6)
    val rowIdxa = Array[Int](0, 2, 1, 0, 1, 2)
    val colPtra = Array[Int](0, 2, 3, 6)
    val vb = Array[Double](3, 1, 2, 2)
    val rowIdxb = Array[Int](1, 0, 2, 0)
    val colPtrb = Array[Int](0, 1, 3, 4)
    val spmat1 = new SparseMatrix(3, 3, colPtra, rowIdxa, va)
    val spmat2 = new SparseMatrix(3, 3, colPtrb, rowIdxb, vb)

    println(spmat1)
    println("-" * 20)
    println(spmat2)
    println("-" * 20)
    println(Block.multiplySparseSparse(spmat1, spmat2))
    println("-" * 20)
    println(Block.elementWiseMultiply(spmat1, spmat2))
    println("-" * 20)
    println(Block.add(spmat1, spmat2))
    println("-" * 20)


    val vd1 = Array[Double](1, 4, 7, 2, 5, 8, 3, 6, 9)
    val vd2 = Array[Double](1, 2, 3, 1, 2, 3, 1, 2, 3)
    val den1 = new DenseMatrix(3, 3, vd1)
    val den2 = new DenseMatrix(3, 3, vd2)
    val denC = DenseMatrix.zeros(3, 3)
    val denV = new DenseMatrix(3, 1, Array[Double](1, 2, 3))
    Block.incrementalMultiply(spmat2, spmat2.transpose, denC)


    println(denC)
    println("-" * 20)
    println(Block.multiplySparseMatDenseVec(spmat1, denV))
    println("-" * 20)
    println(Block.multiplySparseSparse(spmat1, spmat2.transpose))
    println(spmat1)
    println(Block.matrixDivideVector(den2, denV))
  }
}
