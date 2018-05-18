package org.apache.spark.sql.me.matrix

import java.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.me.Serializer.DMatrixSerializer
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.{linalg => newlinalg}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder, HashSet}


@SQLUserDefinedType(udt=classOf[MatrixUDT])
abstract class DistributedMatrix extends Serializable {
  def numRows: Int
  def numCols: Int

  val isTransposed: Boolean = false

  def toArray: Array[Double] = {
    val newArray = new Array[Double](numRows * numCols)
    foreachActive{ (i, j, v) =>
      newArray(j * numRows + i) = v
    }
    newArray
  }

  def colIter: Iterator[Vector]

  def rowIter: Iterator[Vector] = this.transpose.colIter

  def asBreeze: BM[Double]

  def apply(i: Int, j: Int): Double

  def index(i: Int, j: Int): Int

  def update(i: Int, j: Int, v: Double): Unit

  def copy: DistributedMatrix

  def transpose: DistributedMatrix

  def multiply(y: DenseMatrix): DenseMatrix ={
    val C: DenseMatrix = DenseMatrix.zeros(numRows, y.numCols)
    BLAS.gemm(1.0, this, y, 0.0, C)
    C
  }

  def multiply(y:DenseVector): DenseVector ={
    multiply(y.asInstanceOf[Vector])
  }

  def multiply(y: Vector): DenseVector ={
    val output = new DenseVector(new Array[Double](numRows))
    BLAS.gemv(1.0, this, y, 0.0, output)
    output
  }

  override def toString: String = asBreeze.toString()

  def map(f: Double => Double): DistributedMatrix

  def update(f: Double => Double): DistributedMatrix

  def foreachActive(function: (Int, Int, Double) => Unit)

  def numNonzeros: Int

  def numActives: Int

  def asML: newlinalg.Matrix
}


private[me] class MatrixUDT extends UserDefinedType[DistributedMatrix] {
  override def sqlType: StructType ={
    /*
     type: 0 = sparse, 1 = dense
     the dense matrix is built by numRows, numCols, values and isTransposed, all of which are
     set as not nullable, except values since in the future, support for binary matrices might
     be added for which values are not needed.
     the sparse matrix needs colPtrs and rowIndices, which are set as
     null, while building the dense matrix.
    */
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("numRows", IntegerType, nullable = false),
      StructField("numCols", IntegerType, nullable = false),
      StructField("colPtrs", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("rowIndices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true),
      StructField("isTransposed", BooleanType, nullable = false)
    ))
  }

  override def serialize(obj: DistributedMatrix): InternalRow ={
    DMatrixSerializer.serialize(obj)
  }

  override def deserialize(datum: Any): DistributedMatrix ={
    DMatrixSerializer.deserialize(datum)
  }

  override def userClass: Class[DistributedMatrix] = classOf[DistributedMatrix]

  override def equals(other: Any): Boolean = {
    other match {
      case v:MatrixUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[MatrixUDT].getName.hashCode

  override def typeName: String = "DistributedMatrix"

  override def pyUDT: String = "pyspark.mllib.linalg.MatrixUDT"

  override def asNullable: UserDefinedType[DistributedMatrix] = this
}

case object MatrixUDT extends MatrixUDT





@SQLUserDefinedType(udt=classOf[MatrixUDT])
case class DenseMatrix(override val numRows: Int,
                       override val numCols: Int,
                       val values: Array[Double],
                       override val isTransposed: Boolean) extends DistributedMatrix{

  def this(numRows: Int, numCols: Int, values: Array[Double]) = this(numRows, numCols, values, false)

  override def equals(obj: scala.Any): Boolean = obj match{
    case m: DistributedMatrix => asBreeze == m.asBreeze
    case _ => false
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(numRows: Integer, numCols: Integer, toArray)
  }

  override def asBreeze: BM[Double] = {
    if(!isTransposed){
      new BDM[Double](numRows, numCols, values)
    } else{
      val breezeMatrix = new BDM[Double](numCols, numRows, values)
      breezeMatrix.t
    }
  }

  private def apply(i: Int): Double = values(i)

  override def apply(i: Int, j: Int): Double = values(index(i, j))

  override def index(i: Int, j: Int): Int = {
    require(i >= 0 && i < numRows, s"Expected 0 <= i < $numRows, got i = $i.")
    require(j >= 0 && j < numCols, s"Expected 0 <= j < $numCols, got j = $j.")
    if(!isTransposed) i + numRows * j else j + numCols * i
  }

  override def update(i: Int, j: Int, v: Double): Unit ={
    values(index(i, j)) = v
  }

  override def copy: DenseMatrix = new DenseMatrix(numRows, numCols, values.clone())

  override def map(f: Double => Double): DistributedMatrix = new DenseMatrix(numRows, numCols, values.map(f), isTransposed)

  override def update(f: Double => Double): DistributedMatrix = {
    val len = values.length
    var i = 0
    while (i < len){
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  override def transpose: DistributedMatrix = new DenseMatrix(numCols, numRows, values, !isTransposed)

  override def foreachActive(function: (Int, Int, Double) => Unit): Unit = {
    if(!isTransposed){
      var j = 0
      while (j < numCols){
        var i = 0
        val indStart = j * numRows
        while(i < numRows){
          function(i, j, values(indStart + i))
          i += 1
        }
        j += 1
      }
    } else{
      var i = 0
      while(i < numRows){
        var j = 0
        val indStart = i * numCols
        while(j < numCols){
          function(i, j, values(indStart + j))
          j += 1
        }
        i += 1
      }
    }
  }

  override def numNonzeros: Int = values.count(_ != 0)

  override def numActives: Int = values.length

  def toSparse: SparseMatrix ={
    val spVals: mutable.ArrayBuilder[Double] = new mutable.ArrayBuilder.ofDouble
    val colPtrs: Array[Int] = new Array[Int](numCols + 1)
    val rowIndices: mutable.ArrayBuilder[Int] = new mutable.ArrayBuilder.ofInt
    var nnz = 0
    var j = 0
    while(j < numCols){
      var i = 0
      while(i < numRows){
        val v = values(index(i, j))
        if(v != 0.0){
          rowIndices += i
          spVals += v
          nnz += 1
        }
        i += 1
      }
      j += 1
      colPtrs(j) = nnz
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), spVals.result())
  }

  override def colIter: Iterator[Vector] ={
    if(isTransposed){
      Iterator.tabulate(numCols) { j =>
        val col = new Array[Double](numRows)
        blas.dcopy(numRows, values, j, numCols, col, 0, 1)
        new DenseVector(col)
      }
    } else{
      Iterator.tabulate(numCols) { j =>
        new DenseVector(values.slice(j * numRows, (j + 1) * numRows))
      }
    }
  }

  override def asML: newlinalg.DenseMatrix ={
    new newlinalg.DenseMatrix(numRows, numCols, values, isTransposed)
  }
}

object DenseMatrix {
  def zeros(numRows: Int, numCols: Int): DenseMatrix ={
    require(numRows.toLong * numCols <= Int.MaxValue, s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, new Array[Double](numRows * numCols))
  }

  def ones(numRows: Int, numCols: Int): DenseMatrix ={
    require(numRows.toLong * numCols <= Int.MaxValue, s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows* numCols)(1.0))
  }

  def eye(n: Int): DenseMatrix={
    val identity = DenseMatrix.zeros(n, n)
    var i = 0
    while (i < n) {
      identity.update(i, i, 1.0)
      i += 1
    }
    identity
  }

  def rand(numRows: Int, numCols: Int, rng: java.util.Random): DenseMatrix ={
    require(numRows.toLong * numCols <= Int.MaxValue, s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextDouble()))
  }

  def randn(numRows: Int, numCols: Int, rng: java.util.Random): DenseMatrix ={
    require(numRows.toLong * numCols <= Int.MaxValue, s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextGaussian()))
  }

  def diag(vector: Vector): DenseMatrix ={
    val n = vector.size
    val matrix = DenseMatrix.zeros(n, n)
    val valuse = vector.toArray
    var i = 0
    while (i < n){
      matrix.update(i, i, valuse(i))
      i += 1
    }
    matrix
  }

  def fromML(m: newlinalg.DenseMatrix): DenseMatrix ={
    new DenseMatrix(m.numRows, m.numCols, m.values, m.isTransposed)
  }
}








@SQLUserDefinedType(udt = classOf[MatrixUDT])
case class SparseMatrix (override val numRows: Int,
                         override val numCols: Int,
                         val colPtrs: Array[Int],
                         val rowIndices: Array[Int],
                         val values: Array[Double],
                         override val isTransposed: Boolean) extends DistributedMatrix{

  require(values.length == rowIndices.length, s"The number of row indices and values don't match! valuse.length: ${values.length}, rowIndices.length: ${rowIndices.length}")
  if(isTransposed){
    require(colPtrs.length == numRows + 1, s"Expecting ${numRows + 1} colPtrs when numRows = $numRows but got ${colPtrs.length}")
  } else{
    require(colPtrs.length == numCols + 1, s"Expecting ${numCols + 1} colPtrs when numCols = $numCols but got ${colPtrs.length}")
  }
  require(values.length == colPtrs.last, "The last value of colPtrs must equal the number of elements. values.length: ${values.length}, colPtrs.last: ${colPtrs.last}")

  def this(
          numRows: Int,
          numCols: Int,
          colPtrs: Array[Int],
          rowIndices: Array[Int],
          values: Array[Double]) = this(numRows, numCols, colPtrs, rowIndices, values, false)

  override def apply(i: Int, j: Int): Double = {
    val idx = index(i, j)
    if (idx < 0) 0.0 else values(idx)
  }


  override def hashCode(): Int = asBreeze.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match{
    case m: DistributedMatrix => asBreeze == m.asBreeze
    case _ => false
  }

  override def clone(): AnyRef = super.clone()

  override def finalize(): Unit = super.finalize()

  override def colIter: Iterator[Vector] = {
    if(isTransposed){
      val indicesArray = Array.fill(numCols)(mutable.ArrayBuilder.make[Int])
      val valuesArray = Array.fill(numCols)(mutable.ArrayBuilder.make[Double])
      var i = 0
      while(i < numRows){
        var k = colPtrs(i + 1)
        val rowEnd = colPtrs(i +1)
        while(k < rowEnd){
          val j = rowIndices(k)
          indicesArray(j) += i
          valuesArray(j) += values(k)
          k += 1
        }
        i += 1
      }
      Iterator.tabulate(numCols) { j =>
        val ii = indicesArray(j).result()
        val vv = valuesArray(j).result()
        new SparseVector(numRows, ii, vv)
      }
    } else{
      Iterator.tabulate(numCols) { j =>
        val colStart = colPtrs(j)
        val colEnd = colPtrs(j + 1)
        val ii = rowIndices.slice(colStart, colEnd)
        val vv = values.slice(colStart, colEnd)
        new SparseVector(numRows, ii, vv)
      }
    }
  }

  override def asBreeze: BM[Double] = {
    if(!isTransposed){
      new BSM[Double](values, numRows, numCols, colPtrs, rowIndices)
    } else{
      val breezeMatrix = new BSM[Double](values, numCols, numRows, colPtrs, rowIndices)
      breezeMatrix.t
    }
  }

  override def index(i: Int, j: Int): Int = {
    require(i >= 0 && i < numRows, s"Expected 0 <= i < $numRows, got i = $i.")
    require(j >= 0 && j < numCols, s"Expected 0 <= j < $numCols, got j = $j.")
    if(!isTransposed){
      java.util.Arrays.binarySearch(rowIndices, colPtrs(j), colPtrs(j + 1), i)
    } else{
      java.util.Arrays.binarySearch(rowIndices, colPtrs(i), colPtrs(i + 1), j)
    }
  }

  override def update(i: Int, j: Int, v: Double): Unit = {
    val idx = index(i, j)
    if(idx < 0) {
      throw new NoSuchElementException("The given row and column indices correspond to a zero value. Only non-zero elements in Sparse Matrices can be updated.")
    } else{
      values(idx) = v
    }
  }

  override def copy: SparseMatrix = {
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.clone())
  }

  override def transpose: SparseMatrix = new SparseMatrix(numCols, numRows,colPtrs, rowIndices, values, !isTransposed)

  override def map(f: Double => Double): SparseMatrix = new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.map(f), isTransposed)

  override def update(f: Double => Double): SparseMatrix = {
    val len = values.length
    var i = 0
    while(i < len){
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  override def foreachActive(function: (Int, Int, Double) => Unit): Unit = {
    if(!isTransposed){
      var j = 0
      while(j < numCols){
        var idx = colPtrs(j)
        val idxEnd = colPtrs(j + 1)
        while(idx < idxEnd){
          function(rowIndices(idx), j, values(idx))
          idx += 1
        }
        j += 1
      }
    } else{
      var i = 0
      while(i < numRows){
        var idx = colPtrs(i)
        val idxEnd = colPtrs(i + 1)
        while(idx < idxEnd){
          val j = rowIndices(idx)
          function(i, j, values(idx))
          idx += 1
        }
        i += 1
      }
    }
  }

  def toDense: DenseMatrix ={
    new DenseMatrix(numRows, numCols, toArray)
  }

  override def numNonzeros: Int = values.count(_ != 0)

  override def numActives: Int = values.length

  override def asML: newlinalg.SparseMatrix ={
    new newlinalg.SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
  }
}

object SparseMatrix{
  def fromCOO(numRows: Int, numCols: Int, entries: Iterable[(Int, Int, Double)]): SparseMatrix ={
    val sortedEntries = entries.toSeq.sortBy(v => (v._2, v._1))
    val numEntries = sortedEntries.size
    if(sortedEntries.nonEmpty){
      for(col <- Seq(sortedEntries.head._2, sortedEntries.last._2)){
        require(col >= 0 && col < numCols, s"Column index out of range [0, $numCols): $col.")
      }
    }
    val colPtrs = new Array[Int](numCols + 1)
    val rowIndices = mutable.ArrayBuilder.make[Int]
    rowIndices.sizeHint(numEntries)
    val values = mutable.ArrayBuilder.make[Double]
    values.sizeHint(numEntries)
    var nnz = 0
    var prevCol = 0
    var prevRow = -1
    var prevVal = 0.0

    (sortedEntries.view :+ (numRows, numCols, 1.0)).foreach{ case (i, j, v) =>
      if (v != 0){
        if(i == prevRow && j == prevCol){
          prevVal += v
        } else{
          if(prevVal != 0){
            require(prevRow >= 0 && prevRow < numRows, s"Row index out of range [0, $numRows): $prevRow")
            nnz += 1
            rowIndices += prevRow
            values += prevVal
          }
          prevRow = i
          prevVal = v
          while(prevCol < j) {
            colPtrs(prevCol + 1) = nnz
            prevCol += 1
          }
        }
      }
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), values.result())
  }

  def speye(n: Int): SparseMatrix ={
    new SparseMatrix(n, n, (0 to n).toArray, (0 to n).toArray, Array.fill(n)(1.0))
  }

  private def genRandMatrix(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix = {
    require(numRows > 0, s"numRows must be greater than 0 but got $numRows")
    require(numCols > 0, s"numCols must be greater than 0 but got $numCols")
    require(density >= 0.0 && density <= 1.0, s"density must be a double in the range 0.0 <= d <= 1.0. Currently, density: $density")

    val size = numRows.toLong * numCols
    val expected = size * density
    assert(expected < Int.MaxValue, "The expected number of nonzeros can't be greater than Int.MaxValue")
    val nnz = math.ceil(expected).toInt
    if(density == 0.0){
      new SparseMatrix(numRows, numCols, new Array[Int](numCols + 1), Array[Int](), Array[Double]())
    } else if (density == 1.0){
      val colPtrs = Array.tabulate(numCols + 1)(j => j * numRows)
      val rowIndices = Array.tabulate(size.toInt)(idx => idx % numRows)
      new SparseMatrix(numRows, numCols, colPtrs, rowIndices, new Array[Double](numRows * numCols))
    } else if (density < 0.34){
      val entries = mutable.HashSet[(Int, Int)]()
      while(entries.size < nnz){
        entries += ((rng.nextInt(numRows), rng.nextInt(numCols)))
      }
      SparseMatrix.fromCOO(numRows, numCols, entries.map(v => (v._1, v._2, 1.0)))
    } else{
      var idx = 0L
      var numSelected = 0
      var j = 0
      val colPtrs = new Array[Int](numCols + 1)
      val rowIndices = new Array[Int](nnz)
      while(j < numCols && numSelected < nnz){
        var i = 0
        while(i < numRows && numSelected < nnz){
          if(rng.nextDouble() < 1.0 * (nnz - numSelected) / (size - idx)){
            rowIndices(numSelected) = i
            numSelected += 1
          }
          i += 1
          idx += 1
        }
        colPtrs(j + 1) = numSelected
        j += 1
      }
      new SparseMatrix(numRows, numCols, colPtrs, rowIndices, new Array[Double](nnz))
    }
  }

  def sprand(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix ={
    val mat = genRandMatrix(numRows, numCols, density, rng)
    mat.update(i => rng.nextDouble())
  }

  def sprandn(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix ={
    val mat = genRandMatrix(numRows, numCols, density, rng)
    mat.update(i => rng.nextGaussian())
  }
  def spdiag(vector: Vector): SparseMatrix = {
    val n = vector.size
    vector match {
      case sVec: SparseVector =>
        SparseMatrix.fromCOO(n, n, sVec.indices.zip(sVec.values).map(v => (v._1, v._1, v._2)))
      case dVec: DenseVector =>
        val entries = dVec.values.zipWithIndex
        val nnzVals = entries.filter(v => v._1 != 0.0)
        SparseMatrix.fromCOO(n, n, nnzVals.map(v => (v._2, v._2, v._1)))
    }
  }

  def fromML(m: newlinalg.SparseMatrix): SparseMatrix = {
    new SparseMatrix(m.numRows, m.numCols, m.colPtrs, m.rowIndices, m.values, m.isTransposed)
  }
}

object DistributedMatrix{
  def dense(numRows: Int, numCols: Int, values: Array[Double]): DistributedMatrix ={
    new DenseMatrix(numRows, numCols, values)
  }

  def sparse(numRows: Int, numCols: Int, colPtrs: Array[Int], rowIndices: Array[Int], values: Array[Double]): DistributedMatrix ={
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values)
  }

  private def fromBreeze(breeze: BM[Double]): DistributedMatrix ={
    breeze match{
      case dm: BDM[Double] =>
        new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sm: BSM[Double] =>
        new SparseMatrix(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
      case _ =>
        throw new UnsupportedOperationException(s"Don't support conversion from tyep ${breeze.getClass.getName}")
    }
  }

  def zeros(numRows:Int, numCols:Int): DistributedMatrix = DenseMatrix.zeros(numRows, numCols)

  def ones(numRows:Int, numCols:Int): DistributedMatrix= DenseMatrix.ones(numRows, numCols)

  def eye(n: Int): DistributedMatrix = DenseMatrix.eye(n)

  def speye(n: Int): DistributedMatrix = SparseMatrix.speye(n)

  def rand(numRows:Int, numCols:Int, rng:Random): DistributedMatrix = DenseMatrix.rand(numRows, numCols, rng)

  def sprand(numRows:Int, numCols:Int, density: Double, rng:Random): DistributedMatrix = SparseMatrix.sprand(numRows, numCols, density, rng)

  def randn(numRows:Int, numCols:Int, rng:Random): DistributedMatrix = DenseMatrix.randn(numRows, numCols, rng)

  def sprandn(numRows: Int, numCols: Int, density: Double, rng: Random) :DistributedMatrix = SparseMatrix.sprandn(numRows, numCols, density, rng)

  def diag(vec: Vector): DistributedMatrix = DenseMatrix.diag(vec)

  def fromML(m: newlinalg.Matrix): DistributedMatrix = m match{
    case dm: newlinalg.DenseMatrix => DenseMatrix.fromML(dm)
    case sm: newlinalg.SparseMatrix => SparseMatrix.fromML(sm)
  }
}

case class MatrixBlock(pid: Int, rid: Int, cid: Int, matrix: DistributedMatrix)