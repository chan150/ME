package org.apache.spark.sql.me.example

import java.util.Random

import org.apache.spark.sql.me.MeSession
import org.apache.spark.sql.me.matrix._

import org.apache.spark.sql.me.partitioner.{IndexPartitioner, RedunColPartitioner, RedunRowPartitioner, RowPartitioner}


object MatrixOperators {
  def main(args: Array[String]): Unit = {

    val meSession = MeSession
      .builder()
//      .master("local[*]")
      .master("spark://jupiter22:7077")
      .appName("ME")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max.mb", "512")
      .config("spark.shuffle.consolidateFiles", "true")
      .config("spark.shuffle.compress", "false")
      .config("spark.rpc.message.maxSize", "1000")
      .config("spark.network.timeout", "1000000s")
      .config("spark.locality.wait", "1000000s")
//      .config("spark.locality.wait", "3s")
      .config("spark.executor.cores", "2")
      .config("spark.executor.memory", "10g")
      .config("spark.task.cpus", "1")
      .config("spark.reducer.maxSizeInFlight", "256m")
      .config("spark.shuffle.file.buffer", "256k")
      .getOrCreate()


    runMatrixOpElements(meSession)

//    runMatrixTranspose(meSession)


    meSession.stop()

  }

  import scala.reflect.ClassTag
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)

  private def runMatrixOpElements(spark: MeSession): Unit = {
    import spark.implicits._

//
//    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 1, 1))
//    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
//    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
//    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
//      Array[Int](1, 0), Array[Double](4, 2))

    val blkSize = 1000
    val rank = 200
    val sparsity = 0.0001
    val sparsity1 = 0.0001

    val leftRowBlkNum = 100
    val leftColBlkNum = 100



    val rightRowBlkNum = leftColBlkNum
    val rightColBlkNum = 100


    val leftRowNum = leftRowBlkNum * blkSize
    val leftColNum = leftColBlkNum * blkSize

    val rightRowNum = rightRowBlkNum * blkSize
    val rightColNum = rightColBlkNum * blkSize

    val blkMemorySize = sparsity * ((blkSize * blkSize * 8) / (1024 * 1024 * 1024 * 1.0))




    require(blkMemorySize < 2, s"very large block size: ${blkMemorySize}GB")

    val limitNumBlk = Math.ceil(2.0 / blkMemorySize).toInt

    var numPart = leftRowBlkNum * leftColBlkNum / limitNumBlk

    println(s"number of partition: ${numPart}, the size of block: ${blkMemorySize}, the limit number of block in a task: ${limitNumBlk}")


    val ClusterParallelizm = 120

    if(numPart < ClusterParallelizm){
      if(leftRowBlkNum * leftColBlkNum < ClusterParallelizm)
        numPart = leftRowBlkNum * leftColBlkNum
      else
        numPart = leftRowBlkNum * leftColBlkNum
    }

    val V = spark.sparkContext.parallelize(for(i <- 0 until leftRowBlkNum; j <- 0 until leftColBlkNum) yield (i, j), numPart)
      .map(coord =>  MatrixBlock(-1, coord._1, coord._2, SparseMatrix.sprand(blkSize, blkSize,sparsity1, new Random))).toDS()
//      .map(coord =>  MatrixBlock(-1, coord._1, coord._2, DenseMatrix.rand(blkSize, blkSize, new Random))).toDS()


    numPart = rightRowBlkNum * rightColBlkNum / limitNumBlk

    if(numPart < ClusterParallelizm){
      if(rightRowBlkNum * rightColBlkNum < ClusterParallelizm)
        numPart = leftRowBlkNum * leftColBlkNum
      else
        numPart = leftRowBlkNum * leftColBlkNum
    }



    val W = spark.sparkContext.parallelize(for(i <- 0 until rightRowBlkNum; j <- 0 until rightColBlkNum) yield (i, j), numPart)
      .map { coord => MatrixBlock(-1, coord._1, coord._2, SparseMatrix.sprand(blkSize, blkSize,sparsity, new Random))}.toDS()
//      .map(coord =>  MatrixBlock(-1, coord._1, coord._2, DenseMatrix.rand(blkSize, blkSize, new Random))).toDS()

//
//    val tmpRowBlkNum = 1
//    val tmpColBlkNum = 100
//    val tmpRowNum = tmpRowBlkNum * blkSize
//    val tmpColNum = tmpColBlkNum * blkSize
//
//    val H = spark.sparkContext.parallelize(for(i <- 0 until tmpRowBlkNum; j <- 0 until tmpColBlkNum) yield (i, j),60)
//      .map{ coord =>
//        val block: Array[Double] = DenseMatrix.rand(rank, blkSize, new Random()).toArray
//        (0 until blkSize*rank).map(i => d(i) = block(i))
//        MatrixBlock(-1, coord._1, coord._2, new DenseMatrix(blkSize, blkSize, d).toSparse)
//      }.toDS()

    import spark.MeImplicits._

//
//    val newH =  H.multiplyElement(10, 6, tmpRowNum, tmpColNum, W.transpose().matrixMultiply(rightColNum, rightRowNum, V, leftRowNum, leftColNum, blkSize), rightColNum, leftColNum, blkSize)
//
//    val new1 = newH.divideElement(10,6, tmpRowNum, tmpColNum, W.transpose().matrixMultiply(rightColNum, rightRowNum, W, rightRowNum, rightColNum, blkSize)
//      .matrixMultiply(rightColNum, rightColNum, H, tmpRowNum, tmpColNum, blkSize ), tmpRowNum, tmpColNum, blkSize)

    val result = V.matrixMultiply(leftRowNum, leftColNum, W, rightRowNum, rightColNum, blkSize)

    result.explain()

    println( result.rdd.count())

//    println(result.rdd.partitions.size)
//    result.rdd.collect().foreach{ row =>
//
//      val idx = (row.getInt(1), row.getInt(2))
//
//      println(idx + ":")
//      println(row.get(3).asInstanceOf[DistributedMatrix])
//    }
//    println("matrix element-wise divide test")
//
//    val multiply = divided.multiplyElement(2, 5, 4, 4, seq1, 4, 4, 2)
//
//    multiply.rdd.foreach{ row =>
//      val idx = (row.getInt(1), row.getInt(2))
//
//      println(idx + ":")
//      println(row.get(3).asInstanceOf[DistributedMatrix])
//    }
//    println("matrix element-wise multiply test")
//    val add = multiply.addElement(2, 5, 4, 4, divided, 4, 4, 2)
//
//
//    add.rdd.foreach{ row =>
//      val idx = (row.getInt(1), row.getInt(2))
//
//      println(idx + ":")
//      println(row.get(3).asInstanceOf[DistributedMatrix])
//    }
//    println("matrix element-wise add test")
//    val MM = add.matrixMultiply(2, 5, 4, 4, multiply, 4, 4, 2)
//
//
//    MM.rdd.foreach{ row =>
//      val idx = (row.getInt(1), row.getInt(2))
//
//      println(idx + ":")
//      println(row.get(3).asInstanceOf[DistributedMatrix])
//    }
//    println("matrix multiplication test")
  }

  private def runMatrixTranspose(spark: MeSession): Unit = {
    import spark.implicits._
    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 2, 2))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val b4 = new DenseMatrix(2, 2, Array[Double](4, 5, 6, 7))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))

    // val seq = Seq((0, 0, b1), (0, 1, b2), (1, 0, b3), (1, 1, b4))
    val pid = -1
    val seq = Seq(MatrixBlock(-1, 0, 2, s1), MatrixBlock(-1, 2, 3, b2), MatrixBlock(-1, 4, 5, b3), MatrixBlock(-1, 6, 7, b4)).toDS()
    import spark.MeImplicits._
    seq.transpose().rdd.foreach{ row =>
      println(row.get(3).asInstanceOf[DistributedMatrix])
    }
  }
}

