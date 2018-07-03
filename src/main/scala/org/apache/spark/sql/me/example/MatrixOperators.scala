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
        .config("spark.shuffle.consolidateFiles", "true")
        .config("spark.shuffle.compress", "false")
        .config("spark.rpc.message.maxSize", "1000")
        .config("spark.locality.wait", "3s")
        .config("spark.task.cpus", "2")
//      .config("spark.executor.cores", "12")
//      .config("spark.executor.memory", "60g")
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

    val b4 = SparseMatrix.sprand(1000, 1000, 0.01, new Random)
    val D1 = DenseMatrix.rand(1000, 1000, new Random)

//    val A = Seq(
//      MatrixBlock(-1, 0, 0, b4), MatrixBlock(-1, 0, 1, b4), MatrixBlock(-1, 0, 2, b4), MatrixBlock(-1, 0, 3, b4),
//      MatrixBlock(-1, 1, 0, b4), MatrixBlock(-1, 1, 1, b4), MatrixBlock(-1, 1, 2, b4), MatrixBlock(-1, 1, 3, b4),
//      MatrixBlock(-1, 2, 0, b4), MatrixBlock(-1, 2, 1, b4), MatrixBlock(-1, 2, 2, b4), MatrixBlock(-1, 2, 3, b4),
//      MatrixBlock(-1, 3, 0, b4), MatrixBlock(-1, 3, 1, b4), MatrixBlock(-1, 3, 2, b4), MatrixBlock(-1, 3, 3, b4)
//    ).toDS()


    val leftRowBlkNum = 10
    val leftColBlkNum = 10



    val rightRowBlkNum = leftColBlkNum
    val rightColBlkNum = 10
    val blkSize = 1000

    val leftRowNum = leftRowBlkNum * blkSize
    val leftColNum = leftColBlkNum * blkSize

    val rightRowNum = rightRowBlkNum * blkSize
    val rightColNum = rightColBlkNum * blkSize


    val A = spark.sparkContext.parallelize(for(i <- 0 until leftRowBlkNum; j <- 0 until leftColBlkNum) yield (i, j),60)
      .map(coord =>  MatrixBlock(-1, coord._1, coord._2, b4)).toDS()



    val B = spark.sparkContext.parallelize(for(i <- 0 until rightRowBlkNum; j <- 0 until rightColBlkNum) yield (i, j),60)
      .map(coord =>  MatrixBlock(-1, coord._1, coord._2, b4)).toDS()

//
//    val tmpRowBlkNum = 10
//    val tmpColBlkNum = 10
//    val tmpRowNum = tmpRowBlkNum * blkSize
//    val tmpColNum = tmpColBlkNum * blkSize
//
//    val tmp = spark.sparkContext.parallelize(for(i <- 0 until tmpRowBlkNum; j <- 0 until tmpColBlkNum) yield (i, j),60)
//      .map(coord =>  MatrixBlock(-1, coord._1, coord._2, b4)).toDS()

//    seq1.rdd.foreach{ case row =>
//      val idx = (row.rid, row.cid)
//      println(idx + ":")
//      println(row.matrix)
//    }
//
//    seq2.rdd.foreach{ case row =>
//      val idx = (row.rid, row.cid)
//      println(idx + ":")
//      println(row.matrix)
//    }
    import spark.MeImplicits._
//    val tmp = A.addElement(2, 5, 4,4, B.transpose(),4,4,2)
//                .matrixMultiply(2, 5, 4, 4, A, 4, 4, 2)
//
//    val tmp1 = A.matrixMultiply(2, 5, 4, 4, A.transpose(), 4, 4, 2)
//                    .matrixMultiply(2, 5, 4, 4, B, 4, 4, 2)


    val C =  A.matrixMultiply(leftRowNum, leftColNum, B, rightRowNum, rightColNum, blkSize)
//
//    val D = C.matrixMultiply(leftRowNum, rightColNum, tmp, tmpRowNum, tmpColNum, blkSize)
//
//    val result = C.matrixMultiply(tmpRowNum, tmpColNum, D, leftRowNum, leftColNum, blkSize)
//
    C.explain(true)

    println( C.rdd.count())

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

