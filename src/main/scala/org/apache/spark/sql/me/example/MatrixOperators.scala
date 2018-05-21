package org.apache.spark.sql.me.example

import org.apache.spark.sql.me.MeSession
import org.apache.spark.sql.me.matrix._

object MatrixOperators {
  def main(args: Array[String]): Unit = {

    val meSession = MeSession.builder().master("local[2]").appName("SparkSessionForMe").getOrCreate()


    runMatrixOpElements(meSession)

//    runMatrixTranspose(meSession)

    meSession.stop()

  }

  import scala.reflect.ClassTag
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)

  private def runMatrixOpElements(spark: MeSession): Unit = {
    import spark.implicits._

    val b1 = new DenseMatrix(2, 2, Array[Double](1, 1, 1, 1))
    val b2 = new DenseMatrix(2, 2, Array[Double](2, 2, 3, 3))
    val b3 = new DenseMatrix(2, 2, Array[Double](3, 3, 4, 4))
    val s1 = new SparseMatrix(2, 2, Array[Int](0, 1, 2),
      Array[Int](1, 0), Array[Double](4, 2))

    val pid = -1

    val A = Seq(MatrixBlock(-1, 0, 0, b2), MatrixBlock(-1, 1, 1, b2)).toDS()
    val B = Seq(MatrixBlock(-1, 0, 0, b1), MatrixBlock(-1, 1, 1, b3)).toDS()

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
    val tmp = A.divideElement(2, 5, 4,4, B.transpose(),4,4,2)
                .multiplyElement(2, 5, 4, 4, A, 4, 4, 2)

    val tmp1 = A.matrixMultiply(2, 5, 4, 4, A.transpose(), 4, 4, 2)
                    .addElement(2, 5, 4, 4, B, 4, 4, 2)

    val result = tmp.matrixMultiply(2, 5, 4, 4, tmp1, 4, 4, 2)

    result.explain()
    result.explain(true)

    result.rdd.foreach{ row =>

      val idx = (row.getInt(1), row.getInt(2))

      println(idx + ":")
      println(row.get(3).asInstanceOf[DistributedMatrix])
    }
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

