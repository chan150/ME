package org.apache.spark.sql.me.partitioner

import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.me.serializer.MeSerializer

class CubePartitioner(p:Int, q:Int, k:Int) extends Partitioner {
  require(p*q*k >= 0, s"Number of partitions cannot be negative but found ${p*q*k}")

  override val numPartitions = p*q*k

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int, j: Int, z: Int) => (j*k*p)+(i*k)+z
      case _=> throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    val other = obj.asInstanceOf[CubePartitioner]
    obj.isInstanceOf[CubePartitioner] && numPartitions == other.numPartitions && p == other.p
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(numPartitions: java.lang.Integer)
  }
}

object CubePartitioner{
  def apply(rdd: RDD[InternalRow], numPartitions: Int, numRowBlks: Long): RDD[(Int, ((Int,Int), InternalRow))] = {
    val partitioner = new ColumnPartitioner(numPartitions, numRowBlks)


    val newRdd = rdd.map{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)

      (partitioner.getPartition((rid, cid)), ((rid, cid), mat))
    }

    val idxPart = new IndexPartitioner(numPartitions, partitioner)
    val shuffled = new ShuffledRDD[Int, ((Int, Int), InternalRow), ((Int, Int), InternalRow) ](newRdd, idxPart)
    shuffled.setSerializer(new MeSerializer(new SparkConf(false)))
    shuffled
  }

}
