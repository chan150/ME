package org.apache.spark.sql.me.partitioner

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.me.serializer.MeSerializer
import org.apache.spark.{Partitioner, SparkConf}

class RowPartitioner(n: Int, val numRowBlks:Long) extends Partitioner {
  require(n > 0, s"Number of partitions cannot be negative but found $n")

  val rowsInPartition = if(numRowBlks < n) numRowBlks.toDouble else ((numRowBlks*1.0)/(n*1.0))
  override val numPartitions = n

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int, j: Int) => Math.floor((i * 1.0) / (rowsInPartition*1.0)).toInt
      case (i: Int, j: Int, _: Int) =>
        println(s"row part, index: $i, $j  numPartition: $numPartitions, rowsInPartition: $rowsInPartition, key: ${Math.floor((i * 1.0) / (rowsInPartition*1.0)).toInt}")
        Math.floor((i * 1.0) / (rowsInPartition*1.0)).toInt
      case _=> throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    obj.isInstanceOf[RowPartitioner] && numPartitions == obj.asInstanceOf[RowPartitioner].numPartitions
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(n: java.lang.Integer)
  }
}

object RowPartitioner{
  def apply(rdd: RDD[InternalRow], numPartitions: Int, numRowBlks: Long): RDD[(Int, ((Int,Int), InternalRow))] = {
    val partitioner = new RowPartitioner(numPartitions, numRowBlks)


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
