package org.apache.spark.sql.me.partitioner


import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.me.serializer.MeSerializer
import org.apache.spark.{Partitioner, SparkConf}


class ColumnPartitioner(n: Int, val numColBlks:Long) extends Partitioner {
  require(n >= 0, s"Number of partitions cannot be negative but found $n")

  val rowsInPartition = if(numColBlks < n) numColBlks else numColBlks/n
  override val numPartitions = n

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int, j: Int) => ((j.toLong-1) /rowsInPartition).toInt
      case (i: Int, j: Int, _: Int) => ((j.toLong-1) /rowsInPartition).toInt
      case _=> throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    obj.isInstanceOf[ColumnPartitioner] && numPartitions == obj.asInstanceOf[ColumnPartitioner].numPartitions
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(n: java.lang.Integer)
  }
}

object ColumnPartitioner{
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
