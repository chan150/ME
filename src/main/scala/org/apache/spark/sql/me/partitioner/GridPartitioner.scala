package org.apache.spark.sql.me.partitioner

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.me.Serializer.MeSerializer
import org.apache.spark.{Partitioner, SparkConf}

class GridPartitioner(val p: Int, val q: Int, val numRowBlks:Long, val numColBlks:Long) extends Partitioner {
  require(p >= 0, s"value P cannot be negative but found $p")
  require(q >= 0, s"value Q cannot be negative but found $q")



  override def numPartitions: Int= p * q


  override def getPartition(key: Any): Int = {
    val rowsInPartition = if(numRowBlks < numPartitions) numRowBlks.toInt else (numRowBlks/numPartitions).toInt
    val colsInPartition = if(numColBlks < numPartitions) numColBlks.toInt else (numColBlks/numPartitions).toInt

    key match{
      case (i:Int, j:Int) => ((i-1))/rowsInPartition * q + ((j-1)/colsInPartition)
      case (i:Int, j:Int, _:Int) => ((i-1))/rowsInPartition * q + ((j-1)/colsInPartition)
      case _=> throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    obj.isInstanceOf[ColumnPartitioner] && numPartitions == obj.asInstanceOf[ColumnPartitioner].numPartitions
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(numPartitions: java.lang.Integer)
  }
}

object GridPartitioner{
  def apply(rdd: RDD[InternalRow], p: Int, q: Int, numRowBlks:Long, numColBlks:Long): RDD[(Int, ((Int,Int), InternalRow))] = {
    val partitioner = new GridPartitioner(p,q, numRowBlks, numColBlks)


    val newRdd = rdd.map{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)

      (partitioner.getPartition((rid, cid)), ((rid, cid), mat))
    }

    val idxPart = new IndexPartitioner(p*q, partitioner)
    val shuffled = new ShuffledRDD[Int, ((Int, Int), InternalRow), ((Int, Int), InternalRow) ](newRdd, idxPart)
    shuffled.setSerializer(new MeSerializer(new SparkConf(false)))
    shuffled
  }
}