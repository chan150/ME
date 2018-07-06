package org.apache.spark.sql.me.partitioner

import org.apache.spark.Partitioner

class RedunColPartitioner(r: Int, n: Int) extends Partitioner {
  require(n > 0, s"Number of partitions cannot be negative but found $n")
  override val numPartitions = n

  val rf = r

  override def getPartition(key: Any): Int = {
    key match {
      case (i: Int) => i
      case _=> throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    obj.isInstanceOf[RedunColPartitioner] && numPartitions == obj.asInstanceOf[RedunColPartitioner].numPartitions
  }

  override def hashCode(): Int = {
    com.google.common.base.Objects.hashCode(n: java.lang.Integer)
  }

}
