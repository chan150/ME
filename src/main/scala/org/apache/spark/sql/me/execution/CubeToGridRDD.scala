package org.apache.spark.sql.me.execution

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

private[spark]
class CuboToGridPartition(
                          idx: Int,
                          @transient private val rdd: RDD[_],
                          val rddIdxs: Seq[Int]
                        ) extends Partition {
//  var partSet = scala.collection.mutable.HashSet[]
//  var s1 = rdd1.partitions(s1Index)
//  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
//    s1 = rdd1.partitions(s1Index)
//    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

class CubeToGridRDD[T: ClassTag](
                                               sc: SparkContext,
                                               var rdd : RDD[T],
                                               p:Int, q:Int, k:Int,
                                               range:Int,
                                               master:String,
                                               slaves:Array[String]
                                               )
  extends RDD[T](sc, Nil) with Serializable {

  override val partitioner: Some[Partitioner]  = Some(rdd.partitioner.get)

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd.partitioner.get.numPartitions)

    val indexSeq = (0 until Math.floor((p*q*k)* 1.0/range* 1.0).toInt).map(start => (start*range until start*range + range).toSet.filter(_ < p*q*k).toSeq)

    for(seq <- indexSeq){
      val idx = seq.min / range
      array(idx) = new CuboToGridPartition(idx, rdd, seq)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq{slaves((split.asInstanceOf[CuboToGridPartition].index / range) % slaves.length)}
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val currSplit = split.asInstanceOf[CuboToGridPartition]
    val partiterSeq = for(x <- currSplit.rddIdxs) yield {
      rdd.iterator(rdd.partitions(x), context)
    }
    val PartIter = partiterSeq.toIterator

    new Iterator[T]{

      var currIter = if(PartIter.hasNext) PartIter.next() else throw new NoSuchElementException("next on empty iterator")

      override def hasNext: Boolean = if(currIter.hasNext) true else{
        if(PartIter.hasNext){
          true
        } else{
          throw new NoSuchElementException("next on empty iterator")
        }
      }

      override def next(): T = if(currIter.hasNext) currIter.next() else{
        if(PartIter.hasNext){
          currIter = PartIter.next()
          currIter.next()
        } else{
          throw new NoSuchElementException("next on empty iterator")
        }
      }
    }
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd) {
      def getParents(id: Int): Seq[Int] = List(id / range)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd = null
  }

}
