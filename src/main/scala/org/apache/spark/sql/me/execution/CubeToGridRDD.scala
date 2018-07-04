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
  var partSet = for(x<- rddIdxs) yield rdd.partitions(x)
//  var s1 = rdd1.partitions(s1Index)
//  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
//    s1 = rdd1.partitions(s1Index)
//    s2 = rdd2.partitions(s2Index)
    partSet = for(x<- rddIdxs) yield rdd.partitions(x)
    oos.defaultWriteObject()
  }
}

class CubeToGridRDD[T: ClassTag](
                                               sc: SparkContext,
                                               var rdd : RDD[T],
                                               p:Int, q:Int, k:Int,
                                               part:Partitioner,
                                               master:String,
                                               slaves:Array[String]
                                               )
  extends RDD[T](sc, Nil) with Serializable {

  override val partitioner: Some[Partitioner]  = Some(part)

  override def getPartitions: Array[Partition] = {
    // create the cross product split
//    println(s"CubdToGrid, array size: ${part.numPartitions}")
//    println(s"rdd partition number: ${rdd.partitions.length}")
    val array = new Array[Partition](part.numPartitions)

    val indexSeq = (0 until p*q).map(start => (start*k until start*k + k).toSet.filter(_ < p*q*k).toSeq)


    for(seq <- indexSeq){
      val idx = seq.min / k

//      println(s"seq indeice: ${seq}")
//      println(s"index: ${idx}")
      array(idx) = new CuboToGridPartition(idx, rdd, seq)
    }
    array
  }

//  override def getPreferredLocations(split: Partition): Seq[String] = {
//    Seq{slaves((split.asInstanceOf[CuboToGridPartition].index / k) % slaves.length)}
//  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val currSplit = split.asInstanceOf[CuboToGridPartition]
    val partiterSeq = for(x <- currSplit.partSet) yield {
//      println(s"partition index: $x, split index: ${split.index}")
      rdd.iterator(x, context)
    }
//    println(s"test")
    val PartIter = partiterSeq.toIterator

    new Iterator[T]{

      var currIter = if(PartIter.hasNext) PartIter.next() else throw new NoSuchElementException("next on empty iterator")

      override def hasNext: Boolean = if(currIter.hasNext) true else{
        if(PartIter.hasNext){
          true
        } else{
          false
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
      def getParents(id: Int): Seq[Int] = {
        (id until id+k).toList
      }
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd = null
  }

}
