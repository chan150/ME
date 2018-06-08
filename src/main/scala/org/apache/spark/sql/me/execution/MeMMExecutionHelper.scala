package org.apache.spark.sql.me.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.me.serializer.DMatrixSerializer
import org.apache.spark.sql.me.execution.MeExecutionHelper.{BroadcastPartitions, repartitionWithTargetPartitioner}
import org.apache.spark.sql.me.matrix.{Block, DistributedMatrix}
import org.apache.spark.sql.me.partitioner._
import org.apache.spark.Partitioner
import jcuda.jcublas._
import jcuda._
import jcuda.jcublas._
import jcuda.jcusparse._

import jcuda.jcudnn._


import scala.collection.mutable

object MeMMExecutionHelper {

  def cpmm(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowNum: Int, leftColNum: Int, rightRowNum: Int, rightColNum: Int, resultPart: Partitioner): RDD[InternalRow] = {
    val leftRDD = repartitionWithTargetPartitioner(new ColumnPartitioner(n, leftColNum), left)
    val rightRDD = repartitionWithTargetPartitioner(new RowPartitioner(n, rightRowNum), right)
    val newBlocks =leftRDD.zipPartitions(rightRDD, preservesPartitioning = true){ case (iter1, iter2) =>
      val leftBlocks = iter1.toList
      val rightBlocks = iter2.toList
      val res = findResultCPMM(leftRowNum, rightColNum)
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()

      res.par.map{ case (row, col) =>
        leftBlocks.filter(row == _._2._1._1).map{ case a =>
          rightBlocks.filter(col == _._2._1._2).filter(a._2._1._2 == _._2._1._1).map{ case b =>
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                DMatrixSerializer.deserialize(a._2._2),
                DMatrixSerializer.deserialize(b._2._2)))
            } else {
              tmp.put((row, col), Block.incrementalMultiply(
                DMatrixSerializer.deserialize(a._2._2),
                DMatrixSerializer.deserialize(b._2._2),
                tmp.get((row, col)).get))
            }
          }
        }
      }
      tmp.iterator
    }

    resultPart match {
      case rowPart:RowPartitioner =>

        newBlocks.map{ a =>
          val part = new RowPartitioner(n, leftRowNum)
          (part.getPartition(a._1), (a._1, a._2))
        }.groupByKey(new IndexPartitioner(n, new RowPartitioner(n, leftRowNum))).flatMap{ case (pid, blk) =>
          val CPagg = findResultRMMRight(pid, n, leftRowNum, rightColNum)
          val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
          CPagg.par.map{ case (row, col) =>
            blk.filter((row, col) == _._1).map{ case (idx, mat) =>
              if(!tmp.contains((row, col))){
                tmp.put((row, col), mat)
              } else {
                tmp.put((row, col), Block.add(tmp.get((row, col)).get, mat))
              }
            }
          }
          tmp.iterator
        }.map{ row =>
          val part = new RowPartitioner(n, leftRowNum)
          val rid = row._1._1
          val cid = row._1._2
          val pid = part.getPartition((rid, cid))
          val mat = row._2
          val res = new GenericInternalRow(4)
          res.setInt(0, pid)
          res.setInt(1, rid)
          res.setInt(2, cid)
          res.update(3, DMatrixSerializer.serialize(mat))
          res
        }
      case colPart:ColumnPartitioner =>
        newBlocks.map{ a =>
          val part = new ColumnPartitioner(n, rightColNum)
          (part.getPartition(a._1), (a._1, a._2))
        }.groupByKey(new IndexPartitioner(n, new RowPartitioner(n, leftRowNum))).flatMap{ case (pid, blk) =>
          val CPagg = findResultRMMLeft(pid, n, leftRowNum, rightColNum)
          val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
          CPagg.par.map{ case (row, col) =>
            blk.filter((row, col) == _._1).map{ case (idx, mat) =>
              if(!tmp.contains((row, col))){
                tmp.put((row, col), mat)
              } else {
                tmp.put((row, col), Block.add(tmp.get((row, col)).get, mat))
              }
            }
          }
          tmp.iterator
        }.map{ row =>
          val part = new ColumnPartitioner(n, rightColNum)
          val rid = row._1._1
          val cid = row._1._2
          val pid = part.getPartition((rid, cid))
          val mat = row._2
          val res = new GenericInternalRow(4)
          res.setInt(0, pid)
          res.setInt(1, rid)
          res.setInt(2, cid)
          res.update(3, DMatrixSerializer.serialize(mat))
          res
        }
      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for $resultPart")
    }
  }

  def rmmDuplicationRight(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, rightColBlkNum: Int): RDD[InternalRow] = {
    val part = new RowPartitioner(n, leftRowBlkNum)
    val leftRDD = repartitionWithTargetPartitioner(part, left)
    val dupRDD = BroadcastPartitions(right, n)

    leftRDD.zipPartitions(dupRDD, preservesPartitioning = true) { case (iter1, iter2) =>
      val leftBlocks = iter1.toList
      val rightBlocks = iter2.next()._2.toList

      val pid = leftBlocks.head._1

      val res = findResultRMMRight(pid, n, leftRowBlkNum, rightColBlkNum)
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
      res.par.map{ case (row, col) =>
        leftBlocks.filter(row == _._2._1._1).map{ case a =>
          rightBlocks.filter(col == _._1._2).filter(a._2._1._2 == _._1._1).map{ case b =>
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                DMatrixSerializer.deserialize(a._2._2),b._2))
            } else {
              tmp.put((row, col), Block.incrementalMultiply(DMatrixSerializer.deserialize(a._2._2),b._2, tmp.get((row, col)).get))
            }
          }
        }
      }
      tmp.iterator
    }.map{ row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = part.getPartition((rid, cid))
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, DMatrixSerializer.serialize(mat))
      res
    }
  }

  def rmmDuplicationLeft(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowBlkNum: Int, rightColBlkNum: Int): RDD[InternalRow] = {
    val part = new ColumnPartitioner(n, rightColBlkNum)
    val rightRDD = repartitionWithTargetPartitioner(part, right)
    val dupRDD = BroadcastPartitions(left, n)

    dupRDD.zipPartitions(rightRDD, preservesPartitioning = true) { case (iter1, iter2) =>
      val leftBlocks = iter1.next()._2.toList
      val rightBlocks = iter2.toList

      val pid = rightBlocks.head._1

      val res = findResultRMMLeft(pid, n, leftRowBlkNum, rightColBlkNum)
      val tmp = scala.collection.mutable.HashMap[(Int, Int), DistributedMatrix]()
      res.par.map{ case (row, col) =>
        leftBlocks.filter(row == _._1._1).map{ case a =>
          rightBlocks.filter(col == _._2._1._2).filter(a._1._2 == _._2._1._1).map{ case b =>
            if(!tmp.contains((row, col))){
              tmp.put((row, col), Block.matrixMultiplication(
                a._2, DMatrixSerializer.deserialize(b._2._2)))
            } else {
              tmp.put((row, col), Block.incrementalMultiply( a._2, DMatrixSerializer.deserialize(b._2._2), tmp.get((row, col)).get))
            }
          }
        }
      }
      tmp.iterator
    }.map{ row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = part.getPartition((rid, cid))
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, DMatrixSerializer.serialize(mat))
      res
    }
  }

  private def findResultRMMRight(pid: Int, n: Int, rows: Int, cols: Int): scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()

    if(pid == -1){
      tmp
    } else{
      val rowsInPartition = if(rows < n) rows else rows/n
      val rowsBase = pid % n
      val rowIndesList = (0 to rows -1).filter(rowsBase == _ / rowsInPartition)

      rowIndesList.flatMap{ case row =>
        (0 to cols-1).map{ case col =>
          tmp += ((row, col))
        }
      }
      tmp
    }
  }

  private def findResultRMMLeft(pid: Int, n: Int, rows: Int, cols: Int): scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()

    if(pid == -1){
      tmp
    } else{
      val colsInPartition = if(cols < n) cols else cols/n
      val colsBase = pid % n
      val colIndesList = (0 to rows -1).filter(colsBase == _ / colsInPartition)

      colIndesList.flatMap{ case col =>
        (0 to rows-1).map{ case row =>
          tmp += ((row, col))
        }
      }
      tmp
    }
  }

  private def findResultCPMM(rows: Int, cols: Int): scala.collection.mutable.HashSet[(Int, Int)] = {
    val tmp = new mutable.HashSet[(Int, Int)]()

    (0 to rows-1).map(row => (0 to cols-1).map(col => tmp += ((row, col))))

    tmp
  }
}
