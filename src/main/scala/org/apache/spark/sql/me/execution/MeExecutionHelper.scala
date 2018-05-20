package org.apache.spark.sql.me.execution

import org.apache.spark.{Partitioner, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.me.Serializer.DMatrixSerializer
import org.apache.spark.sql.me.matrix._
import org.apache.spark.sql.me.partitioner._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object MeExecutionHelper {

  def repartitionWithTargetPartitioner(partitioner: Partitioner,  rdd: RDD[InternalRow]): RDD[(Int, ((Int, Int), InternalRow))] ={
    partitioner match {
      case idxPart:IndexPartitioner =>  idxPart.basePart match {
        case rowPart:RowPartitioner => RowPartitioner(rdd, rowPart.numPartitions, rowPart.numRowBlks)
        case colPart:ColumnPartitioner => ColumnPartitioner(rdd, colPart.numPartitions, colPart.numColBlks)
        case gridPart:GridPartitioner => GridPartitioner(rdd, gridPart.p, gridPart.q, gridPart.numRowBlks, gridPart.numColBlks)
        case _ => throw new IllegalArgumentException(s"Partitioner not recognized for $partitioner")
      }
      case rowPart:RowPartitioner => RowPartitioner(rdd, rowPart.numPartitions, rowPart.numRowBlks)

      case colPart:ColumnPartitioner => ColumnPartitioner(rdd, colPart.numPartitions, colPart.numColBlks)

      case gridPart:GridPartitioner => GridPartitioner(rdd, gridPart.p, gridPart.q, gridPart.numRowBlks, gridPart.numColBlks)

      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for $partitioner")
    }
  }

  def selfElementAdd(rdd: RDD[InternalRow]): RDD[InternalRow] ={
    rdd.map{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val matrix = DMatrixSerializer.deserialize((row.getStruct(3, 7)))
      val mat = matrix match {
        case dm: DenseMatrix =>
          val arr = dm.values.map(x => 2*x)
          new DenseMatrix(dm.numRows, dm.numCols, arr, dm.isTransposed)
        case sm: SparseMatrix =>
          val arr = sm.values.map(x => 2*x)
          new SparseMatrix(sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices, arr, sm.isTransposed)
        case _ => throw new SparkException("Not supported matrix type.")
      }


      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, mat)
      res
    }
  }

  def selfElementDivide(rdd: RDD[InternalRow]): RDD[InternalRow] ={
    rdd.map{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val matrix = DMatrixSerializer.deserialize(row.getStruct(3, 7))

      val mat = matrix match{
        case dm: DenseMatrix =>
          val arr= dm.values.map(_ => 1.0)
          new DenseMatrix(dm.numRows, dm.numCols, arr, dm.isTransposed)
        case sm: SparseMatrix =>
          val arr = sm.values.map(_ => 1.0)
          new SparseMatrix(sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices, arr, sm.isTransposed)
        case _ => throw new SparkException("Not supported matrix type.")
      }

      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, DMatrixSerializer.serialize(mat))
      res
    }
  }

  def selfElementMultiply(rdd: RDD[InternalRow]): RDD[InternalRow] ={
    rdd.map{ row =>
      val pid = row.getInt(0)
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val matrix = DMatrixSerializer.deserialize((row.getStruct(3, 7)))
      val mat = matrix match {
        case dm: DenseMatrix =>
          val arr = dm.values.map(x => x*x)
          new DenseMatrix(dm.numRows, dm.numCols, arr, dm.isTransposed)
        case sm: SparseMatrix =>
          val arr = sm.values.map(x => x*x)
          new SparseMatrix(sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices, arr, sm.isTransposed)
        case _ => throw new SparkException("Not supported matrix type.")
      }

      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, mat)
      res
    }
  }

  def divideWithPartitioner(rdd1: RDD[(Int, ((Int, Int), InternalRow))],
                            rdd2: RDD[(Int, ((Int, Int), InternalRow))]): RDD[InternalRow] ={
    val part = rdd1.partitioner.get match {
      case idxPart: IndexPartitioner => idxPart.basePart
      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for ${rdd1.partitioner.get}")
    }
    val rdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { case (iter1, iter2) =>
      val idx2val = new TrieMap[(Int, Int), InternalRow]()
        val res = new TrieMap[(Int, Int), InternalRow]()
        for(elem <- iter1){
          val key = elem._2._1
          if(!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2._2)
        }
        for(elem <- iter2){
          val key = elem._2._1
          if(idx2val.contains(key)){
            val tmp = idx2val.get(key).get
            val division = DMatrixSerializer.serialize(Block.elementWiseDivide(DMatrixSerializer.deserialize(tmp),
              DMatrixSerializer.deserialize(elem._2._2)))
            res.putIfAbsent(key, division)
          }
        }
        res.iterator
    }
    rdd.map { row =>
      val pid = part.getPartition(row._1)
      val rid = row._1._1
      val cid = row._1._2
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, mat)
      res
    }
  }



  def multiplyWithPartitioner(rdd1: RDD[(Int, ((Int, Int), InternalRow))],
                            rdd2: RDD[(Int, ((Int, Int), InternalRow))]): RDD[InternalRow] ={
    val part = rdd1.partitioner.get match {
      case idxPart: IndexPartitioner => idxPart.basePart
      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for ${rdd1.partitioner.get}")
    }

    val resultRdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { case (iter1, iter2) =>
      val idx2val = new TrieMap[(Int, Int), InternalRow]()
      val res = new TrieMap[(Int, Int), InternalRow]()

      for(elem <- iter1){
        val key = elem._2._1
        if(!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2._2)
      }
      for(elem <- iter2){
        val key = elem._2._1
        if(idx2val.contains(key)){
          val tmp = idx2val.get(key).get
          val product = DMatrixSerializer.serialize(
            Block.elementWiseMultiply(DMatrixSerializer.deserialize(tmp),
              DMatrixSerializer.deserialize(elem._2._2)))
          res.putIfAbsent(key, product)
        }
      }
      res.iterator
    }

    resultRdd.map{ row =>
      val pid = part.getPartition(row._1)
      val rid = row._1._1
      val cid = row._1._2
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, mat)
      res
    }
  }

  def addWithPartitioner(rdd1: RDD[(Int, ((Int, Int), InternalRow))],
                         rdd2: RDD[(Int, ((Int, Int), InternalRow))]): RDD[InternalRow] ={
    val part = rdd1.partitioner.get match {
      case idxPart: IndexPartitioner => idxPart.basePart
      case _ => throw new IllegalArgumentException(s"Partitioner not recognized for ${rdd1.partitioner.get}")
    }

    val resultRdd = rdd1.zipPartitions(rdd2, preservesPartitioning = true) { case (iter1, iter2) =>
      val idx2val = new TrieMap[(Int, Int), InternalRow]()
      val res = new TrieMap[(Int, Int), InternalRow]()

      for(elem <- iter1){
        val key = elem._2._1
        if(!idx2val.contains(key)) idx2val.putIfAbsent(key, elem._2._2)
      }
      for(elem <- iter2){
        val key = elem._2._1
        if(idx2val.contains(key)){
          val tmp = idx2val.get(key).get
          val product = DMatrixSerializer.serialize(
            Block.add(DMatrixSerializer.deserialize(tmp),
              DMatrixSerializer.deserialize(elem._2._2)))
          res.putIfAbsent(key, product)
        }
      }
      res.iterator
    }

    resultRdd.map{ row =>
      val pid = part.getPartition(row._1)
      val rid = row._1._1
      val cid = row._1._2
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, mat)
      res
    }
  }

  private def BroadcastPartitions(rdd: RDD[InternalRow], numPartitions: Int): RDD[(Int, Iterable[((Int, Int), DistributedMatrix)])] = {
    rdd.flatMap{ row =>
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val matrix = DMatrixSerializer.deserialize(row.getStruct(3, 7))
      for(i <- 0 until numPartitions) yield (i, ((rid, cid), matrix))
    }.groupByKey(new IndexPartitioner(numPartitions, new BroadcastPartitioner(numPartitions)))
  }

  def rmmDuplicationRight(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowNum: Int, rightColNum: Int): RDD[InternalRow] = {
    val part = new RowPartitioner(n, leftRowNum)
    val leftRDD = repartitionWithTargetPartitioner(part, left)
    val dupRDD = BroadcastPartitions(right, n)

    leftRDD.zipPartitions(dupRDD, preservesPartitioning = true) { case (iter1, iter2) =>
      val leftBlocks = iter1.toList
      val rightBlocks = iter2.next()._2.toList

      val pid = leftBlocks.head._1

      val res = findResultRMMRight(pid, n, leftRowNum, rightColNum)
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
      res.update(3, mat)
      res
    }
  }

  def rmmDuplicationLeft(n: Int, left: RDD[InternalRow], right: RDD[InternalRow], leftRowNum: Int, rightColNum: Int): RDD[InternalRow] = {
    val part = new RowPartitioner(n, leftRowNum)
    val rightRDD = repartitionWithTargetPartitioner(part, right)
    val dupRDD = BroadcastPartitions(left, n)

    dupRDD.zipPartitions(rightRDD, preservesPartitioning = true) { case (iter1, iter2) =>
      val leftBlocks = iter1.next()._2.toList
      val rightBlocks = iter2.toList

      val pid = rightBlocks.head._1

      val res = findResultRMMLeft(pid, n, leftRowNum, rightColNum)
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
      res.update(3, mat)
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
            tmp += ((row+1, col+1))
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
          tmp += ((row+1, col+1))
        }
      }
      tmp
    }
  }

  def multiplyOuterProductDuplicationLeft(n: Int, rdd1: RDD[InternalRow],
                                          rdd2: RDD[InternalRow], rightColNum: Int): RDD[InternalRow] ={
    val numPartitions = n
    val part = new ColumnPartitioner(numPartitions, rightColNum)
    val rightRDD = repartitionWithTargetPartitioner(part, rdd2)
    val dupRDD = BroadcastPartitions(rdd1, numPartitions)

    dupRDD.zipPartitions(rightRDD, preservesPartitioning = true) {case (iter1, iter2) =>
        val dup = iter1.next()._2
        for{
          x2 <- iter2
          x1 <- dup
        } yield ((x1._1._1, x2._2._1._2) , Block.matrixMultiplication(x1._2, DMatrixSerializer.deserialize(x2._2._2)))
    }.map {row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = part.getPartition((rid, cid))
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, mat)
      res
    }
  }

  def multiplyOuterProductDuplicationRight(n:Int, rdd1: RDD[InternalRow],
                                          rdd2: RDD[InternalRow], leftRowNum: Int): RDD[InternalRow] ={
    val numPartitions = n
    val part = new RowPartitioner(numPartitions, leftRowNum)
    val leftRDD = repartitionWithTargetPartitioner(part, rdd1)
    val dupRDD = BroadcastPartitions(rdd2, numPartitions)
    leftRDD.zipPartitions(dupRDD, preservesPartitioning = true){ case (iter1, iter2) =>
        val dup = iter2.next()._2
        for{
          x1 <- iter1
          x2 <- dup
        } yield ((x1._2._1._1, x2._1._2), Block.matrixMultiplication(DMatrixSerializer.deserialize(x1._2._2), x2._2))
    }.map{row =>
      val rid = row._1._1
      val cid = row._1._2
      val pid = part.getPartition((rid, cid))
      val mat = row._2
      val res = new GenericInternalRow(4)
      res.setInt(0, pid)
      res.setInt(1, rid)
      res.setInt(2, cid)
      res.update(3, mat)
      res
    }
  }

  def matrixMultiplyGeneral(rdd1: RDD[InternalRow],
                            rdd2: RDD[InternalRow]): RDD[InternalRow] ={
    val leftRdd = rdd1.map{ row =>
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)
      (cid, (rid, mat))
    }.groupByKey()
    val rightRdd = rdd2.map{ row =>
      val rid = row.getInt(1)
      val cid = row.getInt(2)
      val mat = row.getStruct(3, 7)
      (cid, (rid, mat))
    }.groupByKey()

    leftRdd.join(rightRdd).values.flatMap { case (iter1, iter2) =>
        for(blk1 <- iter1; blk2 <- iter2)
          yield ((blk1._1, blk2._1), Block.matrixMultiplication(
            DMatrixSerializer.deserialize(blk1._2),
            DMatrixSerializer.deserialize(blk2._2)
          ))
    }.reduceByKey(Block.add(_, _)).map{ row =>
      val res = new GenericInternalRow(4)
      res.setInt(0, -1)
      res.setInt(1, row._1._1)
      res.setInt(2, row._1._2)
      res.update(3, DMatrixSerializer.serialize(row._2))
      res
    }
  }
}
