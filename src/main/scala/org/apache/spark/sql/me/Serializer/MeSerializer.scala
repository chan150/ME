package org.apache.spark.sql.me.Serializer

import java.math.BigDecimal
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.sql.me.matrix.{DenseMatrix, SparseMatrix}
import org.apache.spark.util.MutablePair
import org.apache.spark.sql.types.Decimal
import com.twitter.chill.ResourcePool

import scala.reflect.ClassTag

private[me] class MeSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()
    kryo.setRegistrationRequired(false)
    kryo.register(classOf[MutablePair[_, _]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow])
    kryo.register(classOf[java.math.BigDecimal], new JavaBigDecimalSerializer)
    kryo.register(classOf[BigDecimal], new ScalaBigDecimalSerializer)

    kryo.register(classOf[Decimal])
    kryo.register(classOf[java.util.HashMap[_, _]])
    kryo.register(classOf[DenseMatrix])
    kryo.register(classOf[SparseMatrix])

    kryo.setReferences(false)
    kryo
  }
}

private[me] class KryoResourcePool(size: Int) extends ResourcePool[SerializerInstance](size) {
  val ser: MeSerializer ={
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new MeSerializer(sparkConf)
  }

  override def newInstance(): SerializerInstance = ser.newInstance()
}

private[me] object MeSerializer{
  @transient
  lazy val resourcePool = new KryoResourcePool(50)

  private[this] def acquireRelease[O](fn: SerializerInstance => O): O ={
    val kryo = resourcePool.borrow()
    try{
      fn(kryo)
    } finally {
      resourcePool.release(kryo)
    }
  }

  def serialize[T: ClassTag](o: T): Array[Byte] ={
    acquireRelease{ k =>
      k.serialize(o).array()
    }
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T = {
    acquireRelease{ k =>
      k.deserialize[T](ByteBuffer.wrap(bytes))
    }
  }
}

private[me] class JavaBigDecimalSerializer extends Serializer[java.math.BigDecimal] {
  override def write(kryo: Kryo, output: Output, bd: java.math.BigDecimal): Unit = {
    output.writeString(bd.toString)
  }

  override def read(kryo: Kryo, input: Input, bd: Class[java.math.BigDecimal]): java.math.BigDecimal = {
    new BigDecimal(input.readString())
  }
}

private[me] class ScalaBigDecimalSerializer extends Serializer[BigDecimal] {
  override def write(kryo: Kryo, output: Output, bd: BigDecimal): Unit = {
    output.writeString(bd.toString)
  }

  override def read(kryo: Kryo, input: Input, bd: Class[BigDecimal]): BigDecimal = {
    new java.math.BigDecimal(input.readString())
  }
}
