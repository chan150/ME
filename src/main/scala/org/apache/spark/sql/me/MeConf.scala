package org.apache.spark.sql.me

import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.immutable


private[me] class MeConf extends Serializable {
  import MeConf._


  @transient protected[me] val settings = java.util.Collections.synchronizedMap( new util.HashMap[String, String]())

  private[me] def sampleRate: Double = getConf(SAMPLE_RATE)

  def setConf(props: Properties): Unit ={
    settings.synchronized{
      props.asScala.foreach{ case (key, value) => setConfString(key, value)}
    }
  }
  def setConfString(key:String, value: String): Unit ={
    require(key != null, s"key can't be null")
    require(value != null, s"value can't be null for key $key")

    val entry = meConfEntries.get(key)
    if(entry != null) {
      entry.valueConverter(value)
    }
    settings.put(key, value)
  }

  def setConf[T](entry: MeConfEntry[T], value : T): Unit={
    require(entry != null, "entry can't be null")
    require(value != null, s"value can't be null for key: ${entry.key}")
    require(meConfEntries.get(entry.key) == entry, s"$entry is not registered")
    settings.put(entry.key, entry.stringConverter(value))
  }

  def getConfString(key:String): String ={
    Option(settings.get(key)).orElse{
      Option(meConfEntries.get(key)).map(_.defaultValueString)
    }.getOrElse(throw new NoSuchElementException(key))
  }

  def getConf[T](entry:MeConfEntry[T], defaultValue: T): T={
    require(meConfEntries.get(entry.key) == entry, s"$entry is not registerd")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).getOrElse(throw new NoSuchElementException(entry.key))
  }

  def getConf[T](entry:MeConfEntry[T]): T ={
    require(meConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).getOrElse(throw new NoSuchElementException(entry.key))
  }

  def getConfString(key: String, defaultVale: String): String ={
    val entry = meConfEntries.get(key)
    if (entry != null && defaultVale != "<undefined>"){
      entry.valueConverter(defaultVale)
    }
    Option(settings.get(key)).getOrElse(defaultVale)
  }

  def getAllConfs: immutable.Map[String, String] = settings.synchronized{settings.asScala.toMap}

  def getAllDefinedConfs: Seq[(String, String, String)] = meConfEntries.synchronized{
    meConfEntries.values.asScala.filter(_.ispublic).map{ entry =>
      (entry.key, entry.defaultValueString, entry.doc)
    }.toSeq
  }

  private[me] def unsetConf(key: String): Unit ={
    settings.remove(key)
  }
  private[me] def unsetConf(entry : MeConfEntry[_]): Unit ={
    settings.remove(entry.key)
  }
  private[me] def clear(): Unit ={
    settings.clear()
  }
}

private[me] object MeConf{

    private val meConfEntries = java.util.Collections.synchronizedMap(new util.HashMap[String, MeConfEntry[_]]())

    private[me] class MeConfEntry[T] private( val key: String,
                                              val defaultValue: Option[T],
                                              val valueConverter: String => T,
                                              val stringConverter: T => String,
                                              val doc: String,
                                              val ispublic: Boolean){
      def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefinde>")

      override def toString: String = {
        s"MeConfEntry(key = $key, defaultValue = $defaultValueString, doc = $doc, isPublic = $ispublic"
      }
    }

    private[me] object MeConfEntry {
      private def apply[T](key: String,
                           defaultValue: Option[T],
                           valueConverter: String => T,
                           stringConverter: T => String,
                           doc: String,
                           isPublic: Boolean): MeConfEntry[T] = meConfEntries.synchronized{
        if(meConfEntries.containsKey(key)){
          throw new IllegalArgumentException(s"Duplicate MeConfEntry. $key has been registered")
        }
        val entry = new MeConfEntry[T](key, defaultValue, valueConverter, stringConverter, doc, isPublic)
        meConfEntries.put(key, entry)
        entry
      }

      def intConf(key:String, defaultValue: Option[Int] = None, doc: String= "", isPublic: Boolean = true): MeConfEntry[Int] =
        MeConfEntry(key, defaultValue, {v =>
          try{
            v.toInt
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"$key should be long type, but was $v")
          }
        }, _.toString, doc, isPublic)

      def LongConf(key:String, defaultValue: Option[Long] = None, doc: String= "", isPublic: Boolean = true): MeConfEntry[Long] =
        MeConfEntry(key, defaultValue, {v =>
          try{
            v.toLong
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"$key should be long type, but was $v")
          }
        }, _.toString, doc, isPublic)

      def doubleConf(key:String, defaultValue: Option[Double] = None, doc: String= "", isPublic: Boolean = true): MeConfEntry[Double] =
        MeConfEntry(key, defaultValue, {v =>
          try{
            v.toDouble
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"$key should be long type, but was $v")
          }
        }, _.toString, doc, isPublic)

      def booleanConf(key:String, defaultValue: Option[Boolean] = None, doc: String= "", isPublic: Boolean = true): MeConfEntry[Boolean] =
        MeConfEntry(key, defaultValue, {v =>
          try{
            v.toBoolean
          } catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"$key should be long type, but was $v")
          }
        }, _.toString, doc, isPublic)

      def stringConf(key: String, defaultValue: Option[String] = None, doc: String ="", isPublic: Boolean=true): MeConfEntry[String] =
        MeConfEntry(key, defaultValue, v => v, v => v, doc, isPublic)
    }

    import MeConfEntry._
    val SAMPLE_RATE = doubleConf("me.sampleRate", defaultValue = Some(0.05))

}
