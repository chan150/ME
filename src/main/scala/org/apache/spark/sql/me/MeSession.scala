package org.apache.spark.sql.me


import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.{Dataset => SqlDataset,SparkSession => SqlSession, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.util.Utils

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal


class MeSession private[me] (@transient override val sparkContext: SparkContext) extends SqlSession(sparkContext){

  self =>

  @transient
  private[sql] override lazy val sessionState: MeSessionState ={
    new MeSessionState(this)
  }

  object MeImplicits extends Serializable{
    def _meContext: MeSession = self
    implicit def datatsetToMeDataset[T: Encoder](df: SqlDataset[T]): Dataset[T] = Dataset(self, df.queryExecution.logical)
  }
}

@InterfaceStability.Stable
object MeSession {

  @InterfaceStability.Stable
  class Builder extends Logging {
    private[this] val options = new mutable.HashMap[String, String]

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (key, value) => options += key -> value }
      this
    }

    def appName(name: String): Builder = config("spark.app.name", name)

    def master(master: String): Builder = config("spark.master", master)

    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException("Unable to instantiate SparkSession with Hive support because " + "Hive classes are not found.")
      }
    }


    def getOrCreate(): MeSession = synchronized {
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (key, value) => session.sessionState.conf.setConfString(key, value) }

        if (options.nonEmpty) {
          logWarning("Using an existing SparkSession; some configuration may not take effect")
        }
        return session
      }

      MeSession.synchronized {
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (key, value) => session.sessionState.conf.setConfString(key, value) }

          if (options.nonEmpty) {
            logWarning("Using an existing SparkSession; some configuration may not take effect")
          }
          return session
        }

        val sparkContext = userSuppliedContext.getOrElse {
          val randomAppName = java.util.UUID.randomUUID().toString
          val sparkConf = new SparkConf()
          options.foreach { case (key, value) => sparkConf.set(key, value) }
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(randomAppName)
          }

          val sc = SparkContext.getOrCreate(sparkConf)

          options.foreach { case (key, value) => sc.conf.set(key, value) }
          if (!sc.conf.contains("spark.app.name")) {
            sc.conf.setAppName(randomAppName)
          }
          sc
        }

        session = new MeSession(sparkContext)
        options.foreach { case (key, value) => session.sessionState.conf.setConfString(key, value) }
        defaultSession.set(session)

        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
            sqlListener.set(null)
          }
        })
      }
      return session
    }
  }

  private[sql] val activeThreadSession = new InheritableThreadLocal[MeSession]
  private[sql] val defaultSession = new AtomicReference[MeSession]
  private[sql] val sqlListener = new AtomicReference[SQLListener]

  def builder(): Builder = new Builder
  def setActiveSession(session: MeSession): Unit={
   activeThreadSession.set(session)
  }
  def clearActiveSession(): Unit={
    activeThreadSession.remove()
  }
  def setDefaultSession(session: MeSession): Unit={
    defaultSession.set(session)
  }

  def clearDefaultSession(): Unit={
    defaultSession.set(null)
  }

  private[sql] def getActiveSession: Option[MeSession] = Option(activeThreadSession.get)
  private[sql] def getDefaultSession: Option[MeSession] = Option(defaultSession.get)

  private val HIVE_SESSION_STATE_CLASS_NAME = "org.apache.spark.sql.hive.HiveSessionState"

  private def sessionStateClassName(conf: SparkConf): String ={
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_SESSION_STATE_CLASS_NAME
      case "in-memory" => classOf[SessionState].getCanonicalName
    }
  }

  private def reflect[T, Arg <: AnyRef](className: String,
                                        ctorArg: Arg)(implicit ctorArgTag: ClassTag[Arg]): T ={
    try{
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag.runtimeClass)
      ctor.newInstance(ctorArg).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private[spark] def hiveClassesArePresent: Boolean ={
    try{
      Utils.classForName(HIVE_SESSION_STATE_CLASS_NAME)
      Utils.classForName("org.apache.haddop.hive.conf.HiveConf")
      true
    } catch{
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }
}
