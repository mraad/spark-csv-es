package com.esri

import org.apache.spark.{Logging, SparkConf}

private[esri] abstract class AbstractRealReader(name: String, index: Int, throwException: Boolean)
  extends FieldReader with Logging {

  val missingSeq: Seq[(String, Any)]

  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = {
    val aReal = splits(index).toLowerCase
    if (aReal.isEmpty)
      missingSeq
    else if (aReal.startsWith("null"))
      missingSeq
    else if (aReal.startsWith("undefined"))
      missingSeq
    else
      try {
        Seq((name, aReal.toDouble))
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aReal for field $name at line $lineno")
          if (throwException)
            throw t
          else
            missingSeq
        }
      }
  }
}

class RealReader(name: String, index: Int, throwException: Boolean)
  extends AbstractRealReader(name, index, throwException) {
  override val missingSeq = Seq.empty
}

class RealMissingReader(name: String, index: Int, throwException: Boolean, missing: Double)
  extends AbstractRealReader(name, index, throwException) {
  override val missingSeq = Seq(name -> missing)
}

class RealReaderFactory(name: String, index: Int, throwException: Boolean)
  extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new RealReader(name, index, throwException)
  }
}

class RealMissingReaderFactory(name: String, index: Int, throwException: Boolean, missing: Double)
  extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new RealMissingReader(name, index, throwException, missing)
  }
}

object RealReaderFactory extends Logging with Serializable {
  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    val throwException = conf.getBoolean("error.exception", true)
    splits match {
      case Array(_, name, index) => new RealReaderFactory(name, index.toInt, throwException)
      case Array(_, name, index, missing) => new RealMissingReaderFactory(name, index.toInt, throwException, missing.toDouble)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}
