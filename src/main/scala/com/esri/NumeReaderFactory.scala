package com.esri

import org.apache.spark.{Logging, SparkConf}

class NumeReader(name: String, index: Int, throwException: Boolean) extends FieldReader with Logging {
  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = {
    val aNume = splits(index).toLowerCase
    if (aNume.isEmpty)
      Seq.empty
    else if (aNume.startsWith("null"))
      Seq.empty
    else if (aNume.startsWith("undefined"))
      Seq.empty
    else
      try {
        Seq((name, aNume.toInt))
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aNume for field $name at line $lineno")
          if (throwException)
            throw t
          else
            Seq.empty
        }
      }
  }
}

class NumeMissingReader(name: String, index: Int, throwException: Boolean, missing: Int) extends FieldReader with Logging {
  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = {
    val aNume = splits(index).toLowerCase
    if (aNume.isEmpty)
      Seq(name -> missing)
    else if (aNume.startsWith("null"))
      Seq(name -> missing)
    else if (aNume.startsWith("undefined"))
      Seq(name -> missing)
    else
      try {
        Seq((name, aNume.toInt))
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aNume for field $name at line $lineno")
          if (throwException)
            throw t
          else
            Seq(name -> missing)
        }
      }
  }
}

class NumeReaderFactory(name: String, index: Int, throwException: Boolean) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new NumeReader(name, index, throwException)
  }
}

class NumeMissingReaderFactory(name: String, index: Int, throwException: Boolean, missing: Int) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new NumeMissingReader(name, index, throwException, missing)
  }
}

object NumeReaderFactory extends Logging with Serializable {
  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    val throwException = conf.getBoolean("error.exception", true)
    splits match {
      case Array(_, name, index) => new NumeReaderFactory(name, index.toInt, throwException)
      case Array(_, name, index, missing) => new NumeMissingReaderFactory(name, index.toInt, throwException, missing.toInt)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}
