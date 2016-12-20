package com.esri

import org.apache.spark.Logging

class TextReader(name: String, index: Int) extends FieldReader {
  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = {
    if (index < splits.length) {
      val aText = splits(index)
      if (aText.isEmpty) Seq.empty else Seq(name -> aText)
    } else {
      Seq.empty
    }
  }
}

class TextMissingReader(name: String, index: Int, missing: String) extends FieldReader {
  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = {
    if (index < splits.length) {
      val aText = splits(index)
      Seq((name, if (aText.isEmpty) missing else aText))
    } else {
      Seq(name -> missing)
    }
  }
}

class TextReaderFactory(name: String, index: Int) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new TextReader(name, index)
  }
}

class TextMissingReaderFactory(name: String, index: Int, missing: String) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new TextMissingReader(name, index, missing)
  }
}

object TextReaderFactory extends Logging with Serializable {
  def apply(splits: Array[String]): FieldReaderFactory = {
    splits match {
      case Array(_, name, index) => new TextReaderFactory(name, index.toInt)
      case Array(_, name, index, missing) => new TextMissingReaderFactory(name, index.toInt, missing)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}
