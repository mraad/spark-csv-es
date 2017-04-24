package com.esri

import org.slf4j.LoggerFactory

private[esri] abstract class AbstractTextReader(name: String, index: Int)
  extends FieldReader {

  val missingSeq: Seq[(String, Any)]

  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = {
    val aText = splits(index)
    if (aText.isEmpty) missingSeq else Seq(name -> aText)
  }
}

class TextReader(name: String, index: Int)
  extends AbstractTextReader(name, index) {
  override val missingSeq = Seq.empty
}

class TextMissingReader(name: String, index: Int, missing: String)
  extends AbstractTextReader(name, index) {
  override val missingSeq = Seq(name -> missing)
}

class TextReaderFactory(name: String, index: Int)
  extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new TextReader(name, index)
  }
}

class TextMissingReaderFactory(name: String, index: Int, missing: String)
  extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new TextMissingReader(name, index, missing)
  }
}

object TextReaderFactory extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)

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
