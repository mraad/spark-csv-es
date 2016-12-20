package com.esri

/**
  */
trait FieldReader extends Serializable {
  def readField(splits: Array[String], lineno: Long): Seq[(String, Any)]
}

case class NoopReader() extends FieldReader {
  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = Seq.empty[(String, Any)]
}