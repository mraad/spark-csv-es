package com.esri

import org.slf4j.LoggerFactory

/**
  */
trait FieldReader extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)

  def readField(splits: Array[String], lineno: Long): Seq[(String, Any)]
}

case class NoopReader() extends FieldReader {
  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = Seq.empty[(String, Any)]
}