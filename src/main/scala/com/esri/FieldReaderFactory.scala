package com.esri

/**
  */
trait FieldReaderFactory extends Serializable {
  def createFieldReader(): FieldReader
}

case class NoopReaderFactory() extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = NoopReader()
}