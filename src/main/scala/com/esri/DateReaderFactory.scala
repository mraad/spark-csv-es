package com.esri

import org.apache.spark.SparkConf
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.slf4j.LoggerFactory


private[esri] abstract class AbstractDateReader(fieldName: String,
                                                fieldIndex: Int,
                                                patternOrig: String,
                                                patternDest: Option[String],
                                                throwException: Boolean
                                               ) extends FieldReader {
  val parser = DateTimeFormat.forPattern(patternOrig).withZoneUTC()
  val formatter = if (patternDest.isDefined) DateTimeFormat.forPattern(patternDest.get).withZoneUTC() else ISODateTimeFormat.dateTime().withZoneUTC()
  val missingSeq: Seq[(String, Any)]

  def genSeq(datetime: DateTime): Seq[(String, Any)]

  override def readField(splits: Array[String], lineNo: Long): Seq[(String, Any)] = {
    val aDate = splits(fieldIndex).toUpperCase
    if (aDate.isEmpty)
      missingSeq
    else if (aDate.startsWith("NULL"))
      missingSeq
    else if (aDate.startsWith("UNDEFINED"))
      missingSeq
    else
      try {
        genSeq(parser.parseDateTime(aDate))
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aDate for field $fieldName at line $lineNo")
          if (throwException)
            throw t
          else
            missingSeq
        }
      }
  }
}

class DateReader(fieldName: String,
                 fieldIndex: Int,
                 patternOrig: String,
                 patternDest: Option[String],
                 throwException: Boolean
                ) extends AbstractDateReader(fieldName, fieldIndex, patternOrig, patternDest, throwException) {

  override val missingSeq: Seq[(String, Any)] = Seq.empty

  override def genSeq(datetime: DateTime): Seq[(String, Any)] = {
    Seq(
      (fieldName, formatter.print(datetime.getMillis)),
      (fieldName + "_yy", datetime.getYear),
      (fieldName + "_mm", datetime.getMonthOfYear),
      (fieldName + "_dd", datetime.getDayOfMonth),
      (fieldName + "_hh", datetime.getHourOfDay),
      (fieldName + "_dow", datetime.getDayOfWeek)
    )
  }
}

class DateOnlyReader(fieldName: String,
                     fieldIndex: Int,
                     patternOrig: String,
                     patternDest: Option[String],
                     throwException: Boolean
                    ) extends AbstractDateReader(fieldName, fieldIndex, patternOrig, patternDest, throwException) {

  override val missingSeq: Seq[(String, Any)] = Seq.empty

  override def genSeq(datetime: DateTime): Seq[(String, Any)] = {
    Seq(
      (fieldName, formatter.print(datetime.getMillis))
    )
  }
}

class DateMissingReader(fieldName: String,
                        fieldIndex: Int,
                        patternOrig: String,
                        patternDest: Option[String],
                        throwException: Boolean,
                        missingVal: String
                       ) extends AbstractDateReader(fieldName, fieldIndex, patternOrig, patternDest, throwException) {

  val missingDate = parser.parseDateTime(missingVal)
  override val missingSeq = Seq(
    (fieldName, formatter.print(missingDate.getMillis)),
    (fieldName + "_yy", missingDate.getYear),
    (fieldName + "_mm", missingDate.getMonthOfYear),
    (fieldName + "_dd", missingDate.getDayOfMonth),
    (fieldName + "_hh", missingDate.getHourOfDay),
    (fieldName + "_dow", missingDate.getDayOfWeek)
  )

  override def genSeq(datetime: DateTime): Seq[(String, Any)] = {
    Seq(
      (fieldName, formatter.print(datetime.getMillis)),
      (fieldName + "_yy", datetime.getYear),
      (fieldName + "_mm", datetime.getMonthOfYear),
      (fieldName + "_dd", datetime.getDayOfMonth),
      (fieldName + "_hh", datetime.getHourOfDay),
      (fieldName + "_dow", datetime.getDayOfWeek)
    )
  }
}

class DateOnlyMissingReader(fieldName: String,
                            fieldIndex: Int,
                            patternOrig: String,
                            patternDest: Option[String],
                            throwException: Boolean,
                            missingVal: String
                           ) extends AbstractDateReader(fieldName, fieldIndex, patternOrig, patternDest, throwException) {

  val missingDate = parser.parseDateTime(missingVal)
  override val missingSeq = Seq(
    (fieldName, formatter.print(missingDate.getMillis))
  )

  override def genSeq(datetime: DateTime): Seq[(String, Any)] = {
    Seq(
      (fieldName, formatter.print(datetime.getMillis))
    )
  }
}

class DateReaderFactory(name: String,
                        index: Int,
                        patternOrig: String,
                        patternDest: Option[String],
                        throwException: Boolean
                       ) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new DateReader(name, index, patternOrig, patternDest, throwException)
  }
}

class DateOnlyReaderFactory(name: String,
                            index: Int,
                            patternOrig: String,
                            patternDest: Option[String],
                            throwException: Boolean
                           ) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new DateOnlyReader(name, index, patternOrig, patternDest, throwException)
  }
}

class DateMissingReaderFactory(name: String,
                               index: Int,
                               patternOrig: String,
                               patternDest: Option[String],
                               throwException: Boolean,
                               missing: String
                              ) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new DateMissingReader(name, index, patternOrig, patternDest, throwException, missing)
  }
}

class DateOnlyMissingReaderFactory(name: String,
                                   index: Int,
                                   patternOrig: String,
                                   patternDest: Option[String],
                                   throwException: Boolean,
                                   missing: String
                                  ) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new DateOnlyMissingReader(name, index, patternOrig, patternDest, throwException, missing)
  }
}

object DateReaderFactory extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)

  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    val throwException = conf.getBoolean("error.exception", true)
    val dateFormat = Some(conf.get("date.pattern", "YYYY-MM-dd HH:mm:ss"))
    splits match {
      case Array(_, name, index, pattern) => new DateReaderFactory(name, index.toInt, pattern, dateFormat, throwException)
      case Array(_, name, index, pattern, missing) => new DateMissingReaderFactory(name, index.toInt, pattern, dateFormat, throwException, missing)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}

object DateOnlyReaderFactory extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)

  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    val throwException = conf.getBoolean("error.exception", true)
    val dateFormat = Some(conf.get("date.pattern", "YYYY-MM-dd HH:mm:ss"))
    splits match {
      case Array(_, name, index, pattern) => new DateOnlyReaderFactory(name, index.toInt, pattern, dateFormat, throwException)
      case Array(_, name, index, pattern, missing) => new DateOnlyMissingReaderFactory(name, index.toInt, pattern, dateFormat, throwException, missing)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}

object DateISOReaderFactory extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)

  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    val throwException = conf.getBoolean("error.exception", true)
    splits match {
      case Array(_, name, index, pattern) => new DateReaderFactory(name, index.toInt, pattern, None, throwException)
      case Array(_, name, index, pattern, missing) => new DateMissingReaderFactory(name, index.toInt, pattern, None, throwException, missing)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}

object DateOnlyISOReaderFactory extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)

  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    val throwException = conf.getBoolean("error.exception", true)
    splits match {
      case Array(_, name, index, pattern) => new DateOnlyReaderFactory(name, index.toInt, pattern, None, throwException)
      case Array(_, name, index, pattern, missing) => new DateOnlyMissingReaderFactory(name, index.toInt, pattern, None, throwException, missing)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}
