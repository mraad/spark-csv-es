package com.esri

import org.apache.spark.{Logging, SparkConf}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}

class DateReader(fieldName: String,
                 fieldIndex: Int,
                 pattern: String,
                 throwException: Boolean
                ) extends FieldReader with Logging {

  val parser = DateTimeFormat.forPattern(pattern).withZoneUTC()
  val formatter = ISODateTimeFormat.dateTime()

  override def readField(splits: Array[String], lineNo: Long): Seq[(String, Any)] = {
    val aDate = splits(fieldIndex).toUpperCase
    if (aDate.isEmpty)
      Seq.empty
    else if (aDate.startsWith("NULL"))
      Seq.empty
    else if (aDate.startsWith("UNDEFINED"))
      Seq.empty
    else
      try {
        val datetime = parser.parseDateTime(aDate)
        Seq(
          (fieldName, formatter.print(datetime.getMillis)),
          (fieldName + "_yy", datetime.getYear),
          (fieldName + "_mm", datetime.getMonthOfYear),
          (fieldName + "_dd", datetime.getDayOfMonth),
          (fieldName + "_hh", datetime.getHourOfDay),
          (fieldName + "_dow", datetime.getDayOfWeek)
        )
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aDate for field $fieldName at line $lineNo")
          if (throwException)
            throw t
          else
            Seq.empty
        }
      }
  }
}

class DateMissingReader(fieldName: String,
                        fieldIndex: Int,
                        pattern: String,
                        throwException: Boolean,
                        missing: String) extends FieldReader with Logging {

  val parser = DateTimeFormat.forPattern(pattern).withZoneUTC()
  val formatter = ISODateTimeFormat.dateTime()
  val missingDate = parser.parseDateTime(missing)
  val missingSeq = Seq(
    (fieldName, formatter.print(missingDate.getMillis)),
    (fieldName + "_yy", missingDate.getYear),
    (fieldName + "_mm", missingDate.getMonthOfYear),
    (fieldName + "_dd", missingDate.getDayOfMonth),
    (fieldName + "_hh", missingDate.getHourOfDay),
    (fieldName + "_dow", missingDate.getDayOfWeek)
  )

  override def readField(splits: Array[String], lineno: Long): Seq[(String, Any)] = {
    val aDate = splits(fieldIndex).toUpperCase
    if (aDate.isEmpty)
      missingSeq
    else if (aDate.startsWith("NULL"))
      missingSeq
    else if (aDate.startsWith("UNDEFINED"))
      missingSeq
    else
      try {
        val datetime = parser.parseDateTime(aDate)
        Seq(
          (fieldName, formatter.print(datetime.getMillis)),
          (fieldName + "_yy", datetime.getYear),
          (fieldName + "_mm", datetime.getMonthOfYear),
          (fieldName + "_dd", datetime.getDayOfMonth),
          (fieldName + "_hh", datetime.getHourOfDay),
          (fieldName + "_dow", datetime.getDayOfWeek)
        )
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aDate for field $fieldName at line $lineno")
          if (throwException)
            throw t
          else
            missingSeq
        }
      }
  }
}

class DateReaderFactory(name: String, index: Int, pattern: String, throwException: Boolean) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new DateReader(name, index, pattern, throwException)
  }
}

class DateMissingReaderFactory(name: String, index: Int, pattern: String, throwException: Boolean, missing: String) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new DateMissingReader(name, index, pattern, throwException, missing)
  }
}

object DateReaderFactory extends Logging with Serializable {
  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    val throwException = conf.getBoolean("error.exception", true)
    splits match {
      case Array(_, name, index, pattern) => new DateReaderFactory(name, index.toInt, pattern, throwException)
      case Array(_, name, index, pattern, missing) => new DateMissingReaderFactory(name, index.toInt, pattern, throwException, missing)
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}
