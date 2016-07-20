package com.esri

import com.esri.hex.HexGrid
import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer

/**
  */
trait FieldValue extends Serializable {
  def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[(String, Any)]
}

case class FieldString(splits: Array[String]) extends FieldValue {
  val fieldName = splits(1)
  val index = splits(2).toInt

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[Pair[String, Any]] = {
    Seq((fieldName, splits(index)))
  }
}

case class FieldInt(splits: Array[String]) extends FieldValue with Logging {
  val fieldName = splits(1)
  val index = splits(2).toInt

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[Pair[String, Any]] = {
    val aInt = splits(index)
    if (aInt.isEmpty)
      Seq.empty
    else if (aInt.toLowerCase.startsWith("null"))
      Seq.empty
    else
      try {
        Seq((fieldName, aInt.toInt))
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aInt for field $fieldName at line $lineno")
          if (throwException)
            throw t
          else
            Seq.empty
        }
      }
  }
}

case class FieldFloat(splits: Array[String]) extends FieldValue with Logging {
  val fieldName = splits(1)
  val index = splits(2).toInt

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[Pair[String, Any]] = {
    val aDouble = splits(index)
    if (aDouble.isEmpty)
      Seq.empty
    else if (aDouble.toLowerCase.startsWith("null"))
      Seq.empty
    else
      try {
        Seq((fieldName, aDouble.toDouble))
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aDouble for field $fieldName at line $lineno")
          if (throwException)
            throw t
          else
            Seq.empty
        }
      }
  }
}

/**
  * date-time,[property_name],[date_index],[time_index],YYYY-MM-dd,HH:mm:ss
  *
  * @param splits
  */
case class FieldDateTime(splits: Array[String]) extends FieldValue with Logging {

  val fieldName = splits(1)
  val indexDate = splits(2).toInt
  val indexTime = splits(3).toInt
  @transient
  lazy val parser = DateTimeFactory.forPattern(splits(4) + " " + splits(5))

  @transient
  lazy val formatter = DateTimeFactory.forPattern("YYYY-MM-dd HH:mm:ss")

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[(String, Any)] = {
    val aDate = splits(indexDate)
    val aTime = splits(indexTime)
    if (aDate.isEmpty || aTime.isEmpty || aDate.toLowerCase.startsWith("null"))
      Seq.empty
    else {
      try {
        val datetime = parser.parseDateTime(aDate + " " + aTime)
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
            Seq.empty
        }
      }
    }
  }
}

case class FieldDate(splits: Array[String]) extends FieldValue with Logging {

  val fieldName = splits(1)
  val index = splits(2).toInt
  @transient
  lazy val parser = DateTimeFactory.forPattern(splits(3))
  @transient
  lazy val formatter = DateTimeFactory.forPattern("YYYY-MM-dd HH:mm:ss")

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[(String, Any)] = {
    val aDate = splits(index)
    if (aDate.isEmpty || aDate.toLowerCase.startsWith("null"))
      Seq.empty
    else {
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
            Seq.empty
        }
      }
    }
  }
}

case class FieldDateOnly(splits: Array[String]) extends FieldValue with Logging {

  val fieldName = splits(1)
  val index = splits(2).toInt
  @transient
  lazy val parser = DateTimeFactory.forPattern(splits(3))
  @transient
  lazy val formatter = DateTimeFactory.forPattern("YYYY-MM-dd HH:mm:ss")

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[(String, Any)] = {
    val aDate = splits(index)
    if (aDate.isEmpty || aDate.toLowerCase.startsWith("null"))
      Seq.empty
    else
      try {
        val datetime = parser.parseDateTime(aDate)
        Seq(
          (fieldName, formatter.print(datetime.getMillis))
        )
      } catch {
        case t: Throwable => {
          log.error(s"Cannot parse $aDate for field $fieldName at line $lineno")
          if (throwException)
            throw t
          else
            Seq.empty
        }
      }
  }
}

case class FieldGeo(conf: Map[String, String], splits: Array[String]) extends FieldValue with Logging {

  val fieldName = splits(1)
  val indexLon = splits(2).toInt
  val indexLat = splits(3).toInt

  val xmin = conf.getOrElse("xmin", "-180.0").toDouble
  val ymin = conf.getOrElse("ymin", "-90.0").toDouble
  val xmax = conf.getOrElse("xmax", "180.0").toDouble
  val ymax = conf.getOrElse("ymax", "90.0").toDouble

  val hexSizes = conf.getOrElse("hex.sizes", "100,100")
  val hexGrids = hexSizes.split(';').map(hexDef => {
    val tokens = hexDef.split(',')
    (fieldName + "_" + tokens(0), HexGrid(tokens(1).toDouble, 0.0, 0.0))
  })

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[(String, Any)] = {

    val list = ArrayBuffer[(String, Any)]()

    val aLon = splits(indexLon)
    val aLat = splits(indexLat)
    try {
      val lon = aLon.toDouble
      val lat = aLat.toDouble

      if (xmin <= lon && lon <= xmax && ymin <= lat && lat <= ymax) {

        list.append((fieldName, "%.6f,%.6f".format(lat, lon)))

        val xMercator = WebMercator.longitudeToX(lon)
        val yMercator = WebMercator.latitudeToY(lat)

        list.append((fieldName + "_x", xMercator))
        list.append((fieldName + "_y", yMercator))

        hexGrids.foreach { case (hexKey, hexVal) => {
          list.append((hexKey, hexVal.convertXYToRowCol(xMercator, yMercator).toText))
        }
        }
      } else {
        log.error(s"($lon,$lat) is not in ($xmin,$ymin,$xmax,$ymax)")
        if (throwException)
          throw new Exception()
      }
    } catch {
      case t: Throwable => {
        log.error(s"Cannot parse $aLon or $aLat for field $fieldName at line $lineno")
        if (throwException)
          throw t
      }
    }

    list
  }
}

case class FieldGrid(conf: Map[String, String], splits: Array[String]) extends FieldValue with Logging {

  val fieldName = splits(1)
  val indexLon = splits(2).toInt
  val indexLat = splits(3).toInt
  val gridSize = splits(4).toDouble

  val xmin = conf.getOrElse("xmin", "-180.0").toDouble
  val ymin = conf.getOrElse("ymin", "-90.0").toDouble
  val xmax = conf.getOrElse("xmax", "180.0").toDouble
  val ymax = conf.getOrElse("ymax", "90.0").toDouble

  override def parse(splits: Array[String], lineno: Long, throwException: Boolean): Seq[(String, Any)] = {

    val aLon = splits(indexLon)
    val aLat = splits(indexLat)
    try {
      val lon = aLon.toDouble
      val lat = aLat.toDouble

      if (xmin <= lon && lon <= xmax && ymin <= lat && lat <= ymax) {

        val xMercator = WebMercator.longitudeToX(lon)
        val yMercator = WebMercator.latitudeToY(lat)
        val gx = math.floor(xMercator / gridSize).toInt
        val gy = math.floor(yMercator / gridSize).toInt

        Seq(
          fieldName -> "%.6f,%.6f".format(lat, lon),
          fieldName + "_x" -> xMercator,
          fieldName + "_y" -> yMercator,
          fieldName + "_g" -> s"$gx:$gy"
        )

      } else {
        log.error(s"($lon,$lat) is not in ($xmin,$ymin,$xmax,$ymax)")
        if (throwException)
          throw new Exception()
        else
          Seq.empty
      }
    } catch {
      case t: Throwable => {
        log.error(s"Cannot parse $aLon or $aLat for field $fieldName at line $lineno")
        if (throwException)
          throw t
        else
          Seq.empty
      }
    }
  }
}
