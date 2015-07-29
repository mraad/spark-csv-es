package com.esri

import com.esri.hex.HexGrid

import scala.collection.mutable.ArrayBuffer

/**
  */
trait FieldValue extends Serializable {
  def parse(splits: Array[String]): Seq[(String, Any)]
}

case class FieldString(splits: Array[String]) extends FieldValue {
  val fieldName = splits(1)
  val index = splits(2).toInt

  override def parse(splits: Array[String]): Seq[Pair[String, Any]] = {
    Seq((fieldName, splits(index)))
  }
}

case class FieldInt(splits: Array[String]) extends FieldValue {
  val fieldName = splits(1)
  val index = splits(2).toInt
  // val missing = if (splits.length == 4) splits(3).toInt else -9999

  override def parse(splits: Array[String]): Seq[Pair[String, Any]] = {
    val aInt = splits(index)
    if (aInt.length == 0)
      Seq.empty
    else
      Seq((fieldName, aInt.toInt))
  }
}

case class FieldFloat(splits: Array[String]) extends FieldValue {
  val fieldName = splits(1)
  val index = splits(2).toInt
  // val missing = if (splits.length == 4) splits(3).toDouble else -9999.0

  override def parse(splits: Array[String]): Seq[Pair[String, Any]] = {
    val aDouble = splits(index)
    if (aDouble.length == 0)
      Seq.empty
    else
      Seq((fieldName, aDouble.toDouble))
  }
}

case class FieldDate(splits: Array[String]) extends FieldValue {

  val fieldName = splits(1)
  val index = splits(2).toInt
  @transient
  lazy val parser = DateTimeFactory.forPattern(splits(3))
  @transient
  lazy val formatter = DateTimeFactory.forPattern("YYYY-MM-dd HH:mm:ss")

  override def parse(splits: Array[String]): Seq[(String, Any)] = {
    val aDate = splits(index)
    if (aDate.length == 0)
      Seq.empty
    else {
      val datetime = parser.parseDateTime(aDate)
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
}

case class FieldDateOnly(splits: Array[String]) extends FieldValue {

  val fieldName = splits(1)
  val index = splits(2).toInt
  @transient
  lazy val parser = DateTimeFactory.forPattern(splits(3))
  @transient
  lazy val formatter = DateTimeFactory.forPattern("YYYY-MM-dd HH:mm:ss")

  override def parse(splits: Array[String]): Seq[(String, Any)] = {
    val datetime = parser.parseDateTime(splits(index))
    Seq(
      (fieldName, formatter.print(datetime.getMillis))
    )
  }
}

case class FieldGeo(conf: Map[String, String], splits: Array[String]) extends FieldValue {

  val fieldName = splits(1)
  val indexLon = splits(2).toInt
  val indexLat = splits(3).toInt

  val xmin = conf.getOrElse("xmin", "-180.0").toDouble
  val ymin = conf.getOrElse("ymin", "-90.0").toDouble
  val xmax = conf.getOrElse("xmax", "180.0").toDouble
  val ymax = conf.getOrElse("ymax", "90.0").toDouble

  val hexOrigX = conf.getOrElse("hex.orig.x", "0.0").toDouble
  val hexOrigY = conf.getOrElse("hex.orig.y", "0.0").toDouble
  val hexSizes = conf.getOrElse("hex.sizes", "100,100.0")
  val hexGrids = hexSizes.split(';').map(hexDef => {
    val tokens = hexDef.split(',')
    (fieldName + "_" + tokens(0), HexGrid(tokens(1).toDouble, hexOrigX, hexOrigY))
  })

  override def parse(splits: Array[String]): Seq[(String, Any)] = {

    val list = ArrayBuffer[(String,Any)]()

    val lon = splits(indexLon).toDouble
    val lat = splits(indexLat).toDouble

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
    }
    else {
      throw new Exception("Not in valid range")
    }

    list
  }
}