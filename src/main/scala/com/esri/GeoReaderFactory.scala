package com.esri

import com.esri.hex.HexGrid
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable.ArrayBuffer

case class GeoConf(xmin: Double, ymin: Double,
                   xmax: Double, ymax: Double,
                   gridX: Double, gridY: Double,
                   hexGrids: Seq[(String, HexGrid)],
                   throwException: Boolean
                  )

class GeoReader(fieldName: String,
                indexLon: Int,
                indexLat: Int,
                conf: GeoConf
               ) extends FieldReader with Logging {

  override def readField(splits: Array[String], lineNo: Long): Seq[(String, Any)] = {

    val list = ArrayBuffer[(String, Any)]()

    val aLon = splits(indexLon)
    val aLat = splits(indexLat)
    try {
      val lon = aLon.toDouble
      val lat = aLat.toDouble

      if (conf.xmin <= lon && lon <= conf.xmax && conf.ymin <= lat && lat <= conf.ymax) {

        // TODO - Make configurable
        list.append((fieldName, "%.6f,%.6f".format(lat, lon)))

        val xMercator = WebMercator.longitudeToX(lon)
        val yMercator = WebMercator.latitudeToY(lat)

        // TODO - Make configurable
        list.append((fieldName + "_xm", xMercator))
        list.append((fieldName + "_ym", yMercator))

        conf.hexGrids.foreach { case (hexKey, hexVal) => {
          list.append((hexKey, hexVal.convertXYToRowCol(xMercator, yMercator).toText))
        }
        }
      } else {
        log.error(s"($lon,$lat) is not in (${conf.xmin},${conf.ymin},${conf.xmax},${conf.ymax})")
        if (conf.throwException)
          throw new Exception()
      }
    } catch {
      case t: Throwable => {
        log.error(s"Cannot parse $aLon or $aLat for field $fieldName at line $lineNo")
        if (conf.throwException)
          throw t
      }
    }

    list
  }
}

class GeoReaderFactory(name: String,
                       lonIndex: Int,
                       latIndex: Int,
                       conf: GeoConf
                      ) extends FieldReaderFactory {
  override def createFieldReader(): FieldReader = {
    new GeoReader(name, lonIndex, latIndex, conf)
  }
}

object GeoReaderFactory extends Logging with Serializable {
  def apply(splits: Array[String], conf: SparkConf): FieldReaderFactory = {
    splits match {
      case Array(_, name, lonIndex, latIndex) => {
        val gridX = conf.getDouble("hex.x", 0.0)
        val gridY = conf.getDouble("hex.y", 0.0)
        val hexGrids = conf
          .get("hex.sizes", "100,100")
          .split(';')
          .map(hexDef => {
            val tokens = hexDef.split(',')
            (name + "_" + tokens.head, HexGrid(tokens.last.toDouble, gridX, gridY))
          })
        val geoConf = GeoConf(
          conf.getDouble("xmin", -180.0),
          conf.getDouble("ymin", -90.0),
          conf.getDouble("xmax", 180.0),
          conf.getDouble("ymax", 90.0),
          gridX,
          gridY,
          hexGrids,
          conf.getBoolean("error.exception", true)
        )
        new GeoReaderFactory(name, lonIndex.toInt, latIndex.toInt, geoConf)
      }
      case _ => {
        log.warn("Skipping field - Invalid parameters {}", splits.mkString(","))
        NoopReaderFactory()
      }
    }
  }
}
