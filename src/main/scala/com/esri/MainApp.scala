package com.esri

import java.io.{File, FileReader}
import java.util.Properties

import com.esri.hex.{Hex00, HexGrid, HexRowCol, HexXY}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark._

import scala.collection.JavaConverters._

/**
  */
object MainApp extends App with Logging {

  val sparkConf = new SparkConf()
    .setAppName(MainApp.getClass.getSimpleName)
    .registerKryoClasses(Array(
      classOf[DateMissingReaderFactory],
      classOf[DateMissingReader],
      classOf[DateReader],
      classOf[DateReaderFactory],
      DateReaderFactory.getClass,
      DateISOReaderFactory.getClass,
      classOf[FieldReader],
      classOf[FieldReaderFactory],
      classOf[NoopReaderFactory],
      classOf[NumeMissingReader],
      classOf[NumeMissingReaderFactory],
      classOf[NumeReader],
      classOf[NumeReaderFactory],
      classOf[RealMissingReader],
      classOf[RealMissingReaderFactory],
      classOf[RealReader],
      classOf[RealReaderFactory],
      classOf[GeoConf],
      classOf[GeoReader],
      classOf[GeoReaderFactory],
      classOf[CSVReader],
      classOf[HexGrid],
      classOf[HexRowCol],
      classOf[HexXY],
      classOf[Hex00])
    )

  val filename = args.length match {
    case 0 => "application.properties"
    case _ => args(0)
  }
  val file = new File(filename)
  if (file.exists()) {
    val reader = new FileReader(file)
    try {
      val properties = new Properties()
      properties.load(reader)
      properties.asScala
        .map {
          case (k, v) => k -> v.trim
        }
        .foreach { case (k, v) => {
          if (v.startsWith("${") && v.endsWith("}")) {
            val envKey = v.substring(2, v.length - 1)
            val envVal = scala.util.Properties.envOrElse(envKey, v)
            sparkConf.set(k, envVal)
          }
          else
            sparkConf.set(k, v)
        }
        }
    }
    finally {
      reader.close()
    }
  }

  val sc = new SparkContext(sparkConf)
  try {
    val conf = sc.getConf
    val confMap = conf.getAll.toMap

    val factories = conf.get("fields", "oid,object_id,-1")
      .split(';')
      .map(_.split(','))
      .filter(_.length > 2)
      .filterNot(splits => splits(2) == "-1")
      .map(splits => {
        splits.head match {
          case "geo" => GeoReaderFactory(splits, conf)
          case "nume" | "int" | "integer" => NumeReaderFactory(splits, conf)
          case "real" | "float" | "double" => RealReaderFactory(splits, conf)
          case "date" => DateReaderFactory(splits, conf)
          case "date-only" => DateOnlyReaderFactory(splits, conf)
          case "date-iso" => DateISOReaderFactory(splits, conf)
          case "date-only-iso" => DateOnlyISOReaderFactory(splits, conf)
          case _ => TextReaderFactory(splits)
        }
      })
    val acc = sc.accumulator[Int](0)
    val fieldSepProp = conf.get("field.sep", "\t")
    val fieldSep = if (fieldSepProp.startsWith("0")) Integer.parseInt(fieldSepProp, 16).toChar else fieldSepProp.charAt(0)
    val headerCount = conf.getInt("header.count", 0) - 1
    sc.textFile(conf.get("input.path"))
      .zipWithIndex()
      .filter(_._2 > headerCount)
      .mapPartitions(iter => {
        val csvReader = CSVReader(fieldSep)
        val fields = factories.map(_.createFieldReader())
        iter.flatMap { case (line, lineno) => {
          try {
            val splits = csvReader.parseCSV(line)
            Some(fields.flatMap(_.readField(splits, lineno)).toMap)
          }
          catch {
            case t: Throwable => {
              log.error(t.getMessage)
              acc += 1
              None
            }
          }
        }
        }
      })
      .filter(_.nonEmpty)
      .saveToEs(conf.get("index.mapping"))
    val value = acc.value
    if (value > 0)
      log.error("Error count = %d".format(value))
  }

  finally {
    sc.stop()
  }
}
