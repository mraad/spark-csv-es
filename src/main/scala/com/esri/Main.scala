package com.esri

import java.io.{File, FileReader}
import java.util.Properties

import com.esri.hex.{Hex00, HexGrid, HexRowCol, HexXY}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark._

import scala.collection.JavaConverters._

/**
  */
object Main extends App with Logging {

  val sparkConf = new SparkConf()
    .setAppName(Main.getClass.getSimpleName)
    .setMaster("local[*]")
    .set("spark.driver.memory", "16g")
    .set("spark.executor.memory", "16g")
    // .set(ConfigurationOptions.ES_NODES, "local192")
    // .set(ConfigurationOptions.ES_MAPPING_ID, "object_id")
    // .set(ConfigurationOptions.ES_WRITE_OPERATION, ConfigurationOptions.ES_OPERATION_UPSERT)
    .registerKryoClasses(Array(
      classOf[FieldDate],
      classOf[FieldDateTime],
      classOf[FieldDateOnly],
      classOf[FieldValue],
      classOf[FieldFloat],
      classOf[FieldInt],
      classOf[FieldGeo],
      classOf[FieldString],
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
      properties.asScala.foreach { case (k, v) => {
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

    val fields = conf.get("fields", "oid,object_id,-1")
      .split(';')
      .map(_.split(','))
      .filterNot(splits => splits(2) == "-1")
      .map(splits => {
        splits(0) match {
          case "geo" => FieldGeo(confMap, splits)
          case "int" => FieldInt(splits)
          case "float" => FieldFloat(splits)
          case "date" => FieldDate(splits)
          case "date-time" => FieldDateTime(splits)
          case "date-only" => FieldDateOnly(splits)
          case _ => FieldString(splits)
        }
      })
    val acc = sc.accumulator[Int](0)
    val fieldSep = conf.get("field.sep", "\t")(0)
    val csvReader = new CSVReader(fieldSep)
    sc.textFile(conf.get("input.path"))
      .flatMap(line => {
        try {
          val splits = csvReader.parseCSV(line)
          Some(fields.flatMap(_.parse(splits)).toMap)
        }
        catch {
          case t: Throwable => {
            // log.warn(t.getMessage)
            acc += 1
            None
          }
        }
      })
      .saveToEs(conf.get("index.mapping"))
    log.info("Throwable count = %d".format(acc.value))
  } finally {
    sc.stop()
  }
}
