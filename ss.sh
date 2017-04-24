#!/usr/bin/env bash
~/spark-1.6.2-bin-hadoop2.6/bin/spark-submit\
 target/spark-csv-es-3.0.4.jar\
 ~/Downloads/hdfsCSV.properties
