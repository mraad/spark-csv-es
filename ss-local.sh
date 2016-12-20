#!/usr/bin/env bash

export ES_NODES=${ES_PORT_9200_TCP_ADDR:-localhost}
curl -XDELETE ${ES_NODES}:9200/sample?pretty
curl -XPOST ${ES_NODES}:9200/sample?pretty -d @data/sample.json

time spark-submit\
 --master local[*]\
 --executor-memory 6G\
 --conf spark.ui.enabled=false\
 --conf spark.ui.showConsoleProgress=false\
 --conf spark.es.batch.size.bytes=10m\
 --conf spark.es.batch.size.entries=0\
 --conf spark.es.batch.write.refresh=false\
 --conf spark.es.batch.write.retry.count=10\
 target/spark-csv-es-2.4.8.jar\
 data/sample.properties
