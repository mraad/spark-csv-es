#!/usr/bin/env bash

export ES_NODE=${ES_PORT_9200_TCP_ADDR:-localhost}
curl -XDELETE ${ES_NODE}:9200/sample?pretty
curl -XPOST ${ES_NODE}:9200/sample?pretty -d @data/sample.json

time ~/spark-1.6.2-bin-hadoop2.6/bin/spark-submit\
 --master local[*]\
 --executor-memory 16G\
 --conf spark.ui.enabled=false\
 --conf spark.ui.showConsoleProgress=true\
 --conf spark.es.batch.size.bytes=10m\
 --conf spark.es.batch.size.entries=0\
 --conf spark.es.batch.write.refresh=false\
 --conf spark.es.batch.write.retry.count=10\
 target/spark-csv-es-3.0.2.jar\
 data/sample.properties

curl -XGET ${ES_NODE}:9200/sample/_refresh?pretty
