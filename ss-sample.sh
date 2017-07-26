#!/usr/bin/env bash

export ES_NODES=${ES_PORT_9200_TCP_ADDR:-localhost}
# curl -XDELETE ${ES_NODES}:9200/sample?pretty
# curl -XPUT ${ES_NODES}:9200/sample?pretty -d @data/sample.json

spark-submit\
 --master yarn\
 --num-executors 2\
 --executor-cores 2\
 --executor-memory 2G\
 --conf spark.ui.enabled=false\
 --conf spark.ui.showConsoleProgress=false\
 --conf spark.es.batch.size.bytes=10m\
 --conf spark.es.batch.size.entries=0\
 --conf spark.es.batch.write.refresh=false\
 --conf spark.es.batch.write.retry.count=10\
 target/spark-csv-es-5.1.jar\
 data/sample.properties

curl -XGET $ES_NODES:9200/sample/_refresh?pretty
curl -XGET $ES_NODES:9200/sample/_count?pretty
