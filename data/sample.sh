#!/usr/bin/env bash
export ES_NODES=${ES_PORT_9200_TCP_ADDR:-localhost}
curl -XDELETE ${ES_NODES}:9200/sample?pretty
curl -XPOST ${ES_NODES}:9200/sample?pretty -d @sample.json
awk -f sample.awk | hdfs dfs -put - sample.csv
# awk -f sample.awk > /tmp/sample.csv
