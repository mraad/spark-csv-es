#!/usr/bin/env bash
set -x
export ES_NODE=${ES_PORT_9200_TCP_ADDR:-localhost}
curl -XDELETE ${ES_NODE}:9200/sample?pretty
curl -XPOST ${ES_NODE}:9200/sample?pretty -d @sample.json
if [ ${ES_NODE} == 'localhost' ]; then
    awk -f sample.awk > /tmp/sample.csv
else
    awk -f sample.awk | hdfs dfs -put - sample.csv
fi
