#!/usr/bin/env bash
# set -x
export ES_NODE=${ES_PORT_9200_TCP_ADDR:-localhost}
echo "Deleting index..."
curl -XDELETE ${ES_NODE}:9200/sample?pretty
echo "Creating index..."
curl -XPUT ${ES_NODE}:9200/sample?pretty -d @sample.json
echo "Creating data..."
if [ ${ES_NODE} == 'localhost' ]; then
    awk -f sample.awk > /tmp/sample.csv
else
    awk -f sample.awk | hdfs dfs -put - /tmp/sample.csv
fi
