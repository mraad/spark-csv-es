#!/usr/bin/env bash
export INDEX=dmat
cat << EOF > /tmp/${INDEX}.json
{
  "template": "dmat-*",
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.refresh_interval": "-1",
    "index.routing.allocation.require.box_type": "hot"
  },
  "mappings": {
    "dmat": {
      "_all": {
        "enabled": false
      },
      "properties": {
        "uuid": {
          "type": "string",
          "index": "not_analyzed"
        },
        "shape": {
          "type": "geo_point"
        },
        "dmat_date": {
          "type": "date"
        }
      }
    }
  }
}
EOF
curl -XDELETE "local192:9200/_template/${INDEX}?pretty"
curl -XPUT "local192:9200/_template/${INDEX}?pretty" -d @/tmp/${INDEX}.json
