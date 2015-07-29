#!/usr/bin/env python
#
# pip install pyjavaproperties
#
import os
import json

import sys
from pyjavaproperties import Properties


def main():
    if len(sys.argv) == 1:
        sys.exit('Usage: %s <file.properties>' % sys.argv[0])

    if not os.path.exists(sys.argv[1]):
        sys.exit('Error: %s does not exist !' % sys.argv[1])

    p = Properties()
    p.load(open(sys.argv[1]))

    hex_sizes = p['hex.sizes'].split(';')
    hex_sizes = [hs.split(',')[0] for hs in hex_sizes]
    mappings = p['index.mapping'].split('/')[1]

    properties = {}

    fields = p['fields']
    for field in fields.split(';'):
        tokens = field.split(',')
        if tokens[0] == 'geo':
            name = tokens[1]
            properties[name] = {"type": "geo_point"}
            properties[name + "_x"] = {"type": "float", "index": "no"}
            properties[name + "_y"] = {"type": "float", "index": "no"}
            for hs in hex_sizes:
                properties[name + "_" + hs] = {"type": "string", "index": "not_analyzed"}
        elif tokens[0] == 'int':
            properties[tokens[1]] = {"type": "integer"}
        elif tokens[0] == 'float':
            properties[tokens[1]] = {"type": "float"}
        elif tokens[0] == 'date':
            name = tokens[1]
            properties[name] = {"type": "date", "format": "YYYY-MM-dd HH:mm:ss"}
            properties[name + "_yy"] = {"type": "integer"}
            properties[name + "_mm"] = {"type": "integer"}
            properties[name + "_dd"] = {"type": "integer"}
            properties[name + "_hh"] = {"type": "integer"}
            properties[name + "_dow"] = {"type": "integer"}
        elif tokens[0] == 'date-only':
            name = tokens[1]
            properties[name] = {"type": "date", "format": "YYYY-MM-dd HH:mm:ss"}
        else:
            name = tokens[1]
            properties[name] = {"type": "string"}

    doc = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "-1",
            "auto_expand_replicas": "false"
        },
        "mappings": {mappings: {
            "_all": {
                "enabled": False
            },
            "_source": {
                "enabled": True
            },
            "properties": properties
        }}
    }

    print json.dumps(doc, ensure_ascii=False)


if __name__ == '__main__':
    main()
