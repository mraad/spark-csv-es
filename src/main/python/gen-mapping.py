#!/usr/bin/env python
#
# pip install pyjavaproperties
#
import json
import os

import sys
from pyjavaproperties import Properties


def main():
    if len(sys.argv) == 1:
        sys.exit('Usage: {} <file.properties>'.format(sys.argv[0]))

    if not os.path.exists(sys.argv[1]):
        sys.exit('Error: {} does not exist !'.format(sys.argv[1]))

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
            properties[name + "_xm"] = {"type": "float", "index": "no"}
            properties[name + "_ym"] = {"type": "float", "index": "no"}
            for hs in hex_sizes:
                properties[name + "_" + hs] = {"type": "string", "index": "not_analyzed"}
        elif tokens[0] == 'grid':
            name = tokens[1]
            properties[name] = {"type": "geo_point"}
            properties[name + "_xm"] = {"type": "float", "index": "no"}
            properties[name + "_ym"] = {"type": "float", "index": "no"}
            properties[name + "_g"] = {"type": "string", "index": "not_analyzed"}
        elif tokens[0] == 'int':
            properties[tokens[1]] = {"type": "integer"}
        elif tokens[0] == 'long':
            properties[tokens[1]] = {"type": "long"}
        elif tokens[0] == 'float':
            properties[tokens[1]] = {"type": "float"}
        elif tokens[0] == 'double':
            properties[tokens[1]] = {"type": "float"}
        elif tokens[0] == 'date' or tokens[0] == 'date-time':
            name = tokens[1]
            date_format = tokens[3] if len(tokens) == 4 else "YYYY-MM-dd HH:mm:ss"
            properties[name] = {"type": "date", "format": date_format}
            properties[name + "_yy"] = {"type": "integer"}
            properties[name + "_mm"] = {"type": "integer"}
            properties[name + "_dd"] = {"type": "integer"}
            properties[name + "_hh"] = {"type": "integer"}
            properties[name + "_dow"] = {"type": "integer"}
        elif tokens[0] == 'date-iso':
            name = tokens[1]
            date_format = tokens[3] if len(tokens) == 4 else "date_optional_time"
            properties[name] = {"type": "date", "format": date_format}
            properties[name + "_yy"] = {"type": "integer"}
            properties[name + "_mm"] = {"type": "integer"}
            properties[name + "_dd"] = {"type": "integer"}
            properties[name + "_hh"] = {"type": "integer"}
            properties[name + "_dow"] = {"type": "integer"}
        elif tokens[0] == 'date-only':
            name = tokens[1]
            date_format = tokens[3] if len(tokens) == 4 else "YYYY-MM-dd HH:mm:ss"
            properties[name] = {"type": "date", "format": date_format}
        else:
            name = tokens[1]
            properties[name] = {"type": "string", "index": "not_analyzed"}

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

    basename, extension = os.path.splitext(sys.argv[1])
    with open(basename + ".json", "wb") as fw:
        fw.write(json.dumps(doc, ensure_ascii=False, indent=2))

    base = os.path.basename(sys.argv[1])
    name = os.path.splitext(base)[0]
    print("Post Example: curl -XPOST localhost:9200/{}?pretty -d @{}.json".format(name.lower(), basename))


if __name__ == '__main__':
    main()
