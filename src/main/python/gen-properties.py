#!/usr/bin/env python

import os
import string

import sys


def main():
    if len(sys.argv) != 2:
        sys.exit('Usage: {} your-file.csv'.format(sys.argv[0]))

    if not os.path.exists(sys.argv[1]):
        sys.exit('Error: {} does not exist !'.format(sys.argv[1]))

    with open(sys.argv[1], "rb") as fr:
        header = fr.readline().rstrip()

        field_sep = ','
        fields = header.split(field_sep)

        arr = ["geo,loc,-1"]
        index = 0
        for field in fields:
            # name = field.lower()
            # remove weird chars in headers
            name = ''.join(s for s in field.lower() if s in string.printable)
            arr.append("string,{0},{1}".format(name, index))
            index += 1

        join = ";\\\n  ".join(arr)

        base = os.path.basename(sys.argv[1])
        name = os.path.splitext(base)[0]
        fullpath = os.path.splitext(sys.argv[1])[0]
        with open(fullpath + ".properties", "wb") as fw:
            fw.write("spark.master=local[*]\n")
            fw.write("spark.app.id=CSV TO ES {0}\n".format(name))
            # fw.write("spark.driver.memory=8g\n")
            fw.write("spark.executor.memory=12g\n")
            fw.write("es.nodes=localhost\n")
            fw.write("index.mapping={0}/geo\n".format(name.lower()))
            fw.write("hex.sizes=10,10;25,25;50,50;100,100;200,200;500,500\n")
            fw.write("input.path={0}\n".format(sys.argv[1]))
            fw.write("field.sep={0}\n".format(field_sep))
            fw.write("fields={0}\n".format(join))


if __name__ == '__main__':
    main()
