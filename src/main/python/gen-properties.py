#!/usr/bin/env python

import os

import sys


def main():
    if len(sys.argv) == 1:
        sys.exit('Usage: %s <file.csv>' % sys.argv[0])

    if not os.path.exists(sys.argv[1]):
        sys.exit('Error: %s does not exist !' % sys.argv[1])

    with open(sys.argv[1], "rb") as fr:
        header = fr.readline().rstrip()

        field_sep = ','
        fields = header.split(field_sep)
        arr = []
        index = 0
        for field in fields:
            lcase = field.lower()
            arr.append("string,{},{}".format(lcase, index))
            index += 1

        join = ";\\\n  ".join(arr)

        basename, extension = os.path.splitext(sys.argv[1])
        with open(basename + ".properties", "wb") as fw:
            fw.write("es.nodes=localhost\n")
            fw.write("input.path={}\n".format(sys.argv[1]))
            fw.write("field.sep={}\n".format(field_sep))
            fw.write("fields={}\n".format(join))


if __name__ == '__main__':
    main()
