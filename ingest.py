#!/usr/bin/env python

import os
import subprocess

host = 'vega.cs.washington.edu:3001'
INT = "INT_TYPE"
DOUBLE = "DOUBLE_TYPE"
STRING = "STRING_TYPE"
DOUBLE_STRING = "STRING_TYPE"
LONG = "LONG_TYPE"

TYPE_MAP = {'int': INT,
            'varchar' : STRING,
            'bit': INT,
            'decimal': DOUBLE,
            'bigint': LONG,
            'money': DOUBLE,
            'uniqueidentifier': STRING,
            'datetime': STRING,
            'smallint': INT,
            'text': STRING,
            }

def convert_type(s):
    return TYPE_MAP[s]

def load_schema(phile):
    schema = []
    with open(phile) as fh:
        for line in fh:
            toks = line.split()
            assert len(toks) == 2
            colname = toks[0]
            if toks[1].startswith('null'):
                coltype = STRING
            else:
                coltype = convert_type(toks[1])
            schema.append((colname, coltype))

    return schema

def do_create(table_name, schema):
    names, types = zip(*schema)
    names_str = ','.join(['"%s"' % s for s in names])
    types_str = ','.join(['"%s"' % s for s in types])

    json = """{
      "relationKey" : {
        "userName" : "public",
        "programName" : "adhoc",
        "relationName" : "%s"
      },
      "schema" : {
        "columnNames" : [%s],
        "columnTypes" : [%s]
      },
      "source" : {
        "dataType" : "Empty"
      }
    }""" % (table_name, names_str, types_str)
    print json

    subprocess.call(['curl', '-i', '-XPOST', '%s/dataset' % host,
                     '-H','Content-type: application/json',
                     '-H', 'Myria-Auth: DEF459', '-d',  json])

def do_ingest(table_name, phile, _format='csv'):
    url = '%s/dataset/user-public/program-adhoc/relation-%s/data?format=%s' % (
        host, table_name, _format)
    args = ['curl', '-i', '-XPUT', url,
            '-H', 'Content-type: application/octet-stream',
            '-H', 'Myria-Auth: DEF459',
            '--data-binary', '@%s' % phile]
    print ' '.join(args)
    subprocess.call(args)

tables = ['txtype', 'txdetail', 'product', 'txheader', 'store']

for table in tables:
    phile = table + '_schema.txt'
    schema = load_schema(phile)
    print schema

    do_create(table, schema)


    if os.path.exists(table + '.tsv'):
        do_ingest(table, table + '.tsv', 'tsv')
    else:
        do_ingest(table, table + '.csv')
