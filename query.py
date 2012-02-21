#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import ast
import json
import socket
import csv

def search(host, port, query):
    query = json.dumps(query) + "\n"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.send(query)
    result = ''
    while 1:
        data = s.recv(1024, socket.MSG_WAITALL)
        if not data or len(data) == 0:
            break
        result += data
    s.close()
    return json.loads(result)

def format_query(querystr, options):
    if not options.advanced:
        querystr = '"%s"' % querystr.replace('"', '\\"')
    if not options.start and options.end:
        options.start = '0'     # some value lexicographically lesser than any year
    if options.start and not options.end:
        options.end = 'Z'       # some value lexicographically greater than any year
    if options.start and options.end:
        querystr += ' timestamp:[%s TO %s]' % (options.start, options.end)
    if options.namespace:
        querystr += ' namespace:' + options.namespace

    return {'q': querystr, 'max_revs': options.maxrevs, 'collapse_hits': 'day' if options.daily else 'month', 'fields': ['rev_id'] if options.revisions else []}

def format_result(writer, result, debug, hyperlink=False):
    for (date,res) in sorted(result['hits'], key=lambda x: x[0]):
        cols = [date]
        if type(res) == list:
            cols.append(len(res))
            if hyperlink:
                cols.append(';'.join(['=HYPERLINK("http://enwp.org/?diff=%s","%s")' % (x[0],x[0]) for x in res])) # assuming having received a list of rev ids
            else:
                cols.append(';'.join([x[0] for x in res]))
        else:
            cols.append(res)
        writer.writerow(cols)
    if debug:
        writer.writerow(['# raw: %s' % repr(result)])
        writer.writerow(['# elapsed: %s' % repr(result['elapsed'])])
        checked = result['debug_positives'] - result['debug_unchecked']
        if checked != 0:
            writer.writerow(['# 1st-pass precision: %f' % (result['hits_all'] / checked)])
        writer.writerow(['# unchecked: %d' % result['debug_unchecked']])

def load_queries(fname):
    queries = {}
    reader = csv.reader(open(fname))

    for ls in list(reader):
        q = ls[0]
        f = q.replace(':', '__').replace('!', '__').replace('<', '__').replace('>', '__')
        f += '.txt'
        if len(ls) >= 2:
            f = ls[1]
        queries[f] = q
    return queries

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', metavar='DATE',
                        dest='start', type=str, default=None,
                        help='')
    parser.add_argument('-e', '--end', metavar='END',
                        dest='end', type=str, default=None,
                        help='')
    parser.add_argument('-m', '--max', metavar='MAX_REVISIONS',
                        dest='maxrevs', type=int, default=10000,
                        help='')
    parser.add_argument('-n', '--namespace', metavar='NAMESPACE_ID',
                        dest='namespace', type=str, default=None),
    parser.add_argument('-r', '--removed',
                        dest='removed', action='store_true', default=False,
                        help='search for removed strings')
    parser.add_argument('-R', '--revisions',
                        dest='revisions', action='store_true', default=False,
                        help='show revision IDs of the hits')
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('-D', '--daily',
                        dest='daily', action='store_true', default=False,
                        help='')
    parser.add_argument('-v', '--verbose',
                        dest='verbose', action='store_true', default=False,
                        help='turn on verbose message output')
    parser.add_argument('-d', '--debug',
                        dest='debug', action='store_true', default=False,
                        help='turn on debug output')
    parser.add_argument('-H', '--hyperlink',
                        dest='hyperlink', action='store_true', default=False,
                        help='add hyperlinks to rev_ids')
    parser.add_argument('-q', '--query-file', metavar='FILE',
                        dest='queryfile', type=str, default=None,
                        help='load queries and output file names from a file')
    parser.add_argument('-a', '--advanced',
                        dest='advanced', action='store_true', default=False,
                        help='turn on advanced query format')
    parser.add_argument('inputs', nargs='*')
    options = parser.parse_args()
    queries = {options.output: ' '.join(options.inputs)}
    if options.queryfile:
        queries = load_queries(options.queryfile)
    if len(queries.items()) == 0:
        print >>sys.stderr, 'no query'
        exit(1)

    if options.verbose:
        print >>sys.stderr, queries

    for (output, query) in queries.items():
        result = search('localhost', 8080, format_query(query, options))

        if options.verbose:
            print >>sys.stderr, result

        if type(output) == str:
            output = open(output, 'w')
        writer = csv.writer(output)
        if options.debug:
            writer.writerow(['# query: %s, options: %s', query, repr(options)])
        format_result(writer, result, debug=options.debug, hyperlink=options.hyperlink)
