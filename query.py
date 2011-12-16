#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import ast
import json
import socket
import csv

def search(host, port, query):
    query = json.dumps(query)
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
    parser.add_argument('inputs', nargs='+')
    options = parser.parse_args()
    querystr = ' '.join(options.inputs)

    if options.verbose:
        print >>sys.stderr, querystr

    if not options.start and options.end:
        options.start = '0'     # some value lexicographically lesser than any year
    if options.start and not options.end:
        options.end = 'Z'       # some value lexicographically greater than any year
    if options.start and options.end:
        querystr += ' timestamp:[%s TO %s]' % (options.start, options.end)
    query = {'q': querystr, 'max_revs': options.maxrevs, 'collapse_hits': 'day' if options.daily else 'month', 'fields': ['rev_id'] if options.revisions else []}
    result = search('localhost', 8080, query)

    if options.verbose:
        print >>sys.stderr, result

    writer = csv.writer(options.output)
    for (date,res) in result['hits']:
        cols = [date]
        if type(res) == list:
            cols.append(len(res))
            cols += ['=HYPERLINK("http://enwp.org/?diff=%s","%s")' % (x[0],x[0]) for x in res] # assuming having received a list of rev ids
        else:
            cols.append(res)
        writer.writerow(cols)
    if options.debug:
        writer.writerow(['# raw: %s' % repr(result)])
        writer.writerow(['# elapsed: %s' % repr(result['elapsed'])])
        writer.writerow(['# 1st-pass precision: %f' % (result['hits_all'] / (result['debug_positives'] - result['debug_unchecked']))])
        writer.writerow(['# unchecked: %d' % result['debug_unchecked']])