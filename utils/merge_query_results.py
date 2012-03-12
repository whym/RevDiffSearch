#! /usr/bin/env python

import csv
import sys
import glob
import json
import argparse
import pylab as plt
import matplotlib
from datetime import datetime

def chart(entries, output, legends, meta):
    # apply labels etc
    if not meta.has_key('labels') or len(meta['labels']) < 2:
        meta['labels'] = ['X', 'Y']
    plt.xlabel(meta['labels'][0])
    plt.ylabel(meta['labels'][1])

    if meta.has_key('xlim'):
        plt.xlim(tuple(meta['xlim']))
    if meta.has_key('ylim'):
        plt.ylim(tuple(meta['ylim']))

    lines = []
    for i in xrange(0, len(entries[0][1])):
        print i
        lines.append(plt.plot([x[0] for x in entries], [x[1][i] for x in entries], '-o'))
    plt.legend(tuple(lines), tuple(legends))
    plt.savefig(output)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='name of the result csv to be written')
    parser.add_argument('-c', '--chart', metavar='FILE',
                        dest='chart', type=lambda x: open(x, 'w'), default=None,
                        help='name of the result chart to be written')
    parser.add_argument('-s', '--font-size', metavar='POINT',
                      dest='fsize', type=int, default=15,
                      help='')
    parser.add_argument('-v', '--verbose',
                      dest='verbose', action='store_true', default=False,
                      help='turn on verbose message output')
    parser.add_argument('inputs', nargs='*')
    options = parser.parse_args()
    
    csv.field_size_limit(1000000000)

    # extract paths resolving globs
    paths = [glob.glob(x) for x in options.inputs]
    if all([len(x) <= 1 for x in paths]):
        options.inputs = ' '.join(options.inputs)
        paths = [[x[0] for x in paths if len(x) == 1]]

    entries = {}
    for (i,path) in enumerate(paths):
        for f in path:
            for row in list(csv.reader(open(f))):

                # remove comments
                if row[0].find('#') >= 0:
                    row = [row[0].split('#')[0]]

                # skip if it doesn't look like a proper entry
                if len(row) < 3:
                    if options.verbose:
                        print >>sys.stderr, 'skip %s' % repr(row)
                    continue

                # adjust the date string (dirty hack)
                k = row[0].replace('-', '/')
                if k.count('/') == 1:
                    k += '/15'

                entries.setdefault(k, [set()]*len(paths))
                entries[k][i] = entries[k][i].union(set([int(x) for x in row[2].split(';')]))

    writer = csv.writer(options.output)

    entries = sorted(entries.items(), key=lambda x: x[0])
    entries = [(x[0], [len(y) for y in x[1]])  for x in entries]
    for ent in entries:
        writer.writerow([ent[0]] + ent[1])
    
    parse_date = lambda x: datetime.strptime(str(x), '%Y/%m/%d')
    if options.chart != None:
        #matplotlib.rc('font', size=options.fsize)
        chart([(parse_date(x[0]), [float(y) for y in x[1]]) for x in entries], options.chart, options.inputs, {})
