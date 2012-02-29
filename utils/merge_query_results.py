#! /usr/bin/env python

import csv
import sys
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
                        help='')
    parser.add_argument('inputs', nargs='*')
    options = parser.parse_args()
    
    csv.field_size_limit(1000000000)
    entries = {}
    for f in options.inputs:
        for row in list(csv.reader(open(f))):
            if row[0].find('#') >= 0:
                row = [row[0].split('#')[0]]
            if len(row) < 2:
                print >>sys.stderr, 'skip %s' % repr(row)
                continue
            k = row[0].replace('-', '/')
            entries.setdefault(k, 0)
            entries[k] += int(row[1])

    writer = csv.writer(options.output)
    for ent in sorted(entries.items(), key=lambda x: x[0]):
        writer.writerow(ent)
