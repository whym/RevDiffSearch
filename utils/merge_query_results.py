#! /usr/bin/env python

import csv
import sys
import glob
import json
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', metavar='FILE',
                        dest='output', type=lambda x: open(x, 'w'), default=sys.stdout,
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
                if row[0].find('#') >= 0:
                    row = [row[0].split('#')[0]]
                if len(row) < 3:
                    if options.verbose:
                        print >>sys.stderr, 'skip %s' % repr(row)
                    continue
                k = row[0].replace('-', '/')
                entries.setdefault(k, [set()]*len(paths))
                entries[k][i] = entries[k][i].union(set([int(x) for x in row[2].split(';')]))

    writer = csv.writer(options.output)
    print >>options.output, '# %s' % json.dumps({'title': 'hits', 'labels': options.inputs})
    for ent in sorted(entries.items(), key=lambda x: x[0]):
        writer.writerow([ent[0]] + [len(x) for x in ent[1]])
