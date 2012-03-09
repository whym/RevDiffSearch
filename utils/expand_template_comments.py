#! /usr/bin/env python

import sys
import csv

writer = csv.writer(sys.stdout)
for line in sys.stdin.readlines():
    lower = line.strip()
    if len(lower) == 0:
        continue
    lower = lower[0].lower() + lower[1:]
    upper = lower.capitalize()
    writer.writerow(['"<!-- Template:%s -->" OR "<!-- Template:%s -->" OR "<!-- Added using Template:%s -->" OR "<!-- Added using Template:%s -->"' % (lower, upper, lower, upper), 'results/%s.txt' % lower])
