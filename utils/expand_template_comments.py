#! /usr/bin/env python

import sys
import csv

writer = csv.writer(sys.stdout)
for line in sys.stdin.readlines():
    lower = line.strip().lower()
    upper = lower.capitalize()
    writer.writerow(['"<!-- Template:%s -->" OR "<!-- Template:%s -->" OR "<!-- Added using Template:%s -->" OR "<!-- Added using Template:%s -->"' % (lower, upper, lower, upper), 'results/%s.txt' % lower])
