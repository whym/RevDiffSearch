import sys
import csv

csv.field_size_limit(1000000000)
reader = csv.reader(sys.stdin)
for cols in reader:
    if len(cols) != 3:
        continue
    for x in cols[2].split(';'):
        if len(x) < 30 and x.isdigit:
            print x
