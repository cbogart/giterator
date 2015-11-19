import read_ga
import os
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
import jsonpath_rw
import csv

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

csv.field_size_limit(10000000)
csvreader = csv.reader(open(sys.argv[1], "rb"))
csvwriter = csv.writer(sys.stdout)

fields = [int(k) for k in sys.argv[2:]]

seen = set()

def logg(*items):
    sys.stderr.write(" ".join([str(i) for i in items]) + "\n")

rowcount = 0
for row in csvreader:
    rowcount += 1
    if rowcount % 10000 == 0:
        logg("row", rowcount, "of 15124130", (rowcount*100/15124130), "%")
    key = "//".join([row[f] for f in fields])
    if key not in seen:
       csvwriter.writerow(row)  # [v.encode('utf8') if isinstance(v,unicode) else v for v in ch])
    seen.add(key)
