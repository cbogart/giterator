import read_ga
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
import jsonpath_rw
import csv

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

csvwriter = csv.writer(sys.stdout)

for line in sys.stdin:
    ch = json.loads(line)
    try:
       csvwriter.writerow([v.encode('utf8') if isinstance(v,unicode) else v for v in ch])
    except UnicodeEncodeError, uee:
       sys.stderr.write( str(len(line)) + "//" + line + "//" + str(uee))
       raise uee
