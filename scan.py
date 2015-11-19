import read_ga
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

fromdate = sys.argv[1]
todate = sys.argv[2] if len(sys.argv) > 2 else fromdate

fromdate = fromdate + " 00:00:00"
todate = todate + " 23:59:59"

try:
    for ch in read_ga.scanner(fromdate, todate):
        print json.dumps(ch)
except IOError, e:
    print e
    pass
