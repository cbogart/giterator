import read_ga
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
import jsonpath_rw
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

for line in sys.stdin:
    if len(line) > 0:
        try:
          ch = json.loads(line.strip())
          print json.dumps(ch, indent=4)
        except Exception, e:
          pass



