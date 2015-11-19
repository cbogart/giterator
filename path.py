import read_ga
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
import jsonpath_rw

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

path = sys.argv[1]
parser = jsonpath_rw.parse(path)

def enlist(chunk, parser):
    matches = parser.find(chunk)
    if len(matches) > 0:
        return  matches[0].value
    else:
        return {}
        
for line in sys.stdin:
    ch = json.loads(line)
    try:
        outp = enlist(ch, parser)
        print json.dumps(outp)
    except Exception, e:
        print e
        quit()
