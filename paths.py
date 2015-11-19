import read_ga
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
import jsonpath_rw

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

paths = sys.argv[1:]
parsers = [jsonpath_rw.parse(p) for p in paths]

def enlist(chunk, parsers):
    res = []
    for p in parsers:
        matches = p.find(chunk)
        if len(matches) > 0:
            res.append(matches[0].value)
        else:
            res.append("missing")
    return res
        
for line in sys.stdin:
    ch = json.loads(line)
    try:
        outp = enlist(ch, parsers)
        print json.dumps(outp)
    except Exception, e:
        print e
        quit()
