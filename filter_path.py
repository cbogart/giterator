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

for line in sys.stdin:
    try:
        ch = json.loads(line)
        haz = parser.find(ch)
        if len(haz) > 0:
            print json.dumps(ch)
    except Exception, e:
        sys.stderr.write( "========>Err"+ str(line) + str(e))
