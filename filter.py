import read_ga
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
import jsonpath_rw
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

(key, match) = sys.argv[1], sys.argv[2]

keyparser = jsonpath_rw.parse(key)
 
for linep in sys.stdin:
    line = linep.strip()
    if len(line) > 0:
        try:
          ch = json.loads(line.strip())
          matches = [m.value for m in keyparser.find(ch)]
          if match in matches:
              print json.dumps(ch)
        except Exception, e:
          pass



