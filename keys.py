import read_ga
from dateutil.parser import parse
from datetime import timedelta
import json
import sys
import datetime
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 


keys = sys.argv[1:]
def follow(chunk, keys):
    if len(keys) == 0:
        return chunk
    elif len(keys) == 1 and keys[0] in chunk:
        return chunk[keys[0]]
    elif len(keys) > 1 and keys[0] in chunk:
        return follow(chunk[keys[0]], keys[1:])
    else:
        raise KeyError
 
for line in sys.stdin:
    ch = json.loads(line)
    try:
        outp = follow(ch, keys)
        print json.dumps(outp)
    except KeyError:
        pass
