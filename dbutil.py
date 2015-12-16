import  mysql.connector

connection = mysql.connector.connect(
    host= 'localhost',
    user= 'local',
    password= 'local',
    time_zone= '+1:00',
    database= 'github',
);

def get_cursor():  
    return connection.cursor(dictionary=True)
    
def execute_at_once(sql, limit = None):
    cur = get_cursor()
    if limit is not None:
        limit_clause = " limit " + str(limit)
    else:
        limit_clause = ""
    print sql + limit_clause
    cur.execute(sql + limit_clause)
    for row in cur:
        yield row

def execute_in_chunks(sql, limit=None, chunksize=5000):
    sofar = 0
    cur = get_cursor()
    while (True):
        limit_clause = " limit " + str(chunksize) + " OFFSET " + str(sofar)
        print sql + limit_clause
        cur.execute(sql + limit_clause)
        count_this_chunk = 0
	for row in cur:
            count_this_chunk += 1
            yield row
        if count_this_chunk < chunksize:
            return
        else:
            sofar += chunksize
        if sofar >= limit_clause:
            return
        
    
