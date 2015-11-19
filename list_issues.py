import mysql.connector
import sys

connection = mysql.connector.connect(
    host= 'localhost',
    user= 'local',
    password= 'local',
    database= 'github'
);


if len(sys.argv) < 3:
    print "usage: python", sys.argv[0], "<owner> <project>";
    quit();

owner = sys.argv[1];
project = sys.argv[2];

keylist = {}

try:

    cur = connection.cursor(dictionary=True);

    cur.execute("""
       select issue_id, created_at, reporter_id from issues         
        where repo_id = (select id from projects where name=%s and         
              owner_id= (select id from users where login=%s)) order by created_at""",
          (project, owner));
    rows = cur.fetchall()
    
    for row in rows:
       print row["issue_id"], row["created_at"], row["reporter_id"];
    
finally:
    connection.close();
