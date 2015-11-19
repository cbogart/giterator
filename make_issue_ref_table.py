import mysql.connector
import re
from dateutil.parser import parse
from collections import defaultdict
import pytz
import json
import datetime
import sys
import csv
import read_ga
import git_comment_conventions
from dateutil.relativedelta import relativedelta

connection = mysql.connector.connect(
    host= 'localhost',
    user= 'local',
    password= 'local',
    time_zone= '+1:00',
    database= 'github',
);

def fixtime(t):
    return t #return t.replace(tzinfo=pytz.utc) if t is not None else None

PROJ_NAME_PAT = re.compile(ur'([A-Za-z][A-Z0-9a-z_\.-]+)/([A-Za-z][A-Z0-9a-z_\.-]+)')
def make_title_references_table():
    cur = connection.cursor(dictionary=True)
    cur.execute("""select title, body, full_name, issues04.id issueDbId, issues04.issue_id issueHumanId  
                   from issue_titles left join issues04 on issues04.id=issue_titles.issue_id 
                   join project_stats on issues04.repo_id=project_stats.project_id limit 109""")
    for row in cur:
        try:
            match = PROJ_NAME_PAT.match(row["full_name"])
            if match is not None:
                owner = match.group(1)
                projectname = match.group(2)
            else:
                raise Exception("Cannot identify owner and project name of " + str(row))
            features = {}
            issuerefs = set()
            git_comment_conventions.find_special(features, row["title"])    
            git_comment_conventions.find_special(features, row["body"])    
            if "issues" in features:
                for i in features["issues"]:
                    issuerefs.add(git_comment_conventions.parse_issue_reference(i, owner, projectname))
            print owner, projectname, row["issueHumanId"], row["issueDbId"], "->", issuerefs
        except Exception, e:
            print e
            raise e
            

if __name__=="__main__":
    make_title_references_table()
