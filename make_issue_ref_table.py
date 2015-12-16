import sys, traceback
import csv
import re
from dateutil.parser import parse
from collections import defaultdict
import pytz
import json
import datetime
import sys
import csv
import read_ga
import dbutil
import pdb
import git_comment_conventions
from dateutil.relativedelta import relativedelta

def fixtime(t):
    return t #return t.replace(tzinfo=pytz.utc) if t is not None else None

# Owner names aren't supposed to start wiht numbers, but they can, e.g. github.com/3nids
# Project names aren't supposed to start wiht symbols, but they can, e.g. github.com/tgstation/-tg-station
PROJ_NAME_PAT = re.compile(ur'([A-Z0-9a-z_\.-]+)/([A-Z0-9a-z_\.-]+)')



def make_title_references_table(csvfile, limit=None):
    """ Read through all issue titles, extract issue reference, and write enough data to a csv
        to eventually merge into a database table."""

    cur = dbutil.execute_at_once("""select title, body, full_name, issues.repo_id, issues.id issueDbId, issues.issue_id issueHumanId  
                   from issue_titles left join issues on issues.id=issue_titles.issue_id 
                   join project_stats on issues.repo_id=project_stats.project_id""", limit=limit)
    rowcount = 0
    for row in cur:
        rowcount += 1
        if rowcount % 1000 == 0: print rowcount
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
                    refown = i["parts"][0]
                    refprj = i["parts"][1]
                    refnum = i["parts"][2]
                    refstyle = i["refstyle"]
                    if refown == "%OWNER%": refown = owner
                    if refprj == "%PROJECT%": refprj = projectname
                    refrepoid = row["repo_id"] if refown==owner and refprj == projectname else "NULL"
                    csvfile.writerow([owner + "/" + projectname, row["repo_id"], row["issueHumanId"], 
                             row["issueDbId"], "NULL", "NULL", "NULL", refstyle, refown + "/" + refprj, refrepoid, refnum, "NULL"])
        except Exception, e:
            print e
            traceback.print_exc(file=sys.stderr)
            raise e

def make_pr_comment_references_table(csvfile, limit=None):
    """ Read through all pull request comments, extract issue reference, and write enough data to a csv
        to eventually merge into a database table."""
    cur = dbutil.execute_at_once("""select body, full_name, pull_requests.base_repo_id repo_id, pull_requests.id prDbId, pullreq_id issueHumanId,
                   pull_request_comments.comment_id pr_comment_id  
                   from pull_request_comments 
                   left join pull_requests on pull_request_comments.pull_request_id=pull_requests.id
                   join project_stats on pull_requests.base_repo_id=project_stats.project_id""", limit=limit)
    rowcount = 0
    for row in cur:
        rowcount += 1
        if rowcount % 1000 == 0: print rowcount
        try:
            match = PROJ_NAME_PAT.match(row["full_name"])
            if match is not None:
                owner = match.group(1)
                projectname = match.group(2)
            else:
                raise Exception("Cannot identify owner and project name of " + str(row))
            features = {}
            issuerefs = set()
            #git_comment_conventions.find_special(features, row["title"])    
            git_comment_conventions.find_special(features, row["body"])    
            if "issues" in features:
                for i in features["issues"]:
                    refown = i["parts"][0]
                    refprj = i["parts"][1]
                    refnum = i["parts"][2]
                    refstyle = i["refstyle"]
                    if refown == "%OWNER%": refown = owner
                    if refprj == "%PROJECT%": refprj = projectname
                    refrepoid = row["repo_id"] if refown==owner and refprj == projectname else "NULL"
                    csvfile.writerow([owner + "/" + projectname, row["repo_id"], row["issueHumanId"], 
                             "NULL", "NULL", row["prDbId"],  row["pr_comment_id"], refstyle, refown + "/" + refprj, refrepoid, refnum, "NULL"])
        except Exception, e:
            print e
            traceback.print_exc(file=sys.stderr)
            raise e


def make_issue_comment_references_table(csvfile, limit=None):
    """ Read through all issue comments, extract issue reference, and write enough data to a csv
        to eventually merge into a database table."""
    cur = dbutil.execute_at_once("""select body, full_name, issues.repo_id, issues.id issueDbId, issues.issue_id issueHumanId,
                   issue_comments.comment_id comment_id  
                   from issue_comments inner join comments on issue_comments.comment_id = comments.comment_id
                   left join issues on issues.id=issue_comments.issue_id 
                   join project_stats on issues.repo_id=project_stats.project_id""", limit=limit)
    rowcount = 0
    for row in cur:
        rowcount += 1
        if rowcount % 1000 == 0: print rowcount
        try:
            match = PROJ_NAME_PAT.match(row["full_name"])
            if match is not None:
                owner = match.group(1)
                projectname = match.group(2)
            else:
                raise Exception("Cannot identify owner and project name of " + str(row))
            features = {}
            issuerefs = set()
            #git_comment_conventions.find_special(features, row["title"])    
            git_comment_conventions.find_special(features, row["body"])    
            if "issues" in features:
                for i in features["issues"]:
                    refown = i["parts"][0]
                    refprj = i["parts"][1]
                    refnum = i["parts"][2]
                    refstyle = i["refstyle"]
                    if refown == "%OWNER%": refown = owner
                    if refprj == "%PROJECT%": refprj = projectname
                    refrepoid = row["repo_id"] if refown==owner and refprj == projectname else "NULL"
                    csvfile.writerow([owner + "/" + projectname, row["repo_id"], row["issueHumanId"], 
                             row["issueDbId"],  row["comment_id"], "NULL", "NULL", refstyle, refown + "/" + refprj, refrepoid, refnum, "NULL"])
        except Exception, e:
            print e
            traceback.print_exc(file=sys.stderr)
            raise e

if __name__=="__main__":
    f = open("issue_refs_table.csv", "w")
    csvw = csv.writer(f)
    csvw.writerow(["from_fullname", "repo_id", "issue_num", "issue_id", "comment_id", "pr_id", "pr_comment_id", "refstyle",
                  "to_fullname", "ref_repo_id", "ref_issue_num", "ref_issue_id"])
    make_title_references_table(csvw)
    make_issue_comment_references_table(csvw)
    make_pr_comment_references_table(csvw)
