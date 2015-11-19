import gzip
import sys
from dateutil.parser import parse
from collections import defaultdict
from datetime import timedelta
import json
import datetime
import pytz
import jsonpath_rw 

githubarchive = "/usr2/scratch/githubarchive";
dateFileFormat = "%Y/%Y-%m-%d-h%H"   # remove the "h" when eliminating leading zero
# "YYYY/YYYY-MM-DD-H"; 

def date2filename(t):
    return (githubarchive + "/" +
           t.strftime(dateFileFormat).replace("h0","h").replace("h","") +
           ".json.gz")

def daterange2files(start, end): 
    starthour = parse(start);
    starthour.replace(tzinfo=pytz.UTC)
    endhour = parse(end);
    endhour.replace(tzinfo=pytz.UTC)
    hours = int((endhour - starthour).total_seconds()/3600)+1
    files = [];
    for h in range(0, hours):
        t = starthour + timedelta(hours=h)
        files.append(date2filename(t));
    return files;


def daterange2files_test():
    files = daterange2files("2015-01-22 08:23:18", "2015-01-22 12:23:18")
    assert len(files) == 5, "test daterange2files: wrong number of hours retrieved: " + str(len(files))
    assert files[0] == "/usr2/scratch/githubarchive/2015/2015-01-22-8.json.gz", "test daterange2files: "+ files[0]
    assert files[4] == "/usr2/scratch/githubarchive/2015/2015-01-22-12.json.gz", "test daterange2files: "+ files[4]

if __name__=="__main__":
    daterange2files_test()


def demo ():
    #for issue in getIssue_GA(392341, "2015-01-22T14:23:18Z"):
    #    print issue

    for e in getIssuePlus("google", "blockly", "40", "2015-01-04T04:08:07Z", "2015-01-04T04:08:07Z"):
        print e

    print "------"
    """
    for issue in getIssue(55155985, "2015-01-22 14:00:04"):
        print issue
        print "==="
        print issue["title"]
        print "---"
        print issue["text"]
    for issueComment in getIssueComment(71025672, "2015-01-22 14:14:54"):
        print issueComment
        print "==="
        print issue["title"]
        print "---"
        print issue["text"]
    for issueComment in getIssueComment(72839454, "2009-04-22 07:44:56"):
        print issueComment
        print "==="
        print issue["title"]
        print "---"
        print issue["text"]
    """

def scanner(fromdate, todate):
    for file in daterange2files(fromdate, todate):
        sys.stderr.write("Reading " + file + "\n")
        try:
            for line in gzip.open(file, "rb"):
                yield json.loads(line) 
        except IOError, ioe:
            print ioe
        except ValueError, ve:
            print "Skipping line in", file, "because", ve
        except UnicodeDecodeError, ude:
            print "Skipping line in", file, "because", ude

def getFromGA(item_type, create_date, id, payloadtype):
    for chunk in scanner(create_date, create_date):
        if (chunk["type"] == item_type and 
            payloadtype in chunk["payload"] and 
            type(chunk["payload"][payloadtype]) == "dict" and
            chunk["payload"][payloadtype]["id"] == id):
                yield chunk

findJsonRepo = jsonpath_rw.parse("$..repo")
findJsonRepository = jsonpath_rw.parse("$..repository")

def print_fields(dct, flds):
    out = ""
    for f in flds:
        if f in dct:
            try:
                out = out + " " + f + ": " + unicode(dct[f]).encode('utf-8')
            except TypeError, te:
                print f, dct[f], te
                quit()
        else:
            out = out + " " + f + ": XXX"
    print out

def get_project_curves(owner, name, fromdate, todate):
    #values = defaultdict("")
    keysets = set()
    for chunk in scanner(fromdate, todate):
        for repox in findJsonRepo.find(chunk):
            repo = repox.context.value
            keysets.add("repo keys: " +  "//".join(sorted(repo.keys())))
            #print_fields(repo, ["owner", "name", "full_name", "stargazers", "stargazers_count", "watchers", "watchers_count", "forks", "has_wiki", "has_pages"])
            if "name" not in repo:
                print repo
        for repositoryx in findJsonRepository.find(chunk):
            repository = repositoryx.context.value
            keysets.add("repository keys: "+  "//".join(sorted(repository.keys())))
            #print_fields(repository, ["owner", "name", "full_name", "stargazers", "stargazers_count", "watchers", "watchers_count", "forks", "has_wiki", "has_pages"])
    print "\n".join(keysets)


def classify(chunk):
    summary = {
        "time": parse(chunk["created_at"]),
        "rectype": chunk["type"],
        "issueid": "unknown",
        "action": chunk["payload"]["action"],
        "title": "",
        "provenance": "githubarchive",
        "text": "//".join(chunk["payload"].keys())
    }
    if "repository" in chunk:
        summary["project_owner"] = chunk["repository"]["owner"]
        summary["project_name"]= chunk["repository"]["name"]
        summary["actor"]= chunk["actor_attributes"]["login"]
    elif "repo" in chunk:
        summary["project_owner"] = chunk["repo"]["name"].split("/")[0]
        summary["project_name"] = chunk["repo"]["name"].split("/")[1]
        summary["actor"] = chunk["actor"]["login"]

    if (chunk["type"] == "IssuesEvent"):
       if ("repository" in chunk):
            summary["issueid"] = chunk["payload"]["number"]
            summary["title"] = "missing issue title (pre 2015)"
            summary["text"] = "missing issue text (pre 2015)"
       else:
            summary["issueid"] = chunk["payload"]["issue"]["number"]
            summary["title"] = chunk["payload"]["issue"]["title"]
            summary["text"] = chunk["payload"]["issue"]["body"]
    elif (chunk["type"] == "IssueCommentEvent" ):
       if ("repo" in chunk):
            summary["issueid"] = chunk["payload"]["issue"]["id"]
            summary["title"] = chunk["payload"]["issue"]["title"]
            summary["text"] = chunk["payload"]["comment"]["body"]
       else:
            summary["issueid"] = "missing issue number (pre 2015)"
            summary["title"] = "missing issue comment title (pre 2015)"
            summary["text"] = "missing issue comment text (pre 2015)"
    elif (chunk["type"] == "PullRequestEvent"):
       summary["issueid"] = chunk["payload"]["pull_request"]["number"]
       summary["title"] = chunk["payload"]["pull_request"]["title"]
       summary["text"] = chunk["payload"]["pull_request"]["body"]
    return summary

findJsonNumber = jsonpath_rw.parse("$..number")
findJsonRepo = jsonpath_rw.parse("$..name")

def getIssuePlus(owner, project, issue, from_date, to_date):
    for chunk in scanner(str(from_date), str(to_date)):
        if "repository" in chunk:
            if (chunk["repository"]["name"] == project and
                chunk["repository"]["owner"] == owner and
                    int(issue) in [int(found.value) for found in findJsonNumber.find(chunk)]):
                print "Found relevant", chunk["type"], "at", chunk["created_at"]
                yield classify(chunk)
            elif (chunk["repository"]["name"] == project and
                chunk["repository"]["owner"] == owner):
                print chunk["type"], "not relevant to ", issue, ": ", [found.value for found in findJsonNumber.find(chunk)]
        elif "repo" in chunk:
            if (chunk["repo"]["name"] == owner + "/" + project and
                    int(issue) in [int(found.value) for found in findJsonNumber.find(chunk)]):
                print "Found relevant", chunk["type"], "at", chunk["created_at"]
                yield classify(chunk)
            elif (chunk["repo"]["name"] == owner + "/" + project):
                print chunk["type"], "not relevant to ", issue, ": ", [found.value for found in findJsonNumber.find(chunk)]

# rectype, issueid, owner, name, actor,time,text
def getIssue(issue_id, create_date):
   if (type(create_date) is datetime.datetime):
       create_date = str(create_date)
   print "Called read_ga.getIssue(", issue_id, create_date, type(create_date), ")"
   for event in getFromGA("IssuesEvent", create_date, issue_id, "issue"):
       yield {"rectype": "IssuesEvent",
              "issueid": issue_id,
              "project_owner": event["repo"]["name"].split("/")[0],
              "project_name": event["repo"]["name"].split("/")[1],
              "actor": event["actor"]["login"],
              "time": event["created_at"],
              "action": event["payload"]["action"],
              "title": event["payload"]["issue"]["title"],
              "provenance": "githubarchive",
              "text": event["payload"]["issue"]["body"]}

def getIssueComment(comment_id, create_date):
   if (type(create_date) is datetime.datetime):
       create_date = str(create_date)
   print "Called getIssueComment(", comment_id, ",", create_date, ")"
   for event in getFromGA("IssueCommentEvent", create_date, comment_id, "comment"):
       yield {"rectype": "IssueCommentEvent",
              "issueid": event["payload"]["issue"]["id"],
              "project_owner": event["repo"]["name"].split("/")[0],
              "project_name": event["repo"]["name"].split("/")[1],
              "actor": event["actor"]["login"],
              "time": event["created_at"],
              "action": event["payload"]["action"],
              "title": event["payload"]["issue"]["title"],
              "provenance": "githubarchive",
              "text": event["payload"]["comment"]["body"]}

if __name__=="__main__":
    #demo()
    get_project_curves("demianturner", "sgl-test", "2013-01-01T00:00:00", "2013-01-01T08:00:00")
