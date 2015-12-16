import mysql.connector
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
import dotgraph

connection = mysql.connector.connect(
    host= 'localhost',
    user= 'local',
    password= 'local',
    time_zone= '+1:00',
    database= 'github',
);

def fixtime(t):
    return t #return t.replace(tzinfo=pytz.utc) if t is not None else None

def list_issues(owner, project):
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
        cur.close();
    
def get_pull_request_history(owner, project, pr, prid): 
    result = []
    try:
        cur = connection.cursor(dictionary=True);
    
        cur.execute("""select pull_request_history.*, users.login from pull_request_history 
           left join users on actor_id = users.id 
             where pull_request_id = %s;""", (prid,));
        rows = cur.fetchall()
        
        for row in rows:
           result.append({"rectype":"pull_request_history", 
                          "issueid":pr, 
                          "project_owner":owner, 
                          "project_name":project, 
                          "actor":row["login"], 
                          "time":fixtime(row["created_at"]),
                          "title":"",
                          "action":row["action"],
                          "provenance":"ghtorrent",
                          "text":""});

    finally:
        cur.close();

    return result;


def get_pull_request_commit_comments(owner, project, pr, prid): 
    result = []
    try:
        cur = connection.cursor(dictionary=True);
    
        cur.execute("""select pull_request_commits.commit_id cid, commit_comments.*, users.login from pull_request_commits 
           left join commit_comments on pull_request_commits.commit_id=commit_comments.commit_id
           left join users on commit_comments.user_id = users.id 
             where pull_request_id = %s;""", (prid,));
        rows = cur.fetchall()
        
        for row in rows:
           result.append({"rectype":"pull_request_commit_comment", 
                          "issueid":pr, 
                          "project_owner":owner, 
                          "project_name":project, 
                          "actor":row["login"], 
                          "time":fixtime(row["created_at"]),
                          "title":"",
                          "action":"comment",
                          "provenance":"ghtorrent",
                          "text":row["body"] if row["body"] != None else "missing commit comment: comment_id=" + str(row["cid"]) });


    finally:
        cur.close();

    return result;

def get_pull_request_comments(owner, project, pr, prid): 
    result = []
    try:
        cur = connection.cursor(dictionary=True);
    
        cur.execute("""select pull_request_comments.*, users.login from pull_request_comments 
           left join users on user_id = users.id 
             where pull_request_id = %s;""", (prid,));
        rows = cur.fetchall()
        
        for row in rows:
           result.append({"rectype":"pull_request_comment", 
                          "issueid":pr, 
                          "project_owner":owner, 
                          "project_name":project, 
                          "actor":row["login"], 
                          "time":fixtime(row["created_at"]),
                          "title":"",
                          "action":"comment",
                          "provenance":"ghtorrent",
                          "text":row["body"]});

    finally:
        cur.close();

    return result;
        
def get_issue_events(owner, project, issue, issue_id): 
    result = []
    try:
        cur = connection.cursor(dictionary=True);
    
        cur.execute("""select issue_events.*, users.login from issue_events 
           left join users on actor_id = users.id 
             where issue_id = %s;""", (issue_id,));
        rows = cur.fetchall()
        
        for row in rows:
           result.append({"rectype":"issue_event", 
                          "issueid":issue, 
                          "project_owner":owner, 
                          "project_name":project, 
                          "actor":row["login"], 
                          "time":fixtime(row["created_at"]),
                          "title":row["action_specific"],
                          "action":row["action"],
                          "provenance":"ghtorrent",
                          "text": ""});

    except mysql.connector.errors.DataError, de:
        print "Caught error ", de
        print cur.statement
    finally:
        cur.close();
        
    return result;

def get_pr_related_hour_ga(owner, project, issue, prid):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select created_at from pull_request_comments where pull_request_id = %s 
               union select created_at from pull_request_commits 
                      left join commits on pull_request_commits.commit_id=commits.id where pull_request_id=%s
               union select created_at from pull_request_history where pull_request_id=%s""", (prid,prid,prid))
        rows = cur.fetchall()
        print "pr union returned ", len(rows), " rows"
        
        for row in rows:
            for info in read_ga.getIssuePlus(owner, project, issue, row["created_at"], row["created_at"]):
                 result.append(info)
    finally:
        cur.close()

    return result

def get_inbound_issue_title_references(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select title, body, issues.created_at, login, from_full_name, from_issue_num 
                from issue_crossref left join issue_titles
                    on from_issue_id=issue_titles.issue_id 
                left join issues on issues.id=from_issue_id
                left join users on users.id=issues.reporter_id
                where to_issue_id=%s and from_comment_id = 0 and from_pr_comment_id = 0""", (issue_id,))
        rows = cur.fetchall()
        for row in rows:
            result.append({"rectype":"issue_crossref", 
                           "issueid":issue, 
                           "project_owner":owner, 
                           "project_name":project, 
                           "actor":row["login"], 
                           "time":fixtime(row["created_at"]),
                           "title":"Referenced in title or body of issue " + \
                                   row["from_full_name"] + "#" + str(row["from_issue_num"]),
                           "action":"write",
                           "provenance":"issue_crossref",
                           "text": row["title"] + "\n\n" + row["body"]});
    finally:
        cur.close()

    return result

def get_inbound_issue_comment_references(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select body, issue_comments.created_at, login, from_full_name, from_issue_num 
                from issue_crossref left join issue_comments 
                    on from_issue_id=issue_comments.issue_id 
                    and from_comment_id=issue_comments.comment_id
                left join comments on issue_crossref.from_comment_id=comments.comment_id  
                left join users on users.id=issue_comments.user_id
                where to_issue_id=%s and from_comment_id != 0""", (issue_id,))
        rows = cur.fetchall()
        for row in rows:
            result.append({"rectype":"issue_crossref", 
                           "issueid":issue, 
                           "project_owner":owner, 
                           "project_name":project, 
                           "actor":row["login"], 
                           "time":fixtime(row["created_at"]),
                           "title":"Referenced in comment in issue " + \
                                   row["from_full_name"] + "#" + str(row["from_issue_num"]),
                           "action":"write",
                           "provenance":"issue_crossref",
                           "text": row["body"]});
    finally:
        cur.close()

    return result

def get_inbound_pull_request_references(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select body, pull_request_comments.created_at, login, from_full_name, from_issue_num 
                from issue_crossref left join pull_request_comments
                    on from_pr_id=pull_request_comments.pull_request_id 
                    and from_pr_comment_id=pull_request_comments.comment_id
                left join users on users.id=pull_request_comments.user_id
                where to_issue_id=%s and from_issue_id=0""", (issue_id,))
        rows = cur.fetchall()
        for row in rows:
            result.append({"rectype":"pull_request_crossref", 
                           "issueid":issue, 
                           "project_owner":owner, 
                           "project_name":project, 
                           "actor":row["login"], 
                           "time":fixtime(row["created_at"]),
                           "title":"Referenced in code review comment in pull request " + \
                                   row["from_full_name"] + "#" + str(row["from_issue_num"]),
                           "action":"write",
                           "provenance":"issue_crossref",
                           "text": row["body"]});
    finally:
        cur.close()

    return result


def get_issue_related_hour_ga(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select created_at from issues where id = %s 
               union select created_at from issue_comments where issue_id=%s
               union select created_at from issue_events where issue_id=%s""", (issue_id,issue_id,issue_id))
        rows = cur.fetchall()
        print "issue union returned ", len(rows), " rows"
        
        for row in rows:
            for info in read_ga.getIssuePlus(owner, project, issue, row["created_at"], row["created_at"]):
                 result.append(info)
    finally:
        cur.close()

    return result

def get_issue_related_day_ga(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select * from issues where id = %s""", (issue_id,))
        rows = cur.fetchall()
        
        spans = set([])
        for row in rows:
            spans.add(row["created_at"].replace(hour=0,minute=0))

        for span in spans:
            print "Checking", span, "through", span+relativedelta(days=1)
            for info in read_ga.getIssuePlus(owner, project, issue, span, span + relativedelta(days=1)):
                 print "Found in timespan: ", info["rectype"], info["time"]
                 result.append(info)

    finally:
        cur.close()

    return result

def get_issue_title(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select * from issues left join issue_titles on issue_titles.issue_id=issues.id left join users on users.id=issues.reporter_id where issues.id = %s""", (issue_id,))
        rows = cur.fetchall()
        
        for row in rows:
           result.append({"rectype":"issue_title", 
                          "issueid":issue, 
                          "project_owner":owner, 
                          "project_name":project, 
                          "actor":row["login"], 
                          "time":fixtime(row["created_at"]),
                          "title":row["title"],
                          "action":"write",
                          "provenance":"githubarchive->issue_titles",
                          "text": row["body"]});
    finally:
        cur.close()

    return result; 

def get_issue_title_ga(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select * from issues where id = %s""", (issue_id,))
        rows = cur.fetchall()
        
        for row in rows:
            print "Called with issue", issue, "issue_id", issue_id, "found id", row["id"], "issue_id", row["issue_id"]
            for issue in read_ga.getIssue(issue_id, row["created_at"]):
                 print "FOUND!!!!", issue["provenance"]
                 result.append(issue)
    finally:
        cur.close()

    return result

"""
def get_issue_event_ga(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute(""select * from issues where issue_id = %s"", (issue_id,))
        rows = cur.fetchall()
        
        for row in rows:
            for issue in read_ga.getIssueEvent(row["id"], row["created_at"]):
                 result.append(issue)
    finally:
        cur.close()

    return result
"""

def get_event_types_ga():
    event_type = defaultdict(int)
    for chunk in read_ga.scanner("2014-01-01T00:00:00", "2014-01-02T00:00:00"):
        event_type[chunk["type"] +  ("/" + chunk["payload"]["action"] if "payload" in chunk and "action" in chunk["payload"] else "")] += 1
    for t in event_type:
        print t, event_type[t]

def get_watchlist_info_ga():
    for chunk in read_ga.scanner("2015-01-01T00:00:00", "2015-01-02T00:00:00"):
        if (chunk["type"] == 'WatchEvent' and "repo" in chunk):
            actor = chunk["actor"]["login"]
            repo = chunk["repo"]["name"]
            action = chunk["payload"]["action"]
            when = chunk["created_at"]
            print actor, repo, action, when
        elif (chunk["type"] == 'WatchEvent' and "repository" in chunk):
            actor = chunk["actor_attributes"]["login"]
            repo = chunk["repository"]["owner"] + "/" + chunk["repository"]["name"]
            action = chunk["payload"]["action"]
            when = chunk["created_at"]
            print actor, repo, action, when

def get_issue_comments_ga(owner, project, issue, issue_id):
    result = []
    try:
        cur = connection.cursor(dictionary=True);
        cur.execute("""select issue_comments.*, comments.body, users.login from issue_comments left join comments 
             on issue_comments.comment_id = comments.comment_id 
             left join users on user_id = users.id 
             where issue_id = %s""", (issue_id,))
        rows = cur.fetchall()
        
        for row in rows:
            print cur.statement
            print row
            for comment in read_ga.getIssueComment(int(row["comment_id"]), row["created_at"]):
                 print "FOUND2!!!!", comment["provenance"]
                 result.append(comment)
    finally:
        cur.close()

    return result

def get_pr_ids(owner, project, issue): 
    result = []
    try:
        cur = connection.cursor(dictionary=True);
    
        cur.execute("""select id from pull_requests where 
              pullreq_id = %s and   
              base_repo_id = (select id from projects where name=%s and         
              owner_id= (select id from users where login=%s))""",
              (issue, project, owner));
        rows = cur.fetchall()
        
        for row in rows:
           result.append(row["id"])

    finally:
        cur.close();
        
    return result;

def get_issue_ids(owner, project, issue): 
    result = []
    try:
        cur = connection.cursor(dictionary=True);
    
        cur.execute("""select id from issues where 
              issue_id= %s and   
              repo_id = (select id from projects where name=%s and         
              owner_id= (select id from users where login=%s))""",
              (issue, project, owner));
        rows = cur.fetchall()
        
        for row in rows:
           result.append(row["id"])

    finally:
        cur.close();
        
    return result;


def get_issue_comments(owner, project, issue, issue_id): 
    result = []
    try:
        cur = connection.cursor(dictionary=True);
    
        cur.execute("""select issue_comments.*, users.login, comments.* from issue_comments left join comments 
        on issue_comments.comment_id = comments.comment_id 
       left join users on user_id = users.id 
         where issue_id = %s""", (issue_id,))
        rows = cur.fetchall()
        
        for row in rows:
           result.append({"rectype":"issue_comment", 
                          "issueid":issue, 
                          "project_owner":owner, 
                          "project_name":project, 
                          "actor":row["login"], 
                          "time":row["created_at"], 
                          "title":"",
                          "action":"issue",
                          "provenance":"ghtorrent",
                          "text":row["body"]})

    finally:
        cur.close();
        
    return result;


"""
module.exports.list_pull_requests = function(owner, project) { 
    module.connection.connect();
    results = [];
    module.connection.query("  \
       select pull_requests.*, users.login, commits.created_at, commits.id cid \
        from pull_requests left join commits on commits.id=pull_requests.base_commit_id \
        left join users on users.id=commits.author_id \
        where base_repo_id = (select id from projects where name=? and         \
              owner_id= (select id from users where login=?)) order by created_at",
      [project, owner], function(err, rows, fields) {

      if (err) throw err;

      for (var r in rows) {
         results.push(["list_pull_requests", rows[r].pullreq_id, owner, project, rows[r].login, rows[r].created_at, ""]);
      }

    });
    module.connection.end();
    return results;     
}

module.exports.list_issues = function(owner, project) { 
    module.connection.connect();
    results = [];
    module.connection.query("  \
       select issue_id, created_at, reporter_id, login from issues         \
        left join users on users.id=reporter_id \
        where repo_id = (select id from projects where name=? and         \
              owner_id= (select id from users where login=?)) order by created_at",
      [project, owner], function(err, rows, fields) {

      if (err) throw err;

      for (var r in rows) {
         results.push(["list_issues", rows[r].issue_id, owner, project, rows[r].login, rows[r].created_at, rows[r].reporter_id]);
      }

    });
    module.connection.end();
    return results;     
}

"""

def pp_dot(owner, project, issue): return  owner + "/" + project + "#" + issue 

def query_all(owner, project,issue):
    print "----------Extracting discussion on ", pp_dot(owner,project,issue)
    csvw = csv.writer(open("samples/repo_" + owner + "_" + project + "_issue" + issue + ".csv", "wb"));
    fields = ["rectype", "issueid", "project_owner",
                     "project_name", "actor",
                     "time", "text", "action", "title", "provenance"]
    feature_tags = ["plus_1", "urls", "issues", "userref", "code"]
    
    csvw.writerow(tuple(fields+feature_tags))
    
    results = [];

    for iid in get_pr_ids(owner, project, issue):
        print "PR id ", iid
        results.extend(get_pull_request_comments(owner, project, issue, iid));
        results.extend(get_pull_request_history(owner, project, issue, iid));
        results.extend(get_pull_request_commit_comments(owner, project, issue, iid));
        ##results.extend(get_pr_related_hour_ga(owner, project, issue, iid));

    for iid in get_issue_ids(owner, project, issue):
        print "Issue id ", iid
        results.extend(get_issue_title(owner, project, issue, iid));
        results.extend(get_issue_events(owner, project, issue, iid));
        results.extend(get_issue_comments(owner, project, issue, iid));
        results.extend(get_inbound_issue_comment_references(owner, project, issue, iid))
        results.extend(get_inbound_issue_title_references(owner, project, issue, iid))
        results.extend(get_inbound_pull_request_references(owner, project, issue, iid))
        ##results.extend(get_issue_comments_ga(owner, project, issue, iid));
        ##results.extend(get_issue_title_ga(owner, project, issue, iid));
        ##results.extend(get_issue_related_hour_ga(owner, project, issue, iid));


    epoch = datetime.datetime.fromtimestamp(0).replace(tzinfo=pytz.utc)
    results.sort(key=lambda r: r["time"].replace(tzinfo=pytz.utc) if r["time"] is not None else epoch )
    otherIssues = set()
    for result in results:
        result["time"] = result["time"].replace(tzinfo=pytz.utc) if result['time'] is not None else None
        features = {}
        git_comment_conventions.find_special(features, result["title"])    
        git_comment_conventions.find_special(features, result["text"])    
        if "issues" in features:
            for i in features["issues"]:
                i["parts"] = list(i["parts"])
                i["parts"].append("rev" if result["provenance"] == "issue_crossref" else "")
                if i["parts"][0] == "%OWNER%": i["parts"][0] = owner
                if i["parts"][1] == "%PROJECT%": i["parts"][1] = project
                print "MKLINK: in", owner, project,issue,"with provenance",result["provenance"],"adding", i["parts"]
                otherIssues.add(tuple(i["parts"]))
        csvw.writerow( tuple([unicode(result[f]).encode("utf-8") for f in fields] + [json.dumps(features.get(k,"")) for k in feature_tags]))
    print "--->Issues referenced: ", otherIssues
    return otherIssues

if __name__=="__main__":
    if len(sys.argv) < 4:
        print "usage: python", sys.argv[0], "<owner> <project> <issue>";
        quit();
    
    owner = sys.argv[1];
    project = sys.argv[2];
    issue = sys.argv[3];
    todoIssues = set([(owner, project, issue, "")])
    doneIssues = set()
    doti = dotgraph.Graphic()
    dotp = dotgraph.Graphic()

    # Examine each issue, adding any unexamined referenced issues to a set of
    # todo items, and eventually get around to the closure of all of these.
    while len(todoIssues) > 0:
       print "]]]]]Stack now contains " + str(len(todoIssues)) + " issues to include"
       (thisowner, thisproject, thisissue, rev) = todoIssues.pop()
       if (thisowner == owner and thisproject == project):
           relatedIssues = query_all(thisowner, thisproject,thisissue)
           for r in relatedIssues:
               #import pdb; pdb.set_trace()
               doti.link(pp_dot(thisowner, thisproject, thisissue), pp_dot(r[0], r[1], r[2]), reverse=(r[3]=="rev"))
               dotp.link(thisowner + "/" + thisproject, r[0] + "/" + r[1], reverse=(r[3]=="rev"))
           todoIssues = todoIssues | relatedIssues
       doneIssues = doneIssues | set([(thisowner, thisproject, thisissue, rev)])
       todoIssues = todoIssues - doneIssues

    doti.draw("samples/repo_" + owner + "_" + project + ".i.dot")
    dotp.draw("samples/repo_" + owner + "_" + project + ".p.dot")
