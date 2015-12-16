import re, urllib
import json
from bs4 import BeautifulSoup
import mistune  # markdown parser

#
#   These are functions that detect conventions in github issue, comment, wiki, etc. text
#
#   Each section below contains a find function to identify the convention, and a test function
#    with examples to validate the finder.  The tests are only run if you call this
#    file by itself (python git_comment_conventions.py).
#
#   find_special() at the bottom of the file calls *all* of the finder functions in sequence
#
#


#
#   Just a utility function for the tests
#

def feature_tester(funct, comment, text, newfeatures, newtext):
    feat = {}
    mynewtext = funct(feat, text)
    assert mynewtext == newtext, comment + ": newtext was (" + repr(mynewtext) + ") should be (" + repr(newtext) + ")"
    assert set(feat.keys()) == set(newfeatures.keys()), \
           comment + ": key differences: (" + str(feat.keys()) + "!=" + str(newfeatures.keys()) + ")"
    for k in feat:
        assert feat[k] == newfeatures[k], \
               comment + ": " + k + " values differ (" + str(newfeatures[k]) + "!=" + str(feat[k]) + ")"

#
#  find_urls: Extract URLs from comments
#

# URL recognizer: https://mathiasbynens.be/demo/url-regex
HAY_URLINTEXT_PAT = re.compile(ur'((?:https?|ftp)://[^\s/$.?#].[^\s]*)', flags=re.I)
def find_urls(features, text):
    urls = [ mgroups for mgroups in HAY_URLINTEXT_PAT.findall(text) ]
    if "urls" not in features: features["urls"] = []
    features["urls"].extend(urls)
    features["urls"].sort()
    return text

def test_find_urls():
    feature_tester(find_urls, "Find urls", "http://goo.gl/xyz#pdq is different\n\r\n\rfrom http://xyz.pdq.com/a=4 ",
                  {"urls": ["http://goo.gl/xyz#pdq", "http://xyz.pdq.com/a=4"]},
                  "http://goo.gl/xyz#pdq is different\n\r\n\rfrom http://xyz.pdq.com/a=4 ")


#
#   find_plus_1: Many comments start with +1 as a way of informally upvoting something.  
#

PLUSONE_PAT = re.compile(ur'^\s*(\+1)\s*(.+)$')
def find_plus_1(features, text):
    plusone = PLUSONE_PAT.findall(text)
    if plusone:
        features["plus_1"] = True
        return plusone[0][1]
    else:
        features["plus_1"] = False
        return text

def test_find_plus_1():
    feature_tester(find_plus_1, "Plus one", "+1 I like this", {"plus_1": True}, "I like this")
    feature_tester(find_plus_1, "Plus one (with line breaks)", "\n\r\n\r +1\n\r\n\r  I like this", {"plus_1": True}, "I like this")


#
#  find_imported_comment: Sometimes people import issue threads from other forum systems;
#  when these follow a pattern, we can try to recognize those
#

def find_imported_comment(features, text):
    """Identifies comments imported from some other forum (not implemented yet)"""

    return text


INC_CODE_PAT = re.compile("(```(\S*)\n(.*?\n)```)")
def find_included_code(features, text):
    """Strips code out of text, and identifies the language"""
    if "code" not in features: features["code"] = []
    text2 = text
    if ("```" in text):
        codeparts = INC_CODE_PAT.findall(text)
        if codeparts is not None:
            for codepart in codeparts:
                features["code"].append(codepart[1] if codepart[1] != "" else "unspecified")
                text2 = text2.replace(codepart[0], "")
        features["code"] = sorted(list(set(features["code"])))

    return text2

markdowner = mistune.Markdown()
def find_included_code_with_mistune(features, text):
    """Strips code out of text, and identifies the language"""
    if "code" not in features: features["code"] = []
    if ("```" in text):
        md = markdowner(text)
        parsed = BeautifulSoup(md, "html.parser")
        for codeblock in parsed.findAll("code"):
            if codeblock.has_attr("class"):
                features["code"].append(codeblock["class"][0])
            else:
                features["code"].append("unspecified")
            codeblock.string = ""
        features["code"] = sorted(list(set(features["code"])))
        return parsed.text
    else:
        return text   

def test_find_included_code():
    feature_tester(find_included_code, "Recognize and strip code language", "noncode\n```python\ncodestuff\n```\nmore noncode\n",
                   {"code": ["python"]}, "noncode\n\nmore noncode\n")
    feature_tester(find_included_code, "Recognize and strip unlabeled code language", "noncode\n```\ncodestuff\n```\nmore noncode\n",
                   {"code": ["unspecified"]}, "noncode\n\nmore noncode\n")
    feature_tester(find_included_code, "Recognize and strip 2 codes", "noncode\n```ruby\ncodestuff\n```\nmore noncode\n```\nmore\n```\nstuff\n",
                   {"code": ["ruby","unspecified"]}, "noncode\n\nmore noncode\n\nstuff\n")

#
# find_issue_references:
# Github-flavored markdown includes references to other issues, which may or may not be part
# of this same project
# 
#  In text associated with project P owner by user U,
#     #42  =  Issue 42 of project U/P
#     Q#42  = Issue 42 of project Q/U    (i.e. it's interpreted as a username)
#     T/Q#42  =  Issue 42 of project T/Q  
#     http://github.com/T/Q/issues/42  = Issue 42 of project T/Q
#     http://github.com/T/Q/pull/42  = Issue 42 of project T/Q
#
# In this recognizer we don't know the project context, so we just return blank for user 
#  or project, to be filled in elsewhere
#

#FULL_ISSUE_REF_PAT = re.compile(ur'(\S+/\S+#\d+)')
#PROJ_ISSUE_REF_PAT = re.compile(ur'(\S+#\d+)')
FULL_ISSUE_REF_PAT = re.compile(ur'(\b[A-Za-z][A-Z0-9a-z_\.-]+/[A-Za-z][A-Z0-9a-z_\.-]+#\d+)')
PROJ_ISSUE_REF_PAT = re.compile(ur'(\b[A-Za-z][A-Z0-9a-z_\.-]+#\d+)')
PLAIN_ISSUE_REF_PAT = re.compile(ur'(#\d+)')
GH_PLAIN_ISSUE_REF_PAT = re.compile(ur'\b([Gg][Hh]-\d+)')
URL_ISSUE_REF_PAT = re.compile(ur'(https?://github.com/[A-Za-z0-9_\.-]+/[A-Za-z0-9_\.-]+/(?:pull|issues)/\d+)')
def find_issue_references(features, text):
    """Gather refererences to other issues"""
    refs = dict()
    if "issues" not in features: features["issues"] = []
    def checkmatch(pattern, refstyle, text2):
        for match in pattern.findall(text2):
            g1 = match #match.group(1)
            text2 = text2.replace(g1, " ")
            refs[g1]={"raw": g1, "refstyle":refstyle, "parts": parse_issue_reference(g1, "%OWNER%", "%PROJECT%")}
        return text2

    # Strip out the reference with each check; but don't return the mangled result; this is just
    # to keep from double-counting
    text2 = checkmatch(URL_ISSUE_REF_PAT, "url", text)

    # After getting our one kind of URL out, strip out all other URLs from consideration
    remove_urls = dict()
    find_urls(remove_urls, text2)
    for url in remove_urls["urls"]:
        text2 = text2.replace(url,"")

    # Now search for issue references
    text2 = checkmatch(FULL_ISSUE_REF_PAT, "o/p#d",text2)
    text2 = checkmatch(PROJ_ISSUE_REF_PAT, "o#d", text2)
    text2 = checkmatch(PLAIN_ISSUE_REF_PAT, "#d", text2)
    text2 = checkmatch(GH_PLAIN_ISSUE_REF_PAT, "gh-d", text2)

    features["issues"].extend(refs.values())
    features["issues"].sort(key=lambda d: d["raw"])
    return text

PARSE_REF_PAT = re.compile(ur'([A-Z0-9a-z_\.-]+/)?([A-Z0-9a-z_\.-]+)?#(\d+)')
GH_PARSE_REF_PAT = re.compile(ur'\b[Gg][Hh]-(\d+)')
URL_REF_PAT = re.compile(ur'https?://github.com/([A-Za-z0-9_\.-]+)/([A-Za-z0-9_\.-]+)/\S+/(\d+)')
def parse_issue_reference(reftext, defaultowner, defaultproject):
    match = PARSE_REF_PAT.match(reftext)
    if match is not None:
        if (match.group(1) is None and match.group(2) is None):
            return (defaultowner, defaultproject, match.group(3))
        elif (match.group(1) is None and match.group(2) is not None):
            return (match.group(2), defaultproject, match.group(3))
        elif (match.group(1) is not None and match.group(2) is not None):
            return (match.group(1).replace("/",""), match.group(2), match.group(3))
        return ("","","")

    match = GH_PARSE_REF_PAT.match(reftext)
    if match is not None:
        if (match.group(1) is not None):
            return (defaultowner, defaultproject, match.group(1))
        return ("","","")

    match = URL_REF_PAT.match(reftext)
    if match is not None:
        if (match.group(1) is not None and match.group(2) is not None and match.group(3) is not None):
            return (match.group(1).replace("/",""), match.group(2), match.group(3))
        return ("","","")

    raise Exception("No pattern matched " + reftext)


def test_parse_issue_reference():
    assert ("Aeij3-3", "f3A_re2", "432") == parse_issue_reference("Aeij3-3/f3A_re2#432", "asdf", "adfg"), "Bad project name parse "
    assert ("Aeij3-3", "adfg", "432") == parse_issue_reference("Aeij3-3#432", "asdf", "adfg"), "Bad project name parse default project "
    assert ("asdf", "adfg", "432") == parse_issue_reference("#432", "asdf", "adfg"), "Bad project name parse default project and user "
    assert ("asdf", "adfg", "432") == parse_issue_reference("GH-432", "asdf", "adfg"), "Bad project name parse gh plus default project and user "


def test_find_issue_references():
    feature_tester(find_issue_references, "Issue Refs", 
                            "#123 #456 alice#21 \n\r\n\r alice/projjy#42 https://github.com/bob/projjx/pull/15 stuff",
                            {"issues": [{"raw": "#123", "refstyle": "#d", "parts": ("%OWNER%", "%PROJECT%", "123")},
                                        {"raw": "#456", "refstyle": "#d", "parts": ("%OWNER%", "%PROJECT%", "456")},
                                        {"raw": "alice#21", "refstyle": "o#d", "parts": ("alice", "%PROJECT%", "21")},
                                        {"raw": "alice/projjy#42", "refstyle": "o/p#d", "parts": ("alice", "projjy", "42")},
                                        {"raw": "https://github.com/bob/projjx/pull/15", "refstyle": "url", "parts": ("bob", "projjx", "15")}]},
                            "#123 #456 alice#21 \n\r\n\r alice/projjy#42 https://github.com/bob/projjx/pull/15 stuff");

#
#  find_user_references: Sometimes in comments Github users are referred to with @username
#

USER_PAT = re.compile(ur'(@[A-Za-z][A-Z0-9a-z_]*)')
def find_user_references(features, text):
    refs = set()
    if "userref" not in features: features["userref"] = []
    for match in USER_PAT.finditer(text):
        refs.add(match.group(1))
    features["userref"].extend(refs)
    features["userref"].sort()
    return text

def test_find_user_references():
    feature_tester(find_user_references, "User refs",
                         "Please assign this to @CodeMonkey and notify @ManagerRob",
                         {"userref": ["@CodeMonkey", "@ManagerRob"]},
                         "Please assign this to @CodeMonkey and notify @ManagerRob")

#ISSUE_URL_PAT = re.compile(ur'https?://github.com/([A-Za-z][A-Z0-9a-z_]*)/([A-Za-z][A-Z0-9a-z_\.-]*)/issues?/(\d+)')
#PULL_URL_PAT = re.compile(ur'https?://github.com/([A-Za-z][A-Z0-9a-z_]*)/([A-Za-z][A-Z0-9a-z_\.-]*)/pull/(\d+)')
#USER_URL_PAT = re.compile(ur'https?://github.com/([A-Za-z][A-Z0-9a-z_]*)$')
#def reclassify_urls(features, text):
    #for u in features["urls"]:
        #imatch = ISSUE_URL_PAT.match(u)
        #if imatch: features["issues"].append(imatch.group(1) + "/" + imatch.group(2) + "#" + imatch.group(3))
        #imatch = PULL_URL_PAT.match(u)
        #if imatch: features["issues"].append(imatch.group(1) + "/" + imatch.group(2) + "#" + imatch.group(3))
        #imatch = USER_URL_PAT.match(u)
        #if imatch: features["userref"].append(imatch.group(1))
    #return text
#
#def test_reclassify_urls():
    #feature_tester(find_special, "Recognize issue reference by URL", "You saw ar/by#32 as http://github.com/al/ph/issues/15",
		#{ "issues": ["ar/by#32", "al/ph#15"], "plus_1": False, "code": [], "urls": ["http://github.com/al/ph/issues/15"], 
                    #"userref": [] },
                #"You saw ar/by#32 as http://github.com/al/ph/issues/15")

#
#   Call all of these functions in sequence, building up a feature set
#   with everything found.  
#
#

def find_special(features, text, issue_refs_only = False):
    original = text
    if not(isinstance(text, str) or isinstance(text, unicode)):
        return text
    if not(isinstance(features, dict)):
        raise ValueError("find_special: feature argument must be dict")

    if not issue_refs_only:
        find_imported_comment(features, text)
        find_plus_1(features, text)
    find_urls(features, text)
    noncode = find_included_code(features, text)
    find_issue_references(features, noncode)
    if not issue_refs_only:
        find_user_references(features, noncode)
    #reclassify_urls(features, text)
    return text

if __name__ == "__main__":
    print "Running tests"
    test_find_included_code()
    test_parse_issue_reference()
    test_find_issue_references()
    test_find_plus_1()
    test_find_urls()
    test_find_user_references()
    #test_reclassify_urls()
    print "Done with tests"
