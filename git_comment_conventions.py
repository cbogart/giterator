import re, urllib
import json
from bs4 import BeautifulSoup
import markdown

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
    assert mynewtext == newtext, comment + ": newtext wrong " + mynewtext
    assert set(feat.keys()) == set(newfeatures.keys()), comment + ": key differences"
    for k in feat:
        assert feat[k] == newfeatures[k], comment + ": " + k + " values differ (" + str(newfeatures[k]) + "!=" + str(feat[k]) + ")"

#
#  find_urls: Extract URLs from comments
#

# URL recognition regex from https://gist.github.com/uogbuji/705383
GRUBER_URLINTEXT_PAT = re.compile(ur'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?\xab\xbb\u201c\u201d\u2018\u2019]))')
def find_urls(features, text):
    urls = [ mgroups[0] for mgroups in GRUBER_URLINTEXT_PAT.findall(text) ]
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


def find_included_code(features, text):
    """Strips code out of text, and identifies the language"""
    md = markdown.markdown(text, extensions=['mdx_gfm'])
    parsed = BeautifulSoup(md, "html.parser")
    if "code" not in features: features["code"] = []
    for codeblock in parsed.findAll("code"):
        if codeblock.has_attr("class"):
            features["code"].append(codeblock["class"][0])
        else:
            features["code"].append("language-unspecified")
        codeblock.string = ""
    features["code"] = sorted(list(set(features["code"])))
    return parsed.text

def test_find_included_code():
    feature_tester(find_included_code, "Recognize and strip code language", "noncode\n```python\ncodestuff\n```\nmore noncode",
                   {"code": ["language-python"]}, "noncode\n\nmore noncode")
    feature_tester(find_included_code, "Recognize and strip unlabeled code language", "noncode\n```\ncodestuff\n```\nmore noncode",
                   {"code": ["language-unspecified"]}, "noncode\n\nmore noncode")
    feature_tester(find_included_code, "Recognize and strip 2 codes", "noncode\n```ruby\ncodestuff\n```\nmore noncode\n```\nmore\n```\nstuff",
                   {"code": ["language-ruby","language-unspecified"]}, "noncode\n\nmore noncode\n\nstuff")

#
# find_issue_references:
# Github-flavored markdown includes references to other issues, which may or may not be part
# of this same project
# 
#  In text associated with project P owner by user U,
#     #42  =  Issue 42 of project U/P
#     Q#42  = Issue 42 of project Q/U    (i.e. it's interpreted as a username)
#     T/Q#42  =  Issue 42 of project T/Q  
#
# In this recognizer we don't know the project context, so we just return blank for user 
#  or project, to be filled in elsewhere
#

#FULL_ISSUE_REF_PAT = re.compile(ur'(\S+/\S+#\d+)')
#PROJ_ISSUE_REF_PAT = re.compile(ur'(\S+#\d+)')
FULL_ISSUE_REF_PAT = re.compile(ur'([A-Za-z][A-Z0-9a-z_\.-]+/[A-Za-z][A-Z0-9a-z_\.-]+#\d+)')
PROJ_ISSUE_REF_PAT = re.compile(ur'([A-Za-z][A-Z0-9a-z_\.-]+#\d+)')
PLAIN_ISSUE_REF_PAT = re.compile(ur'(#\d+)')
def find_issue_references(features, text):
    """Gather refererences to other issues"""
    refs = set()
    t2 = text
    if "issues" not in features: features["issues"] = []
    for match in FULL_ISSUE_REF_PAT.finditer(t2):
        t2 = t2.replace(match.group(1), " ")
        refs.add(match.group(1))
    for match in PROJ_ISSUE_REF_PAT.finditer(t2):
        t2 = t2.replace(match.group(1), " ")
        refs.add(match.group(1))
    for match in PLAIN_ISSUE_REF_PAT.finditer(t2):
        t2 = t2.replace(match.group(1), " ")
        refs.add(match.group(1))

    features["issues"].extend(refs)
    features["issues"].sort()
    return text

PARSE_REF_PAT = re.compile(ur'([A-Za-z][A-Z0-9a-z_\.-]*/)?([A-Za-z][A-Z0-9a-z_\.-]*)?#(\d+)')
def parse_issue_reference(reftext, defaultowner, defaultproject):
    match = PARSE_REF_PAT.match(reftext)
    if (match is None):
        return ("","","")
    if (match.group(1) is None and match.group(2) is None):
        return (defaultowner, defaultproject, match.group(3))
    elif (match.group(1) is None and match.group(2) is not None):
        return (match.group(2), defaultproject, match.group(3))
    elif (match.group(1) is not None and match.group(2) is not None):
        return (match.group(1).replace("/",""), match.group(2), match.group(3))
    else:
        return ("","","")

def test_parse_issue_reference():
    assert ("Aeij3-3", "f3A_re2", "432") == parse_issue_reference("Aeij3-3/f3A_re2#432", "asdf", "adfg"), "Bad project name parse "
    assert ("Aeij3-3", "adfg", "432") == parse_issue_reference("Aeij3-3#432", "asdf", "adfg"), "Bad project name parse default project "
    assert ("asdf", "adfg", "432") == parse_issue_reference("#432", "asdf", "adfg"), "Bad project name parse default project and user "


def test_find_issue_references():
    feature_tester(find_issue_references, "Issue Refs", 
                            "#123 #456 alice#21 \n\r\n\r bob/alice#42",
                            {"issues": ["#123","#456","alice#21","bob/alice#42"]},
                            "#123 #456 alice#21 \n\r\n\r bob/alice#42");

#
#  find_user_references: Sometimes in comments Github users are referred to with @username
#

USER_PAT = re.compile(ur'(@[A-Za-z][A-Z0-9a-z_\.-]*)')
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

ISSUE_URL_PAT = re.compile(ur'https?://github.com/([A-Za-z][A-Z0-9a-z_\.-]*)/([A-Za-z][A-Z0-9a-z_\.-]*)/issues?/(\d+)')
PULL_URL_PAT = re.compile(ur'https?://github.com/([A-Za-z][A-Z0-9a-z_\.-]*)/([A-Za-z][A-Z0-9a-z_\.-]*)/pull/(\d+)')
USER_URL_PAT = re.compile(ur'https?://github.com/([A-Za-z][A-Z0-9a-z_\.-]*)$')
def reclassify_urls(features, text):
    for u in features["urls"]:
        imatch = ISSUE_URL_PAT.match(u)
        if imatch: features["issues"].append(imatch.group(1) + "/" + imatch.group(2) + "#" + imatch.group(3))
        imatch = PULL_URL_PAT.match(u)
        if imatch: features["issues"].append(imatch.group(1) + "/" + imatch.group(2) + "#" + imatch.group(3))
        imatch = USER_URL_PAT.match(u)
        if imatch: features["userref"].append(imatch.group(1))
    return text


#
#   Call all of these functions in sequence, building up a feature set
#   with everything found.  
#
#

def find_special(features, text):
    original = text
    if not(isinstance(text, str) or isinstance(text, unicode)):
        return text
    if not(isinstance(features, dict)):
        raise ValueError("find_special: feature argument must be dict")
    find_imported_comment(features, text)
    find_plus_1(features, text)
    find_urls(features, text)
    noncode = find_included_code(features, text)
    find_issue_references(features, noncode)
    find_user_references(features, noncode)
    reclassify_urls(features, text)
    return text

if __name__ == "__main__":
    print "Running tests"
    test_find_included_code()
    test_parse_issue_reference()
    test_find_issue_references()
    test_find_plus_1()
    test_find_urls()
    test_find_user_references()
    print "Done with tests"
