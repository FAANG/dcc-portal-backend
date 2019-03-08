# In Elastic Search V6, the FAANG backend is comprised of a set of indices
# To help development, it often needs to change among the set of indices
import requests
import sys
import re
import json

host: str = 'http://wp-np3-e2:9200'
# define the expected alias in the set
required_alias = {
    'organism': 1,
    'specimen': 1,
    "dataset": 1,
    "experiment": 1,
    "file": 1
}


def usage():
    print("Usage: python3 change_alias.py [index pattern] [prefix|suffix]")
    print("prefix|suffix indicates how to form the pattern")
    print("Example 1: python3 change_alias.py  lists current aliases")
    print("Example 2: python3 change_alias.py faang_build_1_ will link alias specimen to faang_build_1_specimen")
    print("Example 3: python3 change_alias.py 3 prefix will link alias specimen to specimen3")
    exit()


is_suffix = True
# the python file itself is the sys.argv[0]
len_argv = len(sys.argv)
if len_argv > 3:
    usage()
elif len_argv == 3:
    suffix = sys.argv[2].lower()
    if suffix != 'suffix' and suffix != 'prefix':
        print("Error: unrecognized suffix setting, must be either 'suffix' or 'prefix'")
        usage()
    if suffix == 'prefix':
        is_suffix = False
# get current aliases
current_alias = {}
print("Get current FAANG alias")
url = host + "/_aliases"
print(url)
r = requests.get(url).json()
if type(r) is dict:
    for index in r.keys():
        aliases = r[index]['aliases'].keys()
        for alias in aliases:
            if alias in required_alias:
                current_alias[alias] = index
else:
    print("Could not parse the returned content:\n"+str(r))

if len_argv == 1:
    for alias in current_alias.keys():
        print(alias+": "+current_alias[alias])
    exit()

# generate the index name according to the pattern
indices_to_match = {}
core = sys.argv[1]
for alias in required_alias:
    if is_suffix:
        index_to_match = core + alias
    else:
        index_to_match = alias + core
    indices_to_match[index_to_match] = alias
# print("The expected new aliases will be")
# print(indice_to_match)

# get all indices and iterate through them
# if the index name matches the patter, then add as the new alias while removing the old one
url = host + "/_cat/indices?v"
print("Get all indices")
response = requests.get(url).text
# split the text into lines and remove the headers
lines = response.split("\n")[1:]
pattern = re.compile("^\S+\s+\S+\s+(\S+)")
actions = []
for line in lines:
    m = re.search(pattern, line)
    if m:
        curr = m.group(1)
        if curr in indices_to_match:
            alias = indices_to_match[curr]
            # print("Assign alias " + alias + " to index "+curr)
            add_action = {"index": curr, "alias": alias}
            actions.append({"add": add_action})
            indices_to_match.pop(curr)
            if alias in current_alias:
                # print("Remove old alias "+current_alias[alias]+ " from "+alias)
                remove_action = {"index": current_alias[alias], "alias": alias}
                actions.append({"remove": remove_action})

if not actions:
    print("No matching existing indice have been found, please use command 'curl "
          + host + "/_cat/indices?v' to check all existing indice")
    exit()
payload = json.dumps({"actions": actions})
headers = {'Content-type': 'application/json'}
url = host + "/_aliases"
# print(payload)
r = requests.post(url, data = payload, headers = headers)
print(r.status_code, r.reason)
print(r.text)

if indices_to_match:
    print("The following alias(es) has/ve not changed")
    for curr in indices_to_match.keys():
        print("alias " + indices_to_match[curr] + " could not find the expected index " + curr)