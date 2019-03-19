# In Elastic Search V6, the FAANG backend is comprised of a set of indices
# To help development, the set of indice often needs to be backed up
# This script takes two or three (not supported yet) parameters. The first two parameters defined pattern
# for example, faang_ parameter means that the matched indice would be faang_organism, faang_specimen etc.
# The script copy all indices matching the first parameter to the new indices generating from the second parameter
import subprocess
import sys

def usage():
    print("Usage: python copy_elastic_indice.py <input index pattern> <output index pattern> [prefix|suffix]")
    exit()

len_argv = len(sys.argv)
if len_argv != 3 and len_argv != 4:
    print(len_argv)
    usage()

host: str = 'http://wp-np3-e2:9200/'
# define the expected alias in the set
required_alias = {
    'organism': 1,
    'specimen': 1,
    "dataset": 1,
    "experiment": 1,
    "file": 1
}

for index in required_alias.keys():
    arr = ["elasticdump", "--input=" + host + sys.argv[1] + index, "--output=" + host + sys.argv[2] + index,
           "--type=data"]
    cmd = " ".join(arr)
    print(cmd)
    subprocess.run(arr)