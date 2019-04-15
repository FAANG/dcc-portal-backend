"""
This script generates the set of empty indices
1. es_index_prefix (CLI parameters)
2. type of the data (defined in the global variable TYPES
"""

from elasticsearch import Elasticsearch
import os
import click


TYPES = ["organism", "specimen", "dataset", "experiment", "file"]


# use click library to get command line parameters
@click.command()
@click.option(
    '--es_host',
    default="http://wp-np3-e2:9200",
    help='Specify the Elastic Search server (port should be included), default to be http://wp-np3-e2:9200.'
)
@click.option(
    '--es_index_prefix',
    help='Specify the Elastic Search index prefix'
)
def main(es_host, es_index_prefix) -> None:
    # check mandatory parameter
    es = Elasticsearch(es_host)
    if not es_index_prefix:
        print("Please provide value for es_index_prefix")
        exit()

    prefix = f"{es_host}/{es_index_prefix}"
    for es_type in TYPES:
        # delete the current index if existing
        flag = es.indices.exists(f"{es_index_prefix}_{es_type}")
        if flag:
            es.indices.delete(f"{es_index_prefix}_{es_type}")
        # create index
        cmd = f"curl -X PUT '{prefix}_{es_type}' -H 'Content-Type: application/json' -d @faang_settings.json"
        print(cmd)
        os.system(cmd)
        print()
        # put the mapping
        cmd = f"curl -X PUT '{prefix}_{es_type}/_mapping/_doc' -H 'Content-Type: application/json' " \
            f"-d @{es_type}.mapping.json"
        print(cmd)
        os.system(cmd)
        print()


if __name__ == "__main__":
    main()
