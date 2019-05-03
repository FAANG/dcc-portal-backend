"""
This script generates the set of empty indices
1. es_index_prefix (CLI parameters)
2. type of the data (defined in the global variable TYPES)
"""
import os
import click
from elasticsearch import Elasticsearch
from constants import TYPES


# use click library to get command line parameters
@click.command()
@click.argument('es_index_prefix')
@click.option(
    '--es_host',
    default="http://wp-np3-e2:9200",
    help='Specify the Elastic Search server (port should be included), default to be http://wp-np3-e2:9200.'
)
@click.option(
    '--delete_only',
    default=False,
    help='Indicate whether to create empty indices, i.e. if set to True, this scripts turns to delete existing indices'
)
def main(es_host, es_index_prefix, delete_only) -> None:
    """
    Script to initialize/delete a build of indices determined by parameter es_index_prefix on Elastic Search server
    if parameter delete_only is true, only delete any existing indices matching the prefix pattern,
    no new indices will be created
    """
    """
    :param es_host: Elastic search host
    :param es_index_prefix: 
    :param delete_only: indicates whether it just deletes existing indices (True) or initialize as well (False)
    :return:
    """
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
            if delete_only:
                print(f"{es_index_prefix}_{es_type} deleted")

        if delete_only:
            continue

        # create index
        cmd = f"curl -X PUT '{prefix}_{es_type}' -H 'Content-Type: application/json' " \
            f"-d @../elasticsearch/faang_settings.json"
        print(cmd)
        os.system(cmd)
        print()
        # put the mapping
        cmd = f"curl -X PUT '{prefix}_{es_type}/_mapping/_doc' -H 'Content-Type: application/json' " \
            f"-d @../elasticsearch/{es_type}.mapping.json"
        print(cmd)
        os.system(cmd)
        print()


if __name__ == "__main__":
    main()
