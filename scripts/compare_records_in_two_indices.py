"""
compare two versions of same type records stored in the two different indices
"""
import click
import requests
from typing import Set
import constants
from utils import remove_underscore_from_end_prefix

@click.command()
@click.option(
    '--es_host',
    default=f"http://{constants.STAGING_NODE1}",
    help='Specify the Elastic Search host, default to be wp-np3-e2'
)
@click.option(
    '--es_index_1',
    help='Specify the first index, mandatory field'
)
@click.option(
    '--es_index_2',
    help='Specify the second index, mandatory field'
)
@click.option(
    '--es_type',
    help='Specify the type of data to be comapred, mandatory field'
)
def main(es_host, es_index_1, es_index_2, es_type):
    """
    The main function
    :param es_host: elastic search host server
    :param es_index_1: the index prefix 1
    :param es_index_2: the index prefix 2
    :param es_type: the type of records to be compared
    :return:
    """
    error_flag = False
    if not es_index_1:
        print("mandatory parameter es_index_1 is not provided")
        error_flag = True
    if not es_index_2:
        print("mandatory parameter es_index_2 is not provided")
        error_flag = True
    if not es_type:
        print("mandatory parameter es_type is not provided")
        error_flag = True
    else:
        if es_type not in constants.INDICES:
            print("Unrecognized type which must be one of {}".format(",".join(constants.INDICES)))
            error_flag = True
    if error_flag:
        exit()

    es_index_1 = remove_underscore_from_end_prefix(es_index_1)
    es_index_2 = remove_underscore_from_end_prefix(es_index_2)
    url_1: str = f"{es_host}/{es_index_1}_{es_type}/_search?_source=_id&size=100000"
    url_2: str = f"{es_host}/{es_index_2}_{es_type}/_search?_source=_id&size=100000"
    resp1 = get_ids(url_1)
    resp2 = get_ids(url_2)
    for record_id in sorted(resp1):
        if record_id in resp2:
            resp2.remove(record_id)
        else:
            print(f"Only in {es_index_1}_{es_type}: {record_id}")

    if resp2:
        for record_id in sorted(resp2):
            print(f"Only in {es_index_2}_{es_type}: {record_id}")


def get_ids(url: str) -> Set[str]:
    """
    Return the id list in the form of Dict
    :param url: used to be curled
    :return: the id list
    """
    response = requests.get(url).json()
    results = set()
    for hit in response['hits']['hits']:
        results.add(hit['_id'])
    return results


if __name__ == "__main__":
    main()
