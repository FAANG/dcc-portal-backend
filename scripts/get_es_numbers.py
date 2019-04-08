"""
This script gets the number of records in all specified indices,
which is very useful in developing codes to populate ES
The indices are specified by the combination value of
1. es_index_prefix (CLI parameters)
2. serial number (CLI parameters)
3. type of the data (defined in the global variable TYPES
"""

import requests
from typing import Dict
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
    default="faang_build",
    help='Specify the Elastic Search index prefix, default to be faang_build.'
         'Combined with serial (default 3) then the indices will be faang_build_1_organism etc.'
)
@click.option(
    '--serial',
    default=3,
    help="Combined with es_index_prefix"
)
def main(es_host, es_index_prefix, serial) -> None:
    """
    main function of the script. A typical index is in the pattern of <index prefix>_<number>_<type>
    :param es_host: elastic search host address parameter in CLI
    :param es_index_prefix: elastic search index prefix parameter in CLI
    :param serial: defines the maximum number to be searched
    :return: nothing
    """
    # keys are the founded indices on the ES server located at es_host
    # values are the corresponding number of record in the index
    numbers: Dict[str, int] = read_number_from_es(es_host)
    # making header line
    header = "\t".join(TYPES)
    print("\t"+header)
    # generate all combinations of indices and look up in the numbers dict
    # There will be {serial} rows and each row will have 1+number of types columns
    for i in range(1, serial+1):
        index_base = f"{es_index_prefix}_{str(i)}"
        print(index_base, end='')
        for type in TYPES:
            index = f"{index_base}_{type}"
            count = 0
            if index in numbers:
                count = numbers[index]
            print(f"\t{count}", end='')
        print()


def read_number_from_es(es_host) -> Dict[str, int]:
    """
    Get numbers of records
    :param es_host: the address where the elastic search is hosted
    :return: numbers of records in all indices found at the given address
    """
    counts = {}
    url = f"{es_host}/_cat/indices?v"
    """
    example of response
    health status index                    uuid                   pri rep docs.count docs.deleted store.size 
    pri.store.size
    green  open   faang_build_2_specimen   bo3YAiLmSUWqlgtzVElzKQ   5   1       9388            0     18.4mb            
    9mb
    green  open   faang_build_4_specimen   _jSFP7MRS6qVrkiqcQ9f4Q   5   1       9388            0       19mb          
    9.6mb
    """
    response = requests.get(url).text
    # removes the header of returned value
    lines = response.split("\n")[1:]
    print(response)
    for line in lines:
        elmts = line.split()
        if elmts:
            # 3rd column is the index name and 7th column is docs.count
            counts[elmts[2]] = elmts[6]

    return counts


if __name__ == "__main__":
    main()
