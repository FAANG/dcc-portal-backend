import requests
from typing import Dict
import click
TYPES = ["organism", "specimen", "dataset", "experiment", "file"]

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
def main(es_host, es_index_prefix, serial):
    numbers: Dict[str, int] = read_number_from_es(es_host)
    header = "\t".join(TYPES)
    print("\t"+header)
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


def read_number_from_es(es_host):
    counts = {}
    url = f"{es_host}/_cat/indices?v"
    response = requests.get(url).text
    # removes the header
    lines = response.split("\n")[1:]

    for line in lines:
        elmts = line.split()
        if elmts:
            counts[elmts[2]] = elmts[6]

    return counts


if __name__ == "__main__":
    main()
