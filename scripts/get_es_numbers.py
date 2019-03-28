import requests
from typing import Dict
TYPES = ["organism", "specimen", "dataset", "experiment", "file"]
ES_HOST = 'http://wp-np3-e2:9200'


def main():
    numbers: Dict[str, int] = read_number_from_es()
    header = "\t".join(TYPES)
    print("\t"+header)
    for i in range(1, 5):
        index_base = f"faang_build_{str(i)}"
        print(index_base, end='')
        for type in TYPES:
            index = f"{index_base}_{type}"
            count = 0
            if index in numbers:
                count = numbers[index]
            print(f"\t{count}", end='')
        print()


def read_number_from_es():
    counts = {}
    url = "{}/_cat/indices?v".format(ES_HOST)
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
