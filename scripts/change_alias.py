from elasticsearch import Elasticsearch
import argparse

from utils import *


class ChangeAliases:
    def __init__(self, name, es_staging):
        self.name = name
        self.es_staging = es_staging

    def run(self):
        print("Current aliases in use: ")
        old_index_prefix = print_current_aliases(self.es_staging)
        if old_index_prefix == self.name:
            print("Prefix is already in use, exiting!")
            sys.exit(0)
        else:
            for index in INDICES:
                new_index = "{}{}".format(self.name, index)
                old_index = "{}{}".format(old_index_prefix, index)
                self.change_aliases(new_index, old_index, index)
            print("New aliases: ")
            print_current_aliases(self.es_staging)

    def change_aliases(self, new_index, old_index, alias_name):
        actions = {"actions": [
            {"remove": {"index": old_index, "alias": alias_name}},
            {"add": {"index": new_index, "alias": alias_name}},
        ]}
        self.es_staging.indices.update_aliases(body=actions)


if __name__ == "__main__":
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', help='Name of Prefix for new indexes')
    args = parser.parse_args()

    # If args were not provided print current aliases, usage and halt
    if not args.name:
        print("Print current aliases in use:")
        print_current_aliases(es_staging)
        print()
        parser.print_help()
        sys.exit(0)
    else:
        name = args.name

    change_aliases_object = ChangeAliases(name, es_staging)
    change_aliases_object.run()

