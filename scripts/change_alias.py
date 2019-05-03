from constants import TYPES, STAGING_NODE1
from elasticsearch import Elasticsearch
import click
import logging
from utils import create_logging_instance

from utils import remove_underscore_from_end_prefix

# logger = logging.getLogger(__name__)
# logging.basicConfig(format='%(asctime)s\t%(levelname)s:\t%(name)s line %(lineno)s\t%(message)s', level=logging.INFO)
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)
logger = create_logging_instance('change_alias', level=logging.INFO)


@click.command()
@click.option(
    '--es_hosts',
    default=STAGING_NODE1,
    help='Specify the Elastic Search server(s) (port could be included), e.g. wp-np3-e2:9200. '
         'If multiple servers are provided, please use ";" to separate them, e.g. "wp-np3-e2;wp-np3-e3"'
)
@click.option(
    '--es_index_prefix',
    default="",
    help='Specify the Elastic Search index prefix, e.g. '
         'faang_build_1 then the indices will be faang_build_1_experiment etc.'
         'If not provided, display the current aliases'
)
def main(es_hosts, es_index_prefix):
    """
    This tool helps user can switch aliases easily to different builds of indices
    In ES version 6 or after, no more type concept available. One build of indices
    share the same naming prefix as <es_index_prefix>_<type>. For example, faang_build_3_specimen
    has the es_index_prefix as faang_build_3 and the type is specimen.
    """
    change_aliases_object = ChangeAliases(es_hosts, es_index_prefix)
    change_aliases_object.run()


class ChangeAliases:
    """
    Using this class user can change aliases to point to new indexes
    We use this schema with indices: faang_build_{build number}_{index name}
    Currently master indices has build number 3
    """
    def __init__(self, es_hosts:str, es_index_prefix:str):
        """
        Initialize the tool
        :param es_hosts: Elastic Search host names as a string separated by ";"
        :param es_index_prefix: the index prefix which indicates the build of indices
        """
        hosts = es_hosts.split(";")
        logger.info("Command line parameters")
        logger.info("Hosts: " + str(hosts))
        if es_index_prefix:
            logger.info("Index_prefix:" + es_index_prefix)
        self.hosts = hosts
        self.es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
        self.es = Elasticsearch(hosts)
        self.current_aliases = dict()

    def run(self):
        """
        Main function to run aliases change
        """
        self.get_current_aliases()
        # no prefix parameter given, display current aliases
        if not self.es_index_prefix:
            logger.info("Current aliases in use: ")
            duplicate = set(TYPES)
            for alias, index in self.current_aliases.items():
                logger.info("{} -> {}".format(alias, index))
                duplicate.remove(alias)
            if duplicate:
                logger.warning(f"Some alias {str(duplicate)} has no corresponding index")
        else:
            # generate the indices list according to the given index prefix
            index_to_match = dict()
            for alias in TYPES:
                index_to_match[f"{self.es_index_prefix}_{alias}"] = alias
            # get all indices from the given host
            all_indices = self.get_all_indices()
            # the action list which contains all actions required to make the desired changes
            actions = list()
            # iterate all indices on the given ES server
            for index in all_indices:
                # current index needs to be assigned to one of aliases
                if index in index_to_match:
                    alias = index_to_match[index]
                    # check whether the alias has been already assigned
                    if alias in self.current_aliases:
                        current_alias = self.current_aliases[alias]
                        # already points to the wanted index
                        if current_alias == index:
                            logger.info(f"Alias {alias} has already been set to {current_alias}, nothing changed")
                            index_to_match.pop(index)
                            continue
                        # not then remove the old alias
                        else:
                            remove_action = {"index": current_alias, "alias": alias}
                            actions.append({"remove": remove_action})
                    add_action = {"index": index, "alias": alias}
                    actions.append({"add": add_action})
                    index_to_match.pop(index)
            # no actions to be taken, quit
            if not actions:
                # no actions could be two scenarios 1) already pointed to the wanted indices
                # 2) wanted indices not existing
                if index_to_match:
                    print(f"No matching existing indices have been found, please use command "
                        f"'curl {self.hosts[0]}/_cat/indices?v' to check all existing indices")
                exit()
            # use API to update aliases
            payload = {"actions": actions}
            self.es.indices.update_aliases(body=payload)

            if index_to_match:
                logger.warning("The following alias(es) has/ve not changed")
                for curr in index_to_match.keys():
                    logger.warning("alias " + index_to_match[curr] + " could not find the expected index " + curr)
            else:
                logger.info(f"Aliases have been updated using prefix {self.es_index_prefix}")
                logger.info("New aliases are as following")
                self.get_current_aliases()
                for alias, index in self.current_aliases.items():
                    logger.info("{} -> {}".format(alias, index))

    def get_all_indices(self):
        """
        retrieve all indices from given Elastic Search
        :return: list of names of all indices
        """
        logger.info("Get all indices")
        result = self.es.indices.get_alias("*")
        return list(result.keys())

    def get_current_aliases(self):
        """
        This function will pring current aliases in format 'index_name' -> 'alias_name'
        :param es_staging: staging elasticsearch object
        :return: name of the current prefix or suffix in use
        """
        # could not use the method below which raises error when one specified alias does not exist,
        # e.g. the pointed index gets deleted
        # result = self.es.indices.get_alias(name=','.join(INDICES))
        result = self.es.indices.get_alias("*")
        # aliases is a dict with index as keys and values are dicts with fixed key aliases
        # for example {'faang_build_3_experiment': {'aliases': {'experiment': {}}}}
        for index in result.keys():
            aliases = result[index]['aliases'].keys()
            for alias in aliases:
                if alias in TYPES:
                    self.current_aliases[alias] = index


if __name__ == "__main__":
    main()


