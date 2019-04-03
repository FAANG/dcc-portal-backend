"""
Different function that could be used in any faang backend script
"""
import logging
import sys

from constants import *


def create_logging_instance(name):
    """
    This function will create logger instance that will log information to {name}.log file
    Log example: 29-Mar-19 11:54:33 - DEBUG - This is a debug message
    :param name: name of the logger and file
    :return: logger instance
    """
    # Create a custom logger
    logger = logging.getLogger(name)

    # Create handlers
    f_handler = logging.FileHandler('{}.log'.format(name))
    f_handler.setLevel(logging.DEBUG)

    # Create formatters and add it to handlers
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(f_handler)
    return logger


def print_current_aliases(es_staging):
    """
    This function will pring current aliases in format 'index_name' -> 'alias_name'
    :param es_staging: staging elasticsearch object
    :return: name of the current prefix or suffix in use
    """
    name = set()
    aliases = es_staging.indices.get_alias(name=','.join(INDICES))
    for index_name, alias in aliases.items():
        alias = list(alias['aliases'].keys())[0]
        name.add(index_name.split(alias)[0])
        print("{} -> {}".format(index_name, alias))
    if len(name) != 1:
        print("There are multiple prefixes or suffixes in use, manual check is required!")
        sys.exit(0)
    else:
        return list(name)[0]
