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


def get_number_of_published_papers(data):
    """
    This function will return number of ids that have associated published papers
    :param data:
    :return: dict with yes and no as keys and number of documents for each category
    """
    paper_published_data = {
        'yes': 0,
        'no': 0
    }
    for item in data:
        if 'paperPublished' in item['_source'] and item['_source']['paperPublished'] == 'true':
            paper_published_data['yes'] += 1
        else:
            paper_published_data['no'] += 1
    return paper_published_data


def get_standard(data):
    """
    This function will return number of documents for each existing standard
    :param data: data to parse
    :return: dict with standards names as keys and number of documents with each standard as values
    """
    standard_data = dict()
    for item in data:
        standard_data.setdefault(item['_source']['standardMet'], 0)
        standard_data[item['_source']['standardMet']] += 1
    return standard_data


def create_summary_document_for_es(data):
    """
    This function will create document structure appropriate for es
    :param data: data to parse
    :return: part of document to be inserted into es
    """
    results = list()
    for k, v in data.items():
        results.append({
            "name": k,
            "value": v
        })
    return results


def create_summary_document_for_breeds(data):
    """
    This function will create document structure for breeds summary that are appropriate for es
    :param data: data to parse
    :return: part of document to be inserted into es
    """
    results = list()
    for k, v in data.items():
        tmp_list = list()
        for tmp_k, tmp_v in v.items():
            tmp_list.append({
                'name': tmp_k,
                'value': tmp_v
            })
        results.append({
            "name": k,
            "value": tmp_list
        })
    return results
