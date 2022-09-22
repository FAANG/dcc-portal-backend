"""
Different function that could be used in any faang backend script
"""
import logging
from typing import Set, List, Dict
from constants import STANDARD_FAANG, TYPES
from inspect import currentframe


def create_logging_instance(name, level=logging.INFO, to_file=True):
    """
    This function will create logger instance that will log information to {name}.log file
    Log example: 29-Mar-19 11:54:33 - DEBUG - This is a debug message
    :param name: name of the logger and file
    :param level: level of the logging
    :param to_file: indicates whether write to the file (True, default value) or the screen (False)
    :return: logger instance
    """
    # Create a custom logger
    new_logger = logging.getLogger(name)

    # Create handlers
    if to_file:
        f_handler = logging.FileHandler('{}.log'.format(name))
    else:
        f_handler = logging.StreamHandler()
#    f_handler = logging.FileHandler('{}.log'.format(name))
    # f_handler.setLevel(level)

    # Create formatters and add it to handlers
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - line %(lineno)s - %(message)s',
                                 datefmt='%y-%b-%d %H:%M:%S')
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    new_logger.addHandler(f_handler)
    new_logger.setLevel(level)
    return new_logger


def get_line_number():
    cf = currentframe()
    return cf.f_back.f_lineno


logger = create_logging_instance('utils')
logging.getLogger('elasticsearch').setLevel(logging.WARNING)


def insert_into_es(es, doc_type, doc_id, body):
    """
    index data into ES
    :param es: elasticsearch python library instance
    :param doc_type: determine which index to write into
    :param doc_id: the id of the document to be indexed
    :param body: the data of the document to be indexed
    :return:
    """
    try:
        existing_flag = es.exists(index=doc_type, doc_type="_doc", id=doc_id)
        if existing_flag:
            es.delete(index=doc_type, doc_type="_doc", id=doc_id)

        # add document to index
        es.index(index=doc_type, doc_type="_doc", id=doc_id, body=body)
    except Exception as e:
        logger.error(f"Error when try to insert into index {doc_type}: " + str(e.args))


def get_record_ids(es, data_type: str, only_faang=True) -> Set[str]:
    """
    Get the id list of existing records stored in the Elastic Search
    :param host: the Elastic Search server address
    :param data_type: the type of records
    :param only_faang: indiciates whether only include FAANG standard records (when True) or all records (when False)
    :return: set of FAANG dataset id
    """
    standard_field = 'standardMet'
    details = get_record_details(es, data_type, [standard_field])
    if len(details) == 0:
        return set()
    if only_faang:
        results = set()
        for hit in details.keys():
            if data_type == 'article' or details[hit][standard_field] == STANDARD_FAANG:
                results.add(hit)
        return results
    else:
        return set(details.keys())


def get_record_number(es, data_type: str) -> int:
    """
    Get the number of records of one type in the Elasticsearch, which is necessary to do a full list retrieval as
    the default size is 20 and an arbitrary hard-coded value is also not ideal, may become too small one day
    :param es: the Elastic Search instance
    :param data_type: the type of records
    :return: the number of records
    """
    if data_type not in TYPES:
        return 0

    count = es.cat.count(data_type, params={"format": "json"})
    return int(count[0]['count'])


def get_record_details(es, data_type: str, return_fields: List) -> Dict:
    """
    Get the subset of record details
    :param es: the Elastic Search instance
    :param data_type: the type of records
    :param return_fields: the list of fields containing the wanted information
    :return: a dict having record id as keys, and all field values as values
    """

    total_number = get_record_number(es, data_type)
    if total_number == 0:
        return dict()
    index_name = data_type

    count = 0
    results = dict()

    query_body = {"_source": return_fields,
                  "query": {"match_all": {}}}

    while True:
        res = es.search(index=index_name, size=50000, body=query_body, from_=count, track_total_hits=True)

        for hit in res['hits']['hits']:
            results[hit['_id']] = hit['_source']

        count += 50000

        if count > res['hits']['total']['value']:
            break
    return results


def es_fetch_records(indices, source_fields, sort, query_param, filters, aggregates, es):
    count = 0
    recordset = []

    while True:
        res = es.search(index=indices, _source=source_fields, size=50000, from_=count, track_total_hits=True)
        count += 50000
        records = list(map(lambda rec: rec['_source'], res['hits']['hits']))
        recordset += records
        if count > res['hits']['total']['value']:
            break
    return recordset