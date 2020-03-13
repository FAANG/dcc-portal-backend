"""
Different function that could be used in any faang backend script
"""
import json
import logging
from typing import Set, List, Dict
import requests
from constants import STANDARDS, STANDARD_FAANG, TYPES
from elasticsearch import Elasticsearch
from misc import convert_readable
from datetime import datetime
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


def write_system_log(es, script, level: str, line, detail, to_es=True):
    now = datetime.now()
    if to_es:
        tmp = es.count(index='sys_log')
        current_count = tmp['count'] + 1
        doc = {
            'serial': current_count,
            'script': script,
            'level': level,
            'timestamp': now,
            'line': line,
            'detail': detail
        }
        es.create(index='sys_log', doc_type='_doc', body=doc, id=f'{script}-{now}', refresh=True)
    else:
        print(f'{now} - {script} - {level.upper()} - line {line} - {detail}')


def get_line_number():
    cf = currentframe()
    return cf.f_back.f_lineno


logger = create_logging_instance('utils')
logging.getLogger('elasticsearch').setLevel(logging.WARNING)


def insert_into_es(es, es_index_prefix, doc_type, doc_id, body):
    """
    index data into ES
    :param es: elasticsearch python library instance
    :param es_index_prefix: combined with doc_type to determine which index to write into
    :param doc_type: combined with es_index_prefix to determine which index to write into
    :param doc_id: the id of the document to be indexed
    :param body: the data of the document to be indexed
    :return:
    """
    try:
        existing_flag = es.exists(index=f'{es_index_prefix}_{doc_type}', doc_type="_doc", id=doc_id)
        if existing_flag:
            es.delete(index=f'{es_index_prefix}_{doc_type}', doc_type="_doc", id=doc_id)
        es.create(index=f'{es_index_prefix}_{doc_type}', doc_type="_doc", id=doc_id, body=body)
    except Exception as e:
        logger.error(f"Error when try to insert into index {es_index_prefix}_{doc_type}: " + str(e.args))


def insert_es_log(es, es_index_prefix, doc_type, doc_id, status, detail):
    """
    insert a log entry for the data record into ES
    :param es: elasticsearch python library instance
    :param es_index_prefix: form the string <es_index_prefix>_log, which is the index tge log entry will be written into
    :param doc_type: the type of the data record
    :param doc_id: the id of the data record
    :param status: the status of the data record during importation, one of 'pass', 'warning', 'error'
    :param detail: the detail of the import log, empty if the status is pass
    """
    now = datetime.now()
    doc = {
        'accession': doc_id,
        'type': doc_type,
        'status': status,
        'detail': detail,
        'last_update': now
    }
    insert_into_es(es, es_index_prefix, 'log', doc_id, doc)


def get_record_ids(host: str, es_index_prefix: str, data_type: str, only_faang=True) -> Set[str]:
    """
    Get the id list of existing records stored in the Elastic Search
    :param host: the Elastic Search server address
    :param es_index_prefix: the Elastic Search dataset index
    :param data_type: the type of records
    :param only_faang: indiciates whether only include FAANG standard records (when True) or all records (when False)
    :return: set of FAANG dataset id
    """
    standard_field = 'standardMet'
    details = get_record_details(host, es_index_prefix, data_type, [standard_field])
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


def get_record_number(host: str, es_index_prefix: str, data_type: str) -> int:
    """
    Get the number of records of one type in the Elasticsearch, which is necessary to do a full list retrieval as
    the default size is 20 and an arbitrary hard-coded value is also not ideal, may become too small one day
    :param host: the Elastic Search server address
    :param es_index_prefix: the Elastic Search dataset index
    :param data_type: the type of records
    :return: the number of records
    """
    if data_type not in TYPES:
        return 0
    url = f"http://{host}/{es_index_prefix}_{data_type}/_search?_source=standardMet"
    response = requests.get(url).json()
    return response['hits']['total']


def get_record_details(host: str, es_index_prefix: str, data_type: str, return_fields: List) -> Dict:
    """
    Get the subset of record details
    :param host: the Elastic Search server address
    :param es_index_prefix: the Elastic Search index
    :param data_type: the type of records
    :param return_fields: the list of fields containing the wanted information
    :return: a dict having record id as keys, and all field values as values
    """
    total_number = get_record_number(host, es_index_prefix, data_type)
    if total_number == 0:
        return dict()
    es = Elasticsearch(host)
    index_name = f'{es_index_prefix}_{data_type}'
    source_str = ','.join(return_fields)
    data = es.search(index=index_name, size=total_number, _source=source_str)
    results = dict()
    for hit in data['hits']['hits']:
        results[hit['_id']] = hit['_source']
    return results


def convert_analysis(record, existing_datasets):
    file_server_types = ['ftp', 'galaxy', 'aspera']
    file_server_type = ''
    for tmp in file_server_types:
        key_to_check = f"submitted_{tmp}"
        if key_to_check in record and record[key_to_check] != '':
            file_server_type = tmp
            break
    if len(file_server_type) == 0:
        return dict()

    es_doc = dict()
    files = record[f"submitted_{file_server_type}"].split(";")
    sizes = record["submitted_bytes"].split(";")
    formats = record["submitted_format"].lower().split(";")
    # for ENA, it is fixed to MD5 as the checksum method
    checksums = record["submitted_md5"].split(";")
    if len(files) != len(checksums) or len(files) != len(sizes) or len(files) != len(formats) or len(files) == 0:
        return dict()
    for i, file in enumerate(files):
        fullname = file.split("/")[-1]
        # filename = fullname.split(".")[0]
        suffix = fullname.split(".")[-1]
        if suffix != 'md5':
            es_doc.setdefault('fileNames', list())
            es_doc.setdefault('fileTypes', list())
            es_doc.setdefault('fileSizes', list())
            es_doc.setdefault('checksumMethods', list())
            es_doc.setdefault('checksums', list())
            es_doc.setdefault('urls', list())
            es_doc['fileNames'].append(fullname)
            es_doc['fileTypes'].append(formats[i])
            es_doc['fileSizes'].append(convert_readable(sizes[i]))
            es_doc['checksumMethods'].append('md5')
            es_doc['checksums'].append(checksums[i])
            es_doc['urls'].append(file)
    es_doc['accession'] = record['analysis_accession']
    es_doc['title'] = record['analysis_title']
    es_doc['alias'] = record['analysis_alias']
    es_doc['releaseDate'] = record['first_public']
    es_doc['updateDate'] = record['last_updated']
    es_doc.setdefault('organism', dict())
    es_doc['organism']['text'] = record['scientific_name']
    es_doc['organism']['ontologyTerms'] = f"http://purl.obolibrary.org/obo/NCBITaxon_{record['tax_id']}"

    es_doc['datasetAccession'] = record['study_accession']
    es_doc['datasetInPortal'] = True if record['study_accession'] in existing_datasets else False
    es_doc.setdefault('sampleAccessions', list())

    # es_doc['analysisDate'] = record['analysis_alias']
    es_doc['analysisCenter'] = record['center_name']
    es_doc['analysisType'] = record['analysis_type']
    return es_doc


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
        standard = item['_source'].get('standardMet', None)
        standard_data.setdefault(standard, 0)
        standard_data[standard] += 1
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
                'breedsName': tmp_k,
                'breedsValue': tmp_v
            })
        results.append({
            "speciesName": k,
            "speciesValue": tmp_list
        })
    return results


def determine_file_and_source(record):
    """
    predict the combination of data source and data type to use for file information
    for file type, the preference in the order of fastq, sra, cram_index
    for source type, the preference in the order of ftp, galaxy, aspera
    the order is from the observation of data availabilities in those fields, so subject to change
    :param record: one data record from ENA API
    :return: the predicted file type and source type, if not found, return two empty strings
    """
    file_types = ['fastq', 'sra', 'cram_index']
    source_types = ['ftp', 'galaxy', 'aspera']
    for file_type in file_types:
        for source_type in source_types:
            key_to_check = f"{file_type}_{source_type}"
            if key_to_check in record and record[key_to_check] != '':
                return file_type, source_type
    return '', ''


def check_existsence(data_to_check, field_to_check):
    """
    Check whether a field exists in the data record and return the corresponding value
    :param data_to_check: the record data
    :param field_to_check: the name of the field
    :return: if exists, return the value holding for the field, if not, return None
    """
    if field_to_check in data_to_check:
        if len(data_to_check[field_to_check]) == 0:
            return None
        else:
            return data_to_check[field_to_check]
    else:
        return None


def remove_underscore_from_end_prefix(es_index_prefix: str) -> str:
    """
    Remove the last underscore from index prefix if existing
    :param es_index_prefix: the index prefix may having underscore at the end
    :return: the 'cleaned' index prefix
    """
    if es_index_prefix.endswith("_"):
        str_len = len(es_index_prefix)
        es_index_prefix = es_index_prefix[0:str_len - 1]
    return es_index_prefix


def generate_ena_api_endpoint(result: str, data_portal: str, fields: str, optional: str = ''):
    """
    Generate the url for ENA API endpoint
    :param result: either be read_run (for experiment, file, dataset import) or analysis (for analysis import)
    :param data_portal: either ena (legacy data) or faang (faang data)
    :param fields: all (only faang data supports all) or list of fields separated by ',' (for legacy data)
    :param optional: optional constraint, e.g. species
    :return: the generated url
    """
    if optional == "":
        return f"https://www.ebi.ac.uk/ena/portal/api/search/?" \
           f"result={result}&format=JSON&limit=0&fields={fields}&dataPortal={data_portal}"
    else:
        return f"https://www.ebi.ac.uk/ena/portal/api/search/?" \
           f"result={result}&format=JSON&limit=0&{optional}&fields={fields}&dataPortal={data_portal}"


def process_validation_result(analyses, es, es_index_prefix, validation_results, ruleset_version, rulesets, logger_in):
    analysis_validation = dict()
    for analysis_accession, analysis_es in analyses.items():
        status = ''
        msgs = list()
        for ruleset in rulesets:
            if validation_results[ruleset]['detail'][analysis_accession]['status'] == 'error':
                status = 'error'
                message = validation_results[ruleset]['detail'][analysis_accession]['message']
                msgs.append(f"error\t{ruleset}\t{message}")
                logger_in.info(f"{analysis_accession}\tAnalysis\terror\t{message}")
            else:
                # only indexing when meeting standard
                analysis_validation[analysis_accession] = STANDARDS[ruleset]
                analysis_es['standardMet'] = STANDARDS[ruleset]
                if analysis_es['standardMet'] == STANDARD_FAANG:
                    analysis_es['versionLastStandardMet'] = ruleset_version

                files_es: List[Dict] = list()
                for i in range(0, len(analysis_es['fileNames'])):
                    file_es: Dict = dict()
                    file_es['name'] = analysis_es['fileNames'][i]
                    file_es['type'] = analysis_es['fileTypes'][i]
                    file_es['size'] = analysis_es['fileSizes'][i]
                    file_es['checksumMethod'] = 'md5sum'
                    file_es['checksum'] = analysis_es['checksums'][i]
                    file_es['url'] = analysis_es['urls'][i]
                    files_es.append(file_es)
                analysis_es['files'] = files_es
                analysis_es.pop('fileNames')
                analysis_es.pop('fileTypes')
                analysis_es.pop('fileSizes')
                analysis_es.pop('checksumMethods')
                analysis_es.pop('checksums')
                analysis_es.pop('urls')
                body = json.dumps(analysis_es)
                insert_into_es(es, es_index_prefix, 'analysis', analysis_accession, body)
                status = validation_results[ruleset]['detail'][analysis_accession]['status']
                message = validation_results[ruleset]['detail'][analysis_accession]['message']
                msgs.append(f"{status}\t{ruleset}\t{message}")
                # index into ES so break the loop
                break
        insert_es_log(es, es_index_prefix, 'analysis', analysis_accession, status, ";".join(msgs))


