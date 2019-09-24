"""
Different function that could be used in any faang backend script
"""
import logging
from typing import Set
import requests
import constants
from misc import convert_readable

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
    logger = logging.getLogger(name)

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
    logger.addHandler(f_handler)
    logger.setLevel(level)
    return logger


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
        # TODO logging error
        logger.error(f"Error when try to insert into index {es_index_prefix}_{doc_type}: " + str(e.args))


def get_datasets(host: str, es_index_prefix: str, only_faang=True) -> Set[str]:
    """
    Get the id list of existing datasets stored in the Elastic Search
    :param host: the Elastic Search server address
    :param es_index_prefix: the Elastic Search dataset index
    :param only_faang: indiciates whether only include FAANG standard datasets (when True) or all datasets (when False)
    :return: set of FAANG dataset id
    """
    url = f"http://{host}/{es_index_prefix}_dataset/_search?_source=standardMet"
    response = requests.get(url).json()
    total_number = response['hits']['total']
    if total_number == 0:
        return set()
    datasets = set()
    url = f"{url}&size={total_number}"
    response = requests.get(url).json()
    for hit in response['hits']['hits']:
        if only_faang:
            if hit['_source']['standardMet'] == constants.STANDARD_FAANG:
                datasets.add(hit['_id'])
        else:
            datasets.add(hit['_id'])
    return datasets


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
    # for ENA, it is fixed to MD5 as the checksum method
    checksums = record["submitted_md5"].split(";")
    if len(files) != len(checksums) or len(files) != len(sizes) or len(files) == 0:
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
            es_doc['fileTypes'].append(suffix)
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
    if record['study_accession'] in existing_datasets:
        es_doc['datasetInPortal'] = True
    else:
        es_doc['datasetInPortal'] = False
    es_doc.setdefault('sampleAccessions', list())
    es_doc['sampleAccessions'] = record['sample_accession']

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
    for data source, the preference in the order of fastq, sra, cram_index
    for file type, the preference in the order of ftp, galaxy, aspera
    the order is from the observation of data availabilities in those fields, so subject to change
    :param record: one data record from ENA API
    :return: the predicted file type and source type, if not found, return two empty strings
    """
    data_sources = ['fastq', 'sra', 'cram_index']
    data_types = ['ftp', 'galaxy', 'aspera']
    file_type = ''
    source_type = ''
    for data_source in data_sources:
        for my_type in data_types:
            key_to_check = f"{data_source}_{my_type}"
            if key_to_check in record and record[key_to_check] != '':
                file_type = my_type
                source_type = data_source
                return file_type, source_type
    return file_type, source_type


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


def remove_underscore_from_end_prefix(es_index_prefix: str)->str:
    """
    Remove the last underscore from index prefix if existing
    :param es_index_prefix: the index prefix may having underscore at the end
    :return: the 'cleaned' index prefix
    """
    if es_index_prefix.endswith("_"):
        str_len = len(es_index_prefix)
        es_index_prefix = es_index_prefix[0:str_len - 1]
    return es_index_prefix
