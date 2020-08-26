"""
This script reads from ENA FAANG data portal, parses and validates the data and stores into Elastic Search
It is highly recommended to check out the corresponding rule set
https://github.com/FAANG/faang-metadata/blob/master/rulesets/faang_experiments.metadata_rules.json
to help understanding the code
"""
import click
from constants import TECHNOLOGIES, STANDARDS, CATEGORIES, SPECIES_DICT, EXPERIMENT_TARGETS
from elasticsearch import Elasticsearch
import constants
from typing import Set, Dict, List
from utils import determine_file_and_source, check_existsence, remove_underscore_from_end_prefix, \
    write_system_log, get_line_number, insert_into_es, get_record_ids, generate_ena_api_endpoint
import re
import validate_experiment_record
import sys
import json
import requests
from misc import convert_readable, parse_date

SCRIPT_NAME = 'import_ena_legacy'

to_es_flag = True
es = None

# in FAANG ruleset each technology has mandatory fields in the corresponding section, which is not expected in the
# general ENA datasets, so only validate against Legacy standard
RULESETS = ["FAANG Legacy Experiments"]

SPECIES_TAXONOMY_LIST = list(SPECIES_DICT.keys())

# holds all sample records from ES
BIOSAMPLES_RECORDS = dict()
# holds the material information for all records encountered (retrieve_biosample_record function)
# keys are biosamples accessions and values are dicts which have three sub keys
# confirmed (boolean), material (dict), accession (str)
CACHED_MATERIAL = dict()

# control which assay types to be imported
ASSAY_TYPES_TO_BE_IMPORTED = {
    "ATAC-seq": "ATAC-seq",
    "BS-Seq": "methylation profiling by high throughput sequencing",
    "Hi-C": "Hi-C",
    "DNase": "DNase-Hypersensitivity seq",
    "RNA-Seq": "transcription profiling by high throughput sequencing",
    "miRNA": "microRNA profiling by high throughput sequencing",
    "WGS": "whole genome sequencing assay",
    "ChIP-Seq": "ChIP-seq"
}
# value of all is not allowed in the general ena data portal, not like FAANG, so need to list fields
# which we want to retrieve
FIELD_LIST = [
    'study_accession', 'secondary_study_accession', 'sample_accession', 'experiment_accession', 'run_accession',
    'submission_accession', 'tax_id', 'instrument_platform', 'instrument_model', 'library_strategy',
    'library_selection', 'read_count', 'base_count', 'first_public', 'last_updated',
    'study_title', 'study_alias', 'run_alias', 'fastq_bytes', 'fastq_md5',
    'fastq_ftp', 'fastq_aspera', 'fastq_galaxy', 'submitted_format', 'submitted_ftp', 'submitted_md5',
    'submitted_bytes', 'sra_bytes',
    'sra_md5', 'sra_ftp', 'sra_aspera', 'sra_galaxy', 'cram_index_ftp',
    'cram_index_aspera', 'cram_index_galaxy', 'project_name'
]


def get_biosamples_records_from_es(host, es_index_prefix, es_type):
    """
    Get existing biosample records from elastic search
    :param host: elasticsearch python instance
    :param es_index_prefix: the name of the index set
    :param es_type: type of record, either organism or specimen
    """
    global BIOSAMPLES_RECORDS
    url = f'http://{host}/{es_index_prefix}_{es_type}/_search?size=100000'
    response = requests.get(url).json()
    if 'hits' not in response:
        write_system_log(es, SCRIPT_NAME, 'error', get_line_number(),
                         f'No data retrieved from {url}, please double check whether the URL is correct', to_es_flag)
        return
    for item in response['hits']['hits']:
        BIOSAMPLES_RECORDS[item['_id']] = item['_source']


def retrieve_biosamples_record(es_index_prefix, biosample_id):
    """
    retrieve biosample record on the fly which referenced in the non-FAANG ENA study and store into ES
    status values:
    0: already existing in ES, nothing done in this function
    200: successfully retrieved record
    all other values: error to retrieve the record
    :param es_index_prefix: Elastic search index prefix
    :param biosample_id: BioSanples id
    :return: status code
    """

    # BIOSAMPLE_RECORDS stores the full details of records in ES
    # CACHED_MATERIAL only stores the material type of records and the confirmation status
    # which also indicates whether the record has been processed or not
    global CACHED_MATERIAL
    # check whether the biosamples id has been dealt with by this script
    if biosample_id in CACHED_MATERIAL:
        return 0
    # already in ES, no need to go further, but still need to register with CACHED_MATERIAL
    if biosample_id in BIOSAMPLES_RECORDS:
        CACHED_MATERIAL.setdefault(biosample_id, {})
        CACHED_MATERIAL[biosample_id]['accession'] = biosample_id
        CACHED_MATERIAL[biosample_id]['material'] = BIOSAMPLES_RECORDS[biosample_id]['material']
        CACHED_MATERIAL[biosample_id]['confirmed'] = True
        CACHED_MATERIAL[biosample_id]['source'] = 'ES records'
        return 0
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                     f'Try to get data for {biosample_id} from BioSamples', to_es_flag)
    url = f"https://www.ebi.ac.uk/biosamples/samples/{biosample_id}"
    response = requests.get(url)
    status = response.status_code
    # if not successful, return the status code and add to cache
    if status != 200:  # success
        tmp = {
            'accession': biosample_id,
            'confirmed': False,
            'source': 'error status'
        }
        CACHED_MATERIAL[biosample_id] = tmp
        return status

    default_material_value = {
        'text': 'specimen from organism',
        'ontologyTerms': 'http://purl.obolibrary.org/obo/OBI_0001479'
    }
    # successfully get the sample record
    data = response.json()

    material_key = get_field_name(data, 'Material', 'material')
    if material_key:
        CACHED_MATERIAL.setdefault(biosample_id, dict())
        CACHED_MATERIAL[biosample_id]['accession'] = biosample_id
        CACHED_MATERIAL[biosample_id].setdefault('material', dict())
        CACHED_MATERIAL[biosample_id]['material']['text'] = data['characteristics'][material_key][0]['text']
        CACHED_MATERIAL[biosample_id]['material']['ontologyTerms'] = \
            data['characteristics'][material_key][0]['ontologyTerms'][0]
        CACHED_MATERIAL[biosample_id]['confirmed'] = True
        CACHED_MATERIAL[biosample_id]['source'] = 'biosample server material key'

    parent_animal_list = list()
    if 'relationships' in data:
        relationships = data['relationships']
        for relationship in relationships:
            # child of relationship only exists in animals
            if relationship['type'] == 'child of':
                material_value = {
                    'text': 'organism',
                    'ontologyTerms': 'http://purl.obolibrary.org/obo/OBI_0100026'
                }
                tmp = {
                    'accession': biosample_id,
                    'confirmed': True,
                    'material': material_value,
                    'source': 'child of relationship'
                }
                CACHED_MATERIAL[biosample_id] = tmp
                if relationship['target'] != biosample_id:
                    parent_animal_list.append(relationship['target'])
            elif relationship['type'] == 'derived from':
                if relationship['target'] != biosample_id:
                    # try to get all records from the relationships
                    if relationship['target'] not in CACHED_MATERIAL:
                        retrieve_biosamples_record(es_index_prefix, relationship['target'])
                    related_material = CACHED_MATERIAL[relationship['target']]
                    if 'material' in related_material and related_material['confirmed'] \
                            and related_material['material']['text'] == 'organism':
                        if biosample_id in CACHED_MATERIAL and not CACHED_MATERIAL[biosample_id]['confirmed']:
                            material_value = {
                                'text': 'specimen from organism',
                                'ontologyTerms': 'http://purl.obolibrary.org/obo/OBI_0001479'
                            }
                            tmp = {
                                'accession': biosample_id,
                                'confirmed': True,
                                'material': material_value,
                                'source': 'derived from organism relationship'
                            }
                            CACHED_MATERIAL[biosample_id] = tmp
                        animal = relationship['target']

    if biosample_id not in CACHED_MATERIAL:
        CACHED_MATERIAL.setdefault(biosample_id, {})
        CACHED_MATERIAL[biosample_id]['accession'] = biosample_id
        CACHED_MATERIAL[biosample_id]['confirmed'] = False
        CACHED_MATERIAL[biosample_id]['material'] = default_material_value

    es_doc = dict()
    # sample record labelled as FAANG does not guarantee to be in ES as it may fail the validation,
    # but should not be dealt with here

    # this is a tag used in the ruleset, not a standard, just happening to share the same value
    # therefore not replacing with constant
    if 'project' in data['characteristics'] and data['characteristics']['project'][0]['text'] == 'FAANG':
        return -1
    es_doc['biosampleId'] = biosample_id
    es_doc['name'] = data['name']
    es_doc['material'] = CACHED_MATERIAL[biosample_id]['material']

    is_ebi_record = False
    # id_number is an artificial value which make it possible to sort BioSamples records using numeric order
    if biosample_id[0:5] == 'SAMEA':
        es_doc['id_number'] = biosample_id[5:]
        is_ebi_record = True
    else:  # non EBI BioSamples record, indicating by using - sign
        match = re.search(r'\d+', biosample_id)
        if match:
            es_doc['id_number'] = -int(match.group(0))
    # In the BioSample's code converting NCBI BioSample records into EBI ones,
    # XML <Description><Title>text</Title></Description> is saved in field "description title"
    desc_key = get_field_name(data, 'description', 'description title')
    if desc_key:
        es_doc['description'] = data['characteristics'][desc_key][0]['text']

    species_key = get_field_name(data, 'organism', 'Organism')

    """
    https://www.ncbi.nlm.nih.gov/biosample/docs/attributes/?format=xml
    NCBI uses attributes, like EBI uses characteristics, <Name>field name</Name>
    FAANG mandatory field  => NCBI attribute(s)      EBI
    organism part => tissue                organism part
    cell type => cell type                 cell type
    breed => breed                         breed
    sex => sex                             sex
    developmental stage => development stage     developmental stage
    """

    found_fields = set()

    if CACHED_MATERIAL[biosample_id]['material']['text'] == 'organism':
        es_type = 'organism'
        es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'sex', 'sex')
        es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'breed', 'breed')
        if species_key:
            es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'organism', species_key)
    else:  # specimen
        es_type = 'specimen'
        if is_ebi_record:
            if 'organism part' in data['characteristics']:
                es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'organismPart',
                                                          'organism part', 'specimenFromOrganism')
                es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'cellType', 'organism part')
            elif 'cell type' in data['characteristics']:
                es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'cellType', 'cell type')
            es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'developmentalStage',
                                                      'developmental stage', 'specimenFromOrganism')
        else:  # from NCBI or DDBJ
            if 'tissue' in data['characteristics']:
                es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'cellType', 'tissue')
                es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'organismPart',
                                                          'tissue', 'specimenFromOrganism')
            elif 'cell type' in data['characteristics']:
                es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'cellType', 'cell type')
            es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'developmentalStage',
                                                      'development stage', 'specimenFromOrganism')

        if species_key:
            es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'organism', species_key, 'organism')
        es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'sex', 'sex', 'organism')
        es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'breed', 'breed', 'organism')
        es_doc, found_fields = extract_field_info(data, es_doc, found_fields, 'breed', 'strain', 'organism')

    if 'release' in data:
        es_doc['releaseDate'] = parse_date(data['release'])
    else:
        es_doc['releaseDate'] = None

    if 'update' in data:
        es_doc['updateDate'] = parse_date(data['update'])
    else:
        es_doc['updateDate'] = None
    # as no project = FAANG (otherwise already dealt with by import biosample), the standard is always basic
    es_doc['standardMet'] = constants.STANDARD_BASIC
    # animal only gets assigned during dealing with the relationship derived from
    # https://stackoverflow.com/questions/1592565/determine-if-variable-is-defined-in-python
    try:
        # noinspection PyUnboundLocalVariable
        animal
    except NameError:
        pass
    else:
        es_doc['derivedFrom'] = animal

    if parent_animal_list:
        es_doc.setdefault('childOf', list(parent_animal_list))

    custom_fields = list()
    for field_name in data['characteristics'].keys():
        # these fields have already been dealt with above
        if field_name == desc_key or field_name == species_key \
                or field_name == material_key or field_name in found_fields:
            continue
        if type(data['characteristics'][field_name]) is list:
            to_parse = data['characteristics'][field_name][0]
        else:
            to_parse = data['characteristics'][field_name]
        one_custom_field = dict()
        one_custom_field['name'] = field_name
        if type(to_parse) is dict:
            if 'text' in to_parse:
                one_custom_field['value'] = str(to_parse['text'])
            elif 'value' in to_parse:
                one_custom_field['value'] = str(to_parse['value'])
            if 'unit' in to_parse:
                one_custom_field['unit'] = to_parse['unit']
            if 'ontologyTerms' in to_parse:
                one_custom_field['ontologyTerms'] = to_parse['ontologyTerms'][0]
        else:
            one_custom_field['value'] = to_parse
        custom_fields.append(one_custom_field)
    es_doc['customField'] = custom_fields

    # expected to fail validation (Legacy basic), so no need to carry out
    body = json.dumps(es_doc)
    insert_into_es(es, es_index_prefix, es_type, biosample_id, body)

    BIOSAMPLES_RECORDS[biosample_id] = es_doc
    return status


def extract_field_info(data, es_doc, found_fields, result_field_name, target_field_name, es_section=None):
    """
    extract data for particular field :param target_field_name from the BioSamples API record :param data
    and save into the document :param es_doc as field :param result_field_name while adding into found_fields
    :param data: BioSamples API records
    :param es_doc: Elastic search document
    :param found_fields: list of fields which have been found in the BioSamples record
    :param result_field_name: the field name to store the extract information in the es_doc
    :param target_field_name: the field name to be search in the BioSamples record
    :param es_section: indicate which section the field belongs to in the es_doc, if in the root, set to None
    :return:
    """
    if target_field_name in data['characteristics']:
        found_fields.add(target_field_name)
        if es_section:
            es_doc.setdefault(es_section, dict())
            es_doc[es_section].setdefault(result_field_name, dict())
            es_doc[es_section][result_field_name]['text'] = data['characteristics'][target_field_name][0]['text']
            if 'ontologyTerms' in data['characteristics'][target_field_name][0]:
                es_doc[es_section][result_field_name]['ontologyTerms'] = \
                    data['characteristics'][target_field_name][0]['ontologyTerms'][0]
        else:
            es_doc.setdefault(result_field_name, dict())
            es_doc[result_field_name]['text'] = data['characteristics'][target_field_name][0]['text']
            if 'ontologyTerms' in data['characteristics'][target_field_name][0]:
                es_doc[result_field_name]['ontologyTerms'] = \
                    data['characteristics'][target_field_name][0]['ontologyTerms'][0]
    return es_doc, found_fields


def get_field_name(data, original_key, alternative_value):
    """
    check which column name used in the BioSamples record, if both provided column names not found, return ''
    :param data: one BioSamples record in JSON format retrieved from API
    :param original_key: field name to be checked
    :param alternative_value: alternative field name to be checked
    :return: either original key or alternative value if found in the data, if not found return ''
    """
    # biosamples record always has characteristics section
    if original_key not in data['characteristics']:
        if alternative_value in data['characteristics']:
            return alternative_value
        else:
            return ''
    return original_key


@click.command()
@click.option(
    '--es_hosts',
    default=constants.STAGING_NODE1,
    help='Specify the Elastic Search server(s) (port could be included), e.g. wp-np3-e2:9200. '
         'If multiple servers are provided, please use ";" to separate them, e.g. "wp-np3-e2;wp-np3-e3"'
)
@click.option(
    '--es_index_prefix',
    default="",
    help='Specify the Elastic Search index prefix, e.g. '
         'faang_build_1_ then the indices will be faang_build_1_experiment etc.'
         'If not provided, then work on the aliases, e.g. experiment'
)
@click.option(
    '--to_es',
    default="true",
    help='Specify how to deal with the system log either writing to es or printing out. '
         'It only allows two values: true (to es) or false (print to the terminal)'
)
def main(es_hosts, es_index_prefix, to_es: str):
    """
    Main function that will import legacy data (not FAANG labelled) from ena
    :param es_hosts: elasticsearch hosts where the data import into
    :param es_index_prefix: the index prefix points to a particular version of data
    :param to_es: determine whether to output log to Elasticsearch (True) or terminal (False, printing)
    """
    global to_es_flag
    if to_es.lower() == 'false':
        to_es_flag = False
    elif to_es.lower() == 'true':
        pass
    else:
        print('to_es parameter can only accept value of true or false')
        exit(1)

    global es
    hosts = es_hosts.split(";")
    es = Elasticsearch(hosts)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Start importing ena legacy', to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Command line parameters', to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), f'Hosts: {str(hosts)}', to_es_flag)
    es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
    if es_index_prefix:
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), f'Index_prefix: {es_index_prefix}', to_es_flag)

    get_biosamples_records_from_es(hosts[0], es_index_prefix, 'organism')
    get_biosamples_records_from_es(hosts[0], es_index_prefix, 'specimen')
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                     f'There are {len(BIOSAMPLES_RECORDS)} sample records in the ES', to_es_flag)
    if not BIOSAMPLES_RECORDS:
        write_system_log(es, SCRIPT_NAME, 'error', get_line_number(),
                         'No biosamples data found in the given index, please run import_from_biosamles.py first',
                         to_es_flag)
        sys.exit(1)

    existing_faang_datasets: Set[str] = get_record_ids(hosts[0], es_index_prefix, 'dataset', only_faang=True)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                     f'There are {len(existing_faang_datasets)} FAANG datasets in the ES', to_es_flag)

    # strings used to build ENA API query
    field_str = ",".join(FIELD_LIST)
    species_str = ",".join(SPECIES_TAXONOMY_LIST)

    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Retrieving data from ENA', to_es_flag)
    # collect all data from ENA API and saved into local dict which has keys as study accession
    # and values as array of data related to the study
    todo: Dict[str, List[Dict]] = dict()
    for term in CATEGORIES.keys():
        category = CATEGORIES[term]
        if category not in ASSAY_TYPES_TO_BE_IMPORTED:
            continue
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                         f'term {term} in category {category}', to_es_flag)
        # f"https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0&" \
        #    f"query=library_strategy%3D%22{term}%22%20AND%20tax_eq({species_str})&fields={field_str}"
        # extra constraint based on species and library strategy
        # debug notes: category in records descending order RNA-Seq (32k), WGS,
        # miRNA-Seq (4k), and others (around 1k or less)
        optional_str = f"query=library_strategy%3D%22{term}%22%20AND%20tax_eq({species_str})"
        url = generate_ena_api_endpoint('read_run', 'ena', field_str, optional_str)
        response = requests.get(url)
        if response.status_code == 204:  # 204 is the status code for no content => the current term does not have match
            continue
        data = response.json()
        for hit in data:
            study_accession = hit['study_accession']
            if study_accession in existing_faang_datasets:  # already in the data portal
                continue
            # not replaced with constants.STANDARD_FAANG because they are separate concepts,
            # here is a tag used in the ruleset, not a standard
            if hit['project_name'] == 'FAANG':  # labelled as FAANG which is supposed to be deal with import_from _ena
                continue
            todo.setdefault(term, list())
            todo[term].append(hit)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                     'Finishing retrieving data from ENA', to_es_flag)

    indexed_files = dict()
    datasets = dict()
    experiments = dict()
    files_dict = dict()
    technology = dict()

    for category in todo.keys():
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                         f'{category} has {len(todo[category])} records', to_es_flag)
        assay_type = ASSAY_TYPES_TO_BE_IMPORTED[CATEGORIES[category]]
        experiment_target = EXPERIMENT_TARGETS[CATEGORIES[category]]
        technology[assay_type] = category

        count = dict()
        with_files = dict()
        for record in todo[category]:
            # if fail to get the related sample record, skip the record
            specimen_biosample_id = record['sample_accession']
            if len(specimen_biosample_id) < 5: # invalid sample accession, which should be SAM[M|D|E]
                continue
            status = retrieve_biosamples_record(es_index_prefix, specimen_biosample_id)
            if status != 0 and status != 200:
                continue

            study_accession = record['study_accession']
            count.setdefault(study_accession, 0)
            count[study_accession] += 1

            file_type, source_type = determine_file_and_source(record)

            if file_type == '':
                continue

            with_files.setdefault(study_accession, 0)
            with_files[study_accession] += 1
            if file_type == 'fastq':
                archive = 'ENA'
            elif file_type == 'cram_index':
                archive = 'ENA'
                file_type = 'submitted'
                source_type = 'ftp'
            else:
                archive = 'SRA'

            try:
                files = record[f"{file_type}_{source_type}"].split(";")
                types = record['submitted_format'].split(";")
                sizes = record[f"{file_type}_bytes"].split(";")
            except KeyError:
                print(f"category {category} record {record}")
                exit()

            if len(files) != len(sizes):
                continue
            # for ENA, it is fixed to MD5 as the checksum method
            checksums = record[f"{file_type}_md5"].split(";")
            # logger.info(f"study {study_accession} with sample {specimen_biosample_id} having files {','.join(files)}")

            for index, file in enumerate(files):
                fullname = file.split("/")[-1]
                filename = fullname.split(".")[0]
                type_found = True
                try:
                    file_type = types[index]
                    if len(file_type) == 0:
                        type_found = False
                except IndexError:
                    type_found = False
                if not type_found:
                    start_index = len(filename)+1
                    file_type = fullname[start_index:]

                es_file_doc = {
                    'specimen': specimen_biosample_id,
                    'species': {
                        'text': SPECIES_DICT[record['tax_id']],
                        'ontologyTerms': f"http://purl.obolibrary.org/obo/NCBITaxon_{record['tax_id']}"
                    },
                    'url': file,
                    'name': fullname,
                    'type': file_type,
                    'size': sizes[index],
                    'readableSize': convert_readable(sizes[index]),
                    'checksumMethod': 'md5',
                    'checksum': checksums[index],
                    'archive': archive,
                    'baseCount': record['base_count'],
                    'readCount': record['read_count'],
                    'releaseDate': record['first_public'],
                    'updateDate': record['last_updated'],
                    'submission': record['submission_accession'],
                    'experiment': {
                        'accession': record['experiment_accession'],
                        'assayType': assay_type,
                        'target': experiment_target
                    },
                    'run': {
                        'accession': record['run_accession'],
                        'alias': record['run_alias'],
                        'platform': record['instrument_platform'],
                        'instrument': record['instrument_model']
                    },
                    'study': {
                        'accession': record['study_accession'],
                        'alias': record['study_alias'],
                        'title': record['study_title'],
                        'type': category,
                        'secondaryAccession': record['secondary_study_accession']
                    }
                }
                files_dict[filename] = es_file_doc

                exp_id = record['experiment_accession']
                # one experiment could have multiple runs/files, therefore experiment info needs to be collected once
                if exp_id not in experiments:
                    exp_es = {
                        'accession': exp_id,
                        'assayType': assay_type,
                        'experimentTarget': experiment_target
                    }
                    experiments[exp_id] = exp_es

                # dataset (study) has mutliple experiments/runs/files/specimens_list
                # so collection information into datasets
                # and process it after iteration of all files
                dataset_id = record['study_accession']
                es_doc_dataset = dict()
                if dataset_id in datasets:
                    es_doc_dataset = datasets[dataset_id]
                else:
                    es_doc_dataset['accession'] = dataset_id
                    es_doc_dataset['alias'] = record['study_alias']
                    es_doc_dataset['title'] = record['study_title']
                    es_doc_dataset['secondaryAccession'] = record['secondary_study_accession']
                datasets.setdefault('tmp', dict())
                datasets['tmp'].setdefault(dataset_id, dict())
                datasets['tmp'][dataset_id].setdefault('specimen', set())
                datasets['tmp'][dataset_id]['specimen'].add(specimen_biosample_id)

                datasets['tmp'][dataset_id].setdefault('instrument', set())
                datasets['tmp'][dataset_id]['instrument'].add(record['instrument_model'])

                datasets['tmp'][dataset_id].setdefault('archive', set())
                datasets['tmp'][dataset_id]['archive'].add(archive)

                tmp_file = {
                    'url': file,
                    'name': fullname,
                    'fileId': filename,
                    'experiment': record['experiment_accession'],
                    'type': file_type,
                    'size': sizes[index],
                    'readableSize': convert_readable(sizes[index]),
                    'archive': archive,
                    'baseCount': record['base_count'],
                    'readCount': record['read_count'],
                    'checksumMethod': 'md5',
                    'checksum': checksums[index]
                }
                datasets['tmp'][dataset_id].setdefault('file', dict())
                datasets['tmp'][dataset_id]['file'][fullname] = tmp_file

                tmp_exp = {
                    'accession': record['experiment_accession'],
                    'assayType': assay_type,
                    'target': experiment_target
                }
                datasets['tmp'][dataset_id].setdefault('experiment', dict())
                datasets['tmp'][dataset_id]['experiment'][record['experiment_accession']] = tmp_exp
                datasets[dataset_id] = es_doc_dataset

    if not datasets:
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'No datasets have been found', to_es_flag)
        exit()

    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'The dataset list:', to_es_flag)
    dataset_ids = sorted(list(datasets.keys()))
    for index, dataset_id in enumerate(dataset_ids):
        num_exps = 0
        if dataset_id == 'tmp':
            continue
        if dataset_id in datasets['tmp'] and 'experiment' in datasets['tmp'][dataset_id]:
            num_exps = len(datasets['tmp'][dataset_id]["experiment"])
        printed_index = index + 1
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                         f'{printed_index} {dataset_id} has {num_exps} experiments to be processed', to_es_flag)
    # datasets contains one artificial value set with the key as 'tmp', so need to -1
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                     f'There are {len(datasets) -  1} datasets to be processed', to_es_flag)

    validator = validate_experiment_record.ValidateExperimentRecord(experiments, RULESETS)
    validation_results = validator.validate()
    exp_validation = dict()
    for exp_id in sorted(list(experiments.keys())):
        exp_es = experiments[exp_id]
        for ruleset in RULESETS:
            if validation_results[ruleset]['detail'][exp_id]['status'] == 'error':
                write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                                 f"{exp_id}\tExperiment\terror\t"
                                 f"{validation_results[ruleset]['detail'][exp_id]['message']}", to_es_flag)
            else:
                # only indexing when meeting standard
                exp_validation[exp_id] = STANDARDS[ruleset]
                exp_es['standardMet'] = STANDARDS[ruleset]
                body = json.dumps(exp_es)
                insert_into_es(es, es_index_prefix, 'experiment', exp_id, body)
                # index into ES so break the loop
                break
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'finishing indexing experiments', to_es_flag)

    for file_id in files_dict.keys():
        es_file_doc = files_dict[file_id]
        # noinspection PyTypeChecker
        exp_id = es_file_doc['experiment']['accession']
        # only files linked to valid experiments are allowed into the data portal
        if exp_id not in exp_validation:
            continue
        es_file_doc['experiment']['standardMet'] = exp_validation[exp_id]
        body = json.dumps(es_file_doc)
        insert_into_es(es, es_index_prefix, 'file', file_id, body)
        indexed_files[file_id] = 1
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'finishing indexing files', to_es_flag)

    for dataset_id in datasets:
        if dataset_id == 'tmp':
            continue
        es_doc_dataset = datasets[dataset_id]
        exps = datasets['tmp'][dataset_id]["experiment"]
        only_valid_exps = dict()
        dataset_standard = constants.STANDARD_FAANG
        experiment_type = dict()
        tech_type = dict()
        for exp_id in exps:
            if exp_id in exp_validation:
                if exp_validation[exp_id] == constants.STANDARD_LEGACY:
                    dataset_standard = constants.STANDARD_LEGACY
                only_valid_exps[exp_id] = exps[exp_id]
                assay_type = exps[exp_id]['assayType']
                tech_type.setdefault(TECHNOLOGIES[assay_type], 0)
                tech_type[TECHNOLOGIES[assay_type]] += 1
                experiment_type.setdefault(assay_type, 0)
                experiment_type[assay_type] += 1
            else:
                pass
        num_valid_exps = len(only_valid_exps.keys())
        if num_valid_exps == 0:
            write_system_log(es, SCRIPT_NAME, 'warning', get_line_number(),
                             f'dataset {dataset_id} has no valid experiments, skipped.', to_es_flag)
            continue
        es_doc_dataset['standardMet'] = dataset_standard
        specimen_set = datasets['tmp'][dataset_id]['specimen']
        species = dict()
        specimens_list = list()
        for specimen in specimen_set:
            if specimen not in BIOSAMPLES_RECORDS:
                write_system_log(es, SCRIPT_NAME, 'warning', get_line_number(),
                                 f'BioSamples record {specimen} required by dataset {dataset_id} could not be found',
                                 to_es_flag)
                continue
            specimen_detail = BIOSAMPLES_RECORDS[specimen]
            es_doc_specimen = dict()
            es_doc_specimen['biosampleId'] = check_existsence(specimen_detail, 'biosampleId')
            es_doc_specimen['material'] = check_existsence(specimen_detail, 'material')
            es_doc_specimen['cellType'] = check_existsence(specimen_detail, 'cellType')
            es_doc_specimen['organism'] = check_existsence(specimen_detail['organism'], 'organism')
            es_doc_specimen['sex'] = check_existsence(specimen_detail['organism'], 'sex')
            es_doc_specimen['breed'] = check_existsence(specimen_detail['organism'], 'breed')
            specimens_list.append(es_doc_specimen)
            species[specimen_detail['organism']['organism']['text']] = specimen_detail['organism']['organism']
        if not specimens_list:  # no sepcimen found for the dataset, skip
            write_system_log(es, SCRIPT_NAME, 'warning', get_line_number(),
                             f'Dataset {dataset_id} could not find any related specimen, skipped', to_es_flag)
            continue
        es_doc_dataset['specimen'] = sorted(specimens_list, key=lambda k: k['biosampleId'])
        es_doc_dataset['species'] = list(species.values())
        file_arr = datasets['tmp'][dataset_id]['file'].values()
        valid_files = list()
        for file_entry in sorted(file_arr, key=lambda k: k['name']):
            file_id = file_entry['fileId']
            if file_id in indexed_files:
                valid_files.append(file_entry)
        es_doc_dataset['file'] = valid_files
        es_doc_dataset['experiment'] = list(only_valid_exps.values())
        es_doc_dataset['assayType'] = list(experiment_type.keys())
        es_doc_dataset['tech'] = list(tech_type.keys())
        es_doc_dataset['instrument'] = list(datasets['tmp'][dataset_id]['instrument'])
        es_doc_dataset['archive'] = sorted(list(datasets['tmp'][dataset_id]['archive']))
        body = json.dumps(es_doc_dataset)
        insert_into_es(es, es_index_prefix, 'dataset', dataset_id, body)
    write_system_log(es, SCRIPT_NAME, 'warning', get_line_number(),
                     f'finishing indexing datasets', to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Finish importing ena legacy', to_es_flag)


if __name__ == "__main__":
    main()
