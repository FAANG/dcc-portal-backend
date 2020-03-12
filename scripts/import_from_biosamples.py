from elasticsearch import Elasticsearch
from datetime import datetime
from utils import remove_underscore_from_end_prefix, insert_into_es, insert_es_log, \
    write_system_log, get_line_number
from get_all_etags import fetch_biosample_ids
from columns import *
from misc import *
from typing import Dict
import validate_organism_record
import validate_specimen_record
import requests
import json
import sys
import click
import os
import os.path
import constants

INDEXED_SAMPLES = dict()
ORGANISM = dict()
SPECIMEN_FROM_ORGANISM = dict()
CELL_SPECIMEN = dict()
CELL_CULTURE = dict()
CELL_LINE = dict()
POOL_SPECIMEN = dict()
ORGANISM_FOR_SPECIMEN = dict()
SPECIMEN_ORGANISM_RELATIONSHIP = dict()
ORGANISM_REFERRED_BY_SPECIMEN = dict()
ALL_DERIVED_SPECIMEN = dict()
RULESETS = ["FAANG Samples", "FAANG Legacy Samples"]
TOTAL_RECORDS_TO_UPDATE = 0
ETAGS_CACHE = dict()
ERROR_ESSENTIAL_FILENAME = 'biosamples_without_essential_fields.txt'
known_missing_essential_records = set()
to_es_flag = True

MATERIAL_TYPES = {
    "organism": "OBI_0100026",
    "specimen from organism": "OBI_0001479",
    "cell specimen": "OBI_0001468",
    "pool of specimens": "OBI_0302716",
    "cell culture": "OBI_0001876",
    "cell line": "CLO_0000031"
}
ALL_MATERIAL_TYPES = dict()


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
         'faang_build_1_ then the indices will be faang_build_1_organism etc.'
         'If not provided, then work on the aliases'
)
@click.option(
    '--to_es',
    default="true",
    help='Specify how to deal with the system log either writing to es or printing out. '
         'It only allows two values: true (to es) or false (print to the terminal)'
)
# TODO check single or double quotes
def main(es_hosts, es_index_prefix, to_es: str):
    """
    Main function that will import data from biosamples
    :param es_hosts: elasticsearch hosts where the data import into
    :param es_index_prefix: the index prefix points to a particular version of data
    :return:
    """
    global ETAGS_CACHE
    global ALL_MATERIAL_TYPES
    global to_es_flag
    # initialize ES first as needed to do logging
    hosts = es_hosts.split(";")
    es = Elasticsearch(hosts)

    if to_es.lower() == 'false':
        to_es_flag = False
    elif to_es.lower() == 'true':
        pass
    else:
        print('to_es parameter can only accept value of true or false')
        exit(1)

    today = datetime.now().strftime('%Y-%m-%d')
    cache_filename = f"etag_list_{today}.txt"
    if not os.path.isfile(cache_filename):
        write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                         'Could not find today etag cache file. Generating', to_es_flag)
        os.system("python3 get_all_etags.py")
    try:
        with open(cache_filename, 'r') as f:
            for line in f:
                line = line.rstrip()
                data = line.split("\t")
                ETAGS_CACHE[data[0]] = data[1]
    except FileNotFoundError:
        write_system_log(es, 'import_biosamples', 'error', get_line_number(),
                         f'Could not find the local etag cache file etag_list_{today}.txt', to_es_flag)
        sys.exit(1)
    try:
        with open(ERROR_ESSENTIAL_FILENAME, 'r') as f:
            for line in f:
                line = line.rstrip()
                known_missing_essential_records.add(line)
    except FileNotFoundError:  # file does not exist means that no known records
        pass

    # Material type can only take 6 values e.g. organism etc
    # However it is encouraged to use more specific ontology term, e.g. primary cell culture preferred than cell culture
    # ALL_MATERAIL_TYPES will be populated with all possible allowed terms as keys
    # and corresponding base material type as values
    for base_material in MATERIAL_TYPES.keys():
        ALL_MATERIAL_TYPES[base_material] = base_material
        host = f"http://www.ebi.ac.uk/ols/api/terms?id={MATERIAL_TYPES[base_material]}"
        request = requests.get(host)

        response = request.json()
        num = response['page']['totalElements']
        detail = None
        if num:
            if num > 20:
                host = host + "&size=" + str(num)
                request = requests.get(host)
                response = request.json()
            terms = response['_embedded']['terms']
            for term in terms:
                if term['is_defining_ontology']:
                    detail = term
                    break
        host = f"http://www.ebi.ac.uk/ols/api/ontologies/{detail['ontology_name']}/children?" \
            f"id={MATERIAL_TYPES[base_material]}"
        response = requests.get(host).json()
        num = response['page']['totalElements']
        if num:
            if num > 20:
                host = host + "&size=" + str(num)
                request = requests.get(host)
                response = request.json()
            terms = response['_embedded']['terms']
            for term in terms:
                ALL_MATERIAL_TYPES[term['label']] = base_material

    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Command line parameters', to_es_flag)
    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Hosts: ' + str(hosts), to_es_flag)

    es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
    if es_index_prefix:
        write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                         f'Index_prefix: {es_index_prefix}', to_es_flag)

    ruleset_version = validate_organism_record.ValidateOrganismRecord.get_ruleset_version()
    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'The program starts', to_es_flag)
    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     f'Current ruleset version is {ruleset_version}', to_es_flag)
    etags_es: Dict[str, str] = get_existing_etags(hosts[0], es, es_index_prefix)

    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     f"There are {len(etags_es)} records with etags_es in ES", to_es_flag)
    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     'Finish retrieving existing etags_es', to_es_flag)
    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Importing FAANG data', to_es_flag)

    # when more than half BioSamples records not already stored in ES, take the batch import route
    # otherwise compare each record's etag to decide
    if len(etags_es) == 0 or len(fetch_biosample_ids())/len(etags_es) > 2:
        write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'By project route', to_es_flag)
        fetch_records_by_project(es, es_index_prefix)
    else:
        write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'By individual route', to_es_flag)
        fetch_records_by_project_via_etag(etags_es, es, es_index_prefix)

    if TOTAL_RECORDS_TO_UPDATE == 0:
        write_system_log(es, 'import_biosamples', 'critical', get_line_number(),
                         'Did not obtain any records which need to be updated from BioSamples', to_es_flag)
        sys.exit(0)

    # the order of importation could not be changed due to derive from
    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Indexing organism starts', to_es_flag)
    process_organisms(es, es_index_prefix)

    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     'Indexing specimen from organism starts', to_es_flag)
    process_specimens(es, es_index_prefix)

    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Indexing cell specimen starts', to_es_flag)
    process_cell_specimens(es, es_index_prefix)

    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Indexing cell culture starts', to_es_flag)
    process_cell_cultures(es, es_index_prefix)

    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Indexing pool of specimen starts', to_es_flag)
    process_pool_specimen(es, es_index_prefix)

    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Indexing cell line starts', to_es_flag)
    process_cell_lines(es, es_index_prefix)

    all_organism_list = list(ORGANISM.keys())
    organism_referred_list = list(ORGANISM_REFERRED_BY_SPECIMEN.keys())
    union = dict()
    for acc in all_organism_list:
        union.setdefault(acc, {})
        union[acc].setdefault('count', 0)
        union[acc].setdefault('source', [])
        union[acc]['count'] += 1
        union[acc]['source'].append('organism')
    for acc in organism_referred_list:
        union.setdefault(acc, {})
        union[acc].setdefault('count', 0)
        union[acc].setdefault('source', [])
        union[acc]['count'] += 1
        union[acc]['source'].append('specimen')
    for acc in union:
        if union[acc]['count'] == 1:
            write_system_log(es, 'import_biosamples', 'warning', get_line_number(),
                             f"{acc} only in source {union[acc]['source']}", to_es_flag)
    clean_elasticsearch(f'{es_index_prefix}_specimen', es)
    clean_elasticsearch(f'{es_index_prefix}_organism', es)
    write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Program ends', to_es_flag)


def get_existing_etags(host: str, es, es_index_prefix) -> Dict[str, str]:
    """
    Function gets etags from organisms and specimens in elastic search
    :return: list of etags
    """
    if not host.endswith(":9200"):
        host = host + ":9200"
    results = dict()
    for item in ("organism", "specimen"):
        url = f'http://{host}/{es_index_prefix}_{item}/_search?_source=biosampleId,etag&sort=biosampleId&size=100000'
        response = requests.get(url).json()
        try:
            for result in response['hits']['hits']:
                if 'etag' in result['_source']:
                    results[result['_source']['biosampleId']] = result['_source']['etag']
        except KeyError:
            write_system_log(es, 'import_biosamples', 'error', get_line_number(),
                             f'Failing to get hits from result {url}', to_es_flag)
            exit()
    return results


def fetch_records_by_project_via_etag(etags, es, es_index_prefix):
    global TOTAL_RECORDS_TO_UPDATE
    counts = dict()
    today = datetime.now().strftime('%Y-%m-%d')
    with open("etag_list_{}.txt".format(today), 'r') as f:
        for line in f:
            line = line.rstrip()
            data = line.split("\t")
            # etag in ES matches the live version, no change
            if data[0] in etags and etags[data[0]] and etags[data[0]] == data[1]:
                INDEXED_SAMPLES[data[0]] = 1
                continue
            else:
                single = unify_field_names(fetch_single_record(data[0]))
                single['etag'] = data[1]
                if not check_is_faang(single):
                    sample_type = determine_sample_type(single)
                    insert_es_log(es, es_index_prefix, sample_type, single['accession'], 'error', 'no project=FAANG')
                    continue
                material = single['characteristics']['Material'][0]['text']
                if material in ALL_MATERIAL_TYPES and material != ALL_MATERIAL_TYPES[material]:
                    material = ALL_MATERIAL_TYPES[material]
                    single['characteristics']['Material'][0]['text'] = material
                    single['characteristics']['Material'][0]['ontologyTerms'][0] = MATERIAL_TYPES[material]
                if material == 'organism':
                    ORGANISM[data[0]] = single
                    # this may seem to be duplicate, however necessary: any unrecognized material type will be stored
                    # in counts, but will not be loaded into ES and need to inform FAANG DCC
                    TOTAL_RECORDS_TO_UPDATE += 1
                elif material == 'specimen from organism':
                    SPECIMEN_FROM_ORGANISM[data[0]] = single
                    TOTAL_RECORDS_TO_UPDATE += 1
                elif material == 'cell specimen':
                    CELL_SPECIMEN[data[0]] = single
                    TOTAL_RECORDS_TO_UPDATE += 1
                elif material == 'cell culture':
                    CELL_CULTURE[data[0]] = single
                    TOTAL_RECORDS_TO_UPDATE += 1
                elif material == 'cell line':
                    CELL_LINE[data[0]] = single
                    TOTAL_RECORDS_TO_UPDATE += 1
                elif material == 'pool of specimens':
                    POOL_SPECIMEN[data[0]] = single
                    TOTAL_RECORDS_TO_UPDATE += 1
                else:
                    insert_es_log(es, es_index_prefix, 'sample', data[0], 'error',
                                  f'not recognized material type {material}')
                counts.setdefault(material, 0)
                counts[material] += 1
    if TOTAL_RECORDS_TO_UPDATE == 0:
        write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                         'All records have not been modified since last importation.', to_es_flag)
        write_system_log(es, 'import_biosamples', 'info', get_line_number(), 'Exit program', to_es_flag)
        if counts:
            write_system_log(es, 'import_biosamples', 'warning', get_line_number(),
                             f'Some records with wrong material type have been found: ({counts})', to_es_flag)
        sys.exit(0)
    for k, v in counts.items():
        write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                         f'There are {v} {k} records needing update', to_es_flag)

    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     f'The total number of records to be updated is {TOTAL_RECORDS_TO_UPDATE}', to_es_flag)
    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     f'Finish comparing etags and retrieving necessary records', to_es_flag)


def unify_field_names(biosample):
    """
    BioSamples started to unify the fields name to make sure 1) all lowercase and 2) no underscores
    all lowercase conflicts with FAANG ruleset, e.g. material in BioSamples and Material in FAANG ruleset
    this method provides an automatical way to convert to FAANG standard
    :param biosample: biosample record retrieved from BioSamples API
    :return: converted BioSamples record
    """
    mapped_fields = {
        "material": "Material",
        "sex": "Sex",
        "organism": "Organism"
    }
    for field_name, new_field_name in mapped_fields.items():
        if field_name in biosample['characteristics'] and new_field_name not in biosample['characteristics']:
            # remove field name by pop method which returns the corresponding value
            biosample['characteristics'][new_field_name] = biosample['characteristics'].pop(field_name, None)

    return biosample


def find_essential_fields(biosample: Dict) -> bool:
    """
    check whether the essential field(s) exist in the sample record
    currently, essential field list only includes Material
    :param biosample: sample record
    :return: true if all essential fields are found
    """
    essential_fields = ['Material']
    for essential in essential_fields:
        if essential not in biosample['characteristics']:
            return False
    return True


def fetch_records_by_project(es, es_index_prefix):
    """
    Get all FAANG-labelled sample records from BioSamples using the API
    :return:
    """
    global TOTAL_RECORDS_TO_UPDATE
    biosamples = list()
    counts = dict()

    url = 'https://www.ebi.ac.uk/biosamples/samples?size=1000&filter=attr%3Aproject%3AFAANG'
    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     f'Size of local etag cache: {str(len(ETAGS_CACHE))}', to_es_flag)
    while url:
        write_system_log(es, 'import_biosamples', 'info', get_line_number(), f'Fetching data from {url}', to_es_flag)
        response = requests.get(url).json()
        for biosample in response['_embedded']['samples']:
            if biosample['accession'] in known_missing_essential_records:
                continue
            biosample = unify_field_names(biosample)
            if find_essential_fields(biosample):
                biosample['etag'] = ETAGS_CACHE[biosample['accession']]
                biosamples.append(biosample)
            else:
                with open(ERROR_ESSENTIAL_FILENAME , 'a') as w:
                    w.write(f"{biosample['accession']}\n")
                    sample_type = determine_sample_type(biosample)
                    insert_es_log(es, es_index_prefix, sample_type, biosample['accession'], 'error',
                                  'missing essential fields')
                    # to activate cronjob email notification
                    print(f"{biosample['accession']} does not have essential fields\n")

        if 'next' in response['_links']:
            url = response['_links']['next']['href']
        else:
            url = ''

    for i, biosample in enumerate(biosamples):
        if not check_is_faang(biosample):
            sample_type = determine_sample_type(biosample)
            insert_es_log(es, es_index_prefix, sample_type, biosample['accession'], 'error', 'no project=FAANG')
            continue
        material = biosample['characteristics']['Material'][0]['text']
        if material in ALL_MATERIAL_TYPES and material != ALL_MATERIAL_TYPES[material]:
            material = ALL_MATERIAL_TYPES[material]
            biosample['characteristics']['Material'][0]['text'] = material
            biosample['characteristics']['Material'][0]['ontologyTerms'][0] = MATERIAL_TYPES[material]
        if material == 'organism':
            biosample = deal_with_decimal_degrees(biosample)
            ORGANISM[biosample['accession']] = biosample
        elif material == 'specimen from organism':
            SPECIMEN_FROM_ORGANISM[biosample['accession']] = biosample
        elif material == 'cell specimen':
            CELL_SPECIMEN[biosample['accession']] = biosample
        elif material == 'cell culture':
            CELL_CULTURE[biosample['accession']] = biosample
        elif material == 'cell line':
            CELL_LINE[biosample['accession']] = biosample
        elif material == 'pool of specimens':
            POOL_SPECIMEN[biosample['accession']] = biosample
        counts.setdefault(material, 0)
        counts[material] += 1
    for k, v in counts.items():
        TOTAL_RECORDS_TO_UPDATE += v
        write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                         f'There are {v} {k} records needing update', to_es_flag)
        # logger.info(f"There are {v} {k} records needing update")

    write_system_log(es, 'import_biosamples', 'info', get_line_number(),
                     f'The total number of records to be updated is {TOTAL_RECORDS_TO_UPDATE}', to_es_flag)
    # logger.info(f"The sum is {TOTAL_RECORDS_TO_UPDATE}")


def determine_sample_type(biosample):
    """
    Determine the sample type, organism, specimen or sample (no value provided)
    currently only Material is treated as essential fields, i.e. sample type expected to be 'sample'
    :param biosample: sample record
    :return: sample type
    """
    sample_type = 'sample'
    if 'Material' in biosample['characteristics']:
        material = biosample['characteristics']['Material'][0]['text']
        if material in ALL_MATERIAL_TYPES and material != ALL_MATERIAL_TYPES[material]:
            material = ALL_MATERIAL_TYPES[material]
        sample_type = material
        if sample_type != 'organism':
            sample_type = 'specimen'
    return sample_type


def fetch_single_record(biosample_id):
    """
    Function returns json file of single record from biosamples
    :param biosample_id: accession id or record to return
    :return: json file of sample with biosampleId
    """
    url = f"https://www.ebi.ac.uk/biosamples/samples/{biosample_id}.json?curationdomain=self.FAANG_DCC_curation"
    result = unify_field_names(requests.get(url).json())
    result['etag'] = ETAGS_CACHE[biosample_id]
    return result


def check_is_faang(item):
    """
    Function checks that record belongs to FAANG project
    :param item: item to check
    :return: True if item has FAANG project label and False otherwise
    """
    if 'characteristics' in item and 'project' in item['characteristics']:
        for project in item['characteristics']['project']:
            if 'text' in project and project['text'].lower() == 'faang':
                return True
    return False


def deal_with_decimal_degrees(item):
    """
    BioSamples auto curation modify the valid unit in FAANG of 'decimal degrees' to 'decimal degree'
    Therefore if the unit is decimal degree, only use the curation provided by the FAANG DCC team
    otherwise just return the same record
    :param item:
    :return:
    """
    if item['characteristics']['Material'][0]['text'] == 'organism':
        try:
            if item['characteristics']['birth location latitude'][0]['unit'] == 'decimal degree' or \
                    item['characteristics']['birth location longitude'][0]['unit'] == 'decimal degree':
                url = "https://www.ebi.ac.uk/biosamples/samples/{}.json?curationdomain=self.FAANG_DCC_curation".format(
                    item['accession'])
                biosample = requests.get(url).json()
                biosample['etag'] = ETAGS_CACHE[biosample['accession']]
                return biosample
            else:
                return item
        except KeyError:
            return item
    else:
        return item


# all functions beginning with "process_" need to refer to the ruleset at
# https://github.com/FAANG/faang-metadata/blob/master/rulesets/faang_samples.metadata_rules.json
def process_organisms(es, es_index_prefix) -> None:
    """
    Process the data for all organisms according to ruleset
    which extracts the data and insert into elasticsearch
    """
    converted = dict()
    for accession, item in ORGANISM.items():
        doc_for_update = dict()
        doc_for_update['organism'] = {
            "text": check_existence(item, 'Organism', 'text'),
            "ontologyTerms": check_existence(item, 'Organism', 'ontologyTerms')
        }
        doc_for_update['sex'] = {
            "text": check_existence(item, 'Sex', 'text'),
            "ontologyTerms": check_existence(item, 'Sex', 'ontologyTerms')
        }
        doc_for_update['birthDate'] = {
            "text": check_existence(item, 'birth date', 'text'),
            "unit": check_existence(item, 'birth date', 'unit')
        }
        doc_for_update['breed'] = {
            "text": check_existence(item, 'breed', 'text'),
            "ontologyTerms": check_existence(item, 'breed', 'ontologyTerms')
        }
        doc_for_update['birthLocation'] = check_existence(item, 'birth location', 'text')
        doc_for_update['birthLocationLongitude'] = {
            "text": check_existence(item, 'birth location longitude', 'text'),
            "unit": check_existence(item, 'birth location longitude', 'unit')
        }
        doc_for_update['birthLocationLatitude'] = {
            "text": check_existence(item, 'birth location latitude', 'text'),
            "unit": check_existence(item, 'birth location latitude', 'unit')
        }
        doc_for_update['birthWeight'] = {
            "text": check_existence(item, 'birth weight', 'text'),
            "unit": check_existence(item, 'birth weight', 'unit')
        }
        doc_for_update['placentalWeight'] = {
            "text": check_existence(item, 'placental weight', 'text'),
            "unit": check_existence(item, 'placental weight', 'unit')
        }
        doc_for_update['pregnancyLength'] = {
            "text": check_existence(item, 'pregnancy length', 'text'),
            "unit": check_existence(item, 'pregnancy length', 'unit')
        }
        doc_for_update['deliveryTiming'] = check_existence(item, 'delivery timing', 'text')
        doc_for_update['deliveryEase'] = check_existence(item, 'delivery ease', 'text')
        doc_for_update['pedigree'] = check_existence(item, 'pedigree', 'text')
        doc_for_update = populate_basic_biosample_info(doc_for_update, item)
        doc_for_update = extract_custom_field(doc_for_update, item, 'organism')
        doc_for_update['healthStatus'] = get_health_status(item)
        relationships: Dict = parse_relationship(item)
        if 'childOf' in relationships:
            # after python 3.3.1 dict.keys() return dict_keys rather than list, so wrapped with list() function
            doc_for_update['childOf'] = list(relationships['childOf'].keys())
        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        add_organism_info_for_specimen(accession, item)
        if 'strain' in item['characteristics']:
            doc_for_update['breed'] = {
                'text': check_existence(item, 'strain', 'text'),
                'ontologyTerms': check_existence(item, 'strain', 'ontologyTerms')
            }
            ORGANISM_FOR_SPECIMEN[accession]['breed'] = {
                'text': check_existence(item, 'strain', 'text'),
                'ontologyTerms': check_existence(item, 'strain', 'ontologyTerms')
            }

        converted[accession] = doc_for_update
    import_into_es(converted, es_index_prefix, 'organism', es)


def process_specimens(es, es_index_prefix) -> None:
    """
    Process the data for all specimen from organism record
    :param es: Elasticsearch object
    :param es_index_prefix: the index prefix (build version)
    """
    converted = dict()
    for accession, item in SPECIMEN_FROM_ORGANISM.items():
        doc_for_update = dict()
        relationships = parse_relationship(item)
        url = check_existence(item, 'specimen collection protocol', 'text')
        organism_accession = None
        filename = get_filename_from_url(url, accession)
        if 'derivedFrom' in relationships:
            organism_accession = list(relationships['derivedFrom'].keys())[0]
        SPECIMEN_ORGANISM_RELATIONSHIP[accession] = organism_accession
        doc_for_update['derivedFrom'] = organism_accession
        doc_for_update.setdefault('specimenFromOrganism', {})
        doc_for_update['specimenFromOrganism']['specimenCollectionDate'] = {
            'text': check_existence(item, 'specimen collection date', 'text'),
            'unit': check_existence(item, 'specimen collection date', 'unit')
        }
        doc_for_update['specimenFromOrganism']['animalAgeAtCollection'] = {
            'text': check_existence(item, 'animal age at collection', 'text'),
            'unit': check_existence(item, 'animal age at collection', 'unit')
        }
        doc_for_update['specimenFromOrganism']['developmentalStage'] = {
            'text': check_existence(item, 'developmental stage', 'text'),
            'ontologyTerms': check_existence(item, 'developmental stage', 'ontologyTerms')
        }
        doc_for_update['specimenFromOrganism']['organismPart'] = {
            'text': check_existence(item, 'organism part', 'text'),
            'ontologyTerms': check_existence(item, 'organism part', 'ontologyTerms')
        }
        doc_for_update['specimenFromOrganism']['specimenCollectionProtocol'] = {
            'url': url,
            'filename': filename
        }
        doc_for_update['specimenFromOrganism']['fastedStatus'] = check_existence(item, 'fasted status', 'text')
        doc_for_update['specimenFromOrganism']['numberOfPieces'] = {
            'text': check_existence(item, 'number of pieces', 'text'),
            'unit': check_existence(item, 'number of pieces', 'unit')
        }
        doc_for_update['specimenFromOrganism']['specimenVolume'] = {
            'text': check_existence(item, 'specimen volume', 'text'),
            'unit': check_existence(item, 'specimen volume', 'unit')
        }
        doc_for_update['specimenFromOrganism']['specimenSize'] = {
            'text': check_existence(item, 'specimen size', 'text'),
            'unit': check_existence(item, 'specimen size', 'unit')
        }
        doc_for_update['specimenFromOrganism']['specimenWeight'] = {
            'text': check_existence(item, 'specimen weight', 'text'),
            'unit': check_existence(item, 'specimen weight', 'unit')
        }
        doc_for_update['specimenFromOrganism']['gestationalAgeAtSampleCollection'] = {
            'text': check_existence(item, 'gestational age at sample collection', 'text'),
            'unit': check_existence(item, 'gestational age at sample collection', 'unit')
        }
        doc_for_update = populate_basic_biosample_info(doc_for_update, item)
        doc_for_update = extract_custom_field(doc_for_update, item, 'specimen from organism')
        # cellType is an artificial field, which is displayed in the specimen list page
        # different specimen type extracted from different resources
        doc_for_update['cellType'] = {
            'text': check_existence(item, 'organism part', 'text'),
            'ontologyTerms': check_existence(item, 'organism part', 'ontologyTerms')
        }
        doc_for_update['specimenFromOrganism'].setdefault('specimenPictureUrl', [])
        if 'specimen picture url' in item['characteristics']:
            for picture_url in item['characteristics']['specimen picture url']:
                doc_for_update['specimenFromOrganism']['specimenPictureUrl'].append(picture_url['text'])
        doc_for_update['specimenFromOrganism'].setdefault('healthStatusAtCollection', [])
        if 'health status at collection' in item['characteristics']:
            # TODO to Alexey:
            #  check with get_health_status function, no need to have codes in both places which could be confusing
            for health_status in item['characteristics']['health status at collection']:
                doc_for_update['specimenFromOrganism']['healthStatusAtCollection'].append(
                    {
                        'text': health_status['text'],
                        'ontologyTerms': health_status['ontologyTerms'][0]
                    }
                )

        successful = add_organism(es, es_index_prefix, accession, organism_accession)
        if not successful:
            continue

        doc_for_update['organism'] = ORGANISM_FOR_SPECIMEN[organism_accession]
        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
        ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
        converted[accession] = doc_for_update
    import_into_es(converted, es_index_prefix, 'specimen', es)


def add_organism(es, es_index_prefix, specimen_accession, organism_accession):
    try:
        if organism_accession not in ORGANISM_FOR_SPECIMEN:
            add_organism_info_for_specimen(organism_accession, fetch_single_record(organism_accession))
    except:
        insert_es_log(es,es_index_prefix, 'specimen', specimen_accession, 'error',
                      f"No animal information for given organism accession {organism_accession}")
        print(f"Encounter error when trying to retrieve animal information by accesion {organism_accession} "
              f"for specimen {specimen_accession}")
        return False
    return True


def process_cell_specimens(es, es_index_prefix) -> None:
    """
    Process the data for all cell specimen records
    :param es: Elasticsearch object
    :param es_index_prefix: the index prefix (build version)
    """
    converted = dict()
    for accession, item in CELL_SPECIMEN.items():
        doc_for_update = dict()
        relatioships = parse_relationship(item)
        url = check_existence(item, 'purification protocol', 'text')
        filename = get_filename_from_url(url, accession)
        specimen_from_organism_accession = list(relatioships['derivedFrom'].keys())[0]
        organism_accession = ''
        if specimen_from_organism_accession in SPECIMEN_ORGANISM_RELATIONSHIP:
            organism_accession = SPECIMEN_ORGANISM_RELATIONSHIP[specimen_from_organism_accession]
        else:
            tmp = parse_relationship(fetch_single_record(specimen_from_organism_accession))
            if 'derivedFrom' in tmp:
                organism_accession = list(tmp['derivedFrom'].keys())[0]
        SPECIMEN_ORGANISM_RELATIONSHIP[accession] = organism_accession
        doc_for_update['derivedFrom'] = specimen_from_organism_accession
        # cell specimen can only derive from specimen from organism
        doc_for_update['allDeriveFromSpecimens'] = specimen_from_organism_accession
        ALL_DERIVED_SPECIMEN[accession] = list()
        ALL_DERIVED_SPECIMEN[accession].append(specimen_from_organism_accession)
        doc_for_update.setdefault('cellSpecimen', {})
        doc_for_update['cellSpecimen']['markers'] = check_existence(item, 'markers', 'text')
        doc_for_update['cellSpecimen']['purificationProtocol'] = {
            'url': url,
            'filename': filename
        }
        doc_for_update = populate_basic_biosample_info(doc_for_update, item)
        doc_for_update = extract_custom_field(doc_for_update, item, 'cell specimen')
        doc_for_update['cellType'] = {
            'text': check_existence(item, 'cell type', 'text'),
            'ontologyTerms': check_existence(item, 'cell type', 'ontologyTerms')
        }
        doc_for_update['cellSpecimen'].setdefault('cellType', [])
        if 'cell type' in item['characteristics']:
            for cell_type in item['characteristics']['cell type']:
                doc_for_update['cellSpecimen']['cellType'].append(cell_type)
        doc_for_update['alternativeId'] = get_alternative_id(relatioships)
        successful = add_organism(es, es_index_prefix, accession, organism_accession)
        if not successful:
            continue
        doc_for_update['organism'] = ORGANISM_FOR_SPECIMEN[organism_accession]
        ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
        ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
        converted[accession] = doc_for_update
    import_into_es(converted, es_index_prefix, 'specimen', es)


def process_cell_cultures(es, es_index_prefix) -> None:
    """
    Process the data for all cell culture records
    :param es: Elasticsearch object
    :param es_index_prefix: the index prefix (build version)
    """
    converted = dict()
    for accession, item in CELL_CULTURE.items():
        doc_for_update = dict()
        relationships = parse_relationship(item)
        url = check_existence(item, 'cell culture protocol', 'text')
        filename = get_filename_from_url(url, accession)
        derived_from_accession = list(relationships['derivedFrom'].keys())[0]
        organism_accession = ''
        ALL_DERIVED_SPECIMEN[accession] = list()
        # derived_from_accession is a specimen (not sure about type) already been processed
        if derived_from_accession in SPECIMEN_ORGANISM_RELATIONSHIP:
            organism_accession = SPECIMEN_ORGANISM_RELATIONSHIP[derived_from_accession]
            tmp_set = set()
            if derived_from_accession in ALL_DERIVED_SPECIMEN:
                tmp_set = set(ALL_DERIVED_SPECIMEN[derived_from_accession])
            tmp_set.add(derived_from_accession)
            ALL_DERIVED_SPECIMEN[accession] = list(tmp_set)
        else:
            tmp = parse_relationship(fetch_single_record(derived_from_accession))
            tmp_set = set()
            tmp_set.add(derived_from_accession)
            if 'derivedFrom' in tmp:
                organism_accession = list(tmp['derivedFrom'].keys())[0]
                candidate = fetch_single_record(organism_accession)
                if candidate['characteristics']['Material'][0]['text'] == 'specimen from organism':
                    tmp_set.add(organism_accession)
                    tmp2 = parse_relationship(candidate)
                    organism_accession = list(tmp2['derivedFrom'].keys())[0]
            ALL_DERIVED_SPECIMEN[accession] = list(tmp_set)
        doc_for_update['allDeriveFromSpecimens'] = ALL_DERIVED_SPECIMEN[accession]

        SPECIMEN_ORGANISM_RELATIONSHIP[accession] = organism_accession
        doc_for_update['derivedFrom'] = derived_from_accession
        doc_for_update.setdefault('cellCulture', {})
        doc_for_update['cellCulture']['cultureType'] = {
            'text': check_existence(item, 'culture type', 'text'),
            'ontologyTerms': check_existence(item, 'culture type', 'ontologyTerms')
        }
        doc_for_update['cellCulture']['cellType'] = {
            'text': check_existence(item, 'cell type', 'text'),
            'ontologyTerms': check_existence(item, 'cell type', 'ontologyTerms')
        }
        doc_for_update['cellCulture']['cellCultureProtocol'] = {
            'url': url,
            'filename': filename
        }
        doc_for_update['cellCulture']['cultureConditions'] = check_existence(item, 'culture conditions', 'text')
        doc_for_update['cellCulture']['numberOfPassages'] = check_existence(item, 'number of passages', 'text')
        doc_for_update = populate_basic_biosample_info(doc_for_update, item)
        doc_for_update = extract_custom_field(doc_for_update, item, 'cell culture')
        doc_for_update['cellType'] = {
            'text': check_existence(item, 'cell type', 'text'),
            'ontologyTerms': check_existence(item, 'cell type', 'ontologyTerms')
        }
        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        successful = add_organism(es, es_index_prefix, accession, organism_accession)
        if not successful:
            continue
        doc_for_update['organism'] = ORGANISM_FOR_SPECIMEN[organism_accession]
        ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
        ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
        converted[accession] = doc_for_update
    import_into_es(converted, es_index_prefix, 'specimen', es)


def process_pool_specimen(es, es_index_prefix) -> None:
    """
    Process the data for all pool of specimen records
    :param es: Elasticsearch object
    :param es_index_prefix: the index prefix (build version)
    """
    global SPECIMEN_FROM_ORGANISM
    global SPECIMEN_ORGANISM_RELATIONSHIP
    converted = dict()
    for accession, item in POOL_SPECIMEN.items():
        doc_for_update = dict()
        relationships = parse_relationship(item)
        url = check_existence(item, 'pool creation protocol', 'text')
        filename = get_filename_from_url(url, accession)
        # noinspection PyTypeChecker
        doc_for_update.setdefault('poolOfSpecimens', {})
        # noinspection PyTypeChecker
        doc_for_update['poolOfSpecimens']['poolCreationDate'] = {
            'text': check_existence(item, 'pool creation date', 'text'),
            'unit': check_existence(item, 'pool creation date', 'unit')
        }
        # noinspection PyTypeChecker
        doc_for_update['poolOfSpecimens']['poolCreationProtocol'] = {
            'url': url,
            'filename': filename
        }
        # noinspection PyTypeChecker
        doc_for_update['poolOfSpecimens']['specimenVolume'] = {
            'text': check_existence(item, 'specimen volume', 'text'),
            'unit': check_existence(item, 'specimen volume', 'unit')
        }
        # noinspection PyTypeChecker
        doc_for_update['poolOfSpecimens']['specimenSize'] = {
            'text': check_existence(item, 'specimen size', 'text'),
            'unit': check_existence(item, 'specimen size', 'unit')
        }
        # noinspection PyTypeChecker
        doc_for_update['poolOfSpecimens']['specimenWeight'] = {
            'text': check_existence(item, 'specimen weight', 'text'),
            'unit': check_existence(item, 'specimen weight', 'unit')
        }
        doc_for_update = populate_basic_biosample_info(doc_for_update, item)
        doc_for_update = extract_custom_field(doc_for_update, item, 'pool of specimens')
        doc_for_update.setdefault('cellType', {})
        doc_for_update['cellType']['text'] = 'Not Applicable'
        doc_for_update['poolOfSpecimens'].setdefault('specimenPictureUrl', [])
        if 'specimen picture url' in item['characteristics']:
            for spu in item['characteristics']['specimen picture url']:
                doc_for_update['poolOfSpecimens']['specimenPictureUrl'].append(spu['text'])
        tmp = dict()
        if 'derivedFrom' in relationships:
            derived_from = list(relationships['derivedFrom'].keys())
            doc_for_update['derivedFrom'] = derived_from
            doc_for_update['allDeriveFromSpecimens'] = derived_from

            for acc in derived_from:
                if acc not in SPECIMEN_FROM_ORGANISM:
                    tmp_specimen = fetch_single_record(acc)
                    SPECIMEN_FROM_ORGANISM[acc] = tmp_specimen
                if acc not in SPECIMEN_ORGANISM_RELATIONSHIP:
                    item = SPECIMEN_FROM_ORGANISM[acc]
                    relationships = parse_relationship(item)
                    if 'derivedFrom' in relationships:
                        organism_accession = list(relationships['derivedFrom'].keys())[0]
                        SPECIMEN_ORGANISM_RELATIONSHIP[acc] = organism_accession
                organism_accession = SPECIMEN_ORGANISM_RELATIONSHIP[acc]
                ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
                ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
                if organism_accession not in ORGANISM_FOR_SPECIMEN:
                    add_organism_info_for_specimen(organism_accession, fetch_single_record(organism_accession))
                tmp['organism'] = {
                    organism_accession: {
                        'organism': {
                            'text': ORGANISM_FOR_SPECIMEN[organism_accession]['organism']['text'],
                            'ontologyTerms': ORGANISM_FOR_SPECIMEN[organism_accession]['organism']['ontologyTerms']
                        }
                    }
                }
                tmp['sex'] = {
                    organism_accession: {
                        'sex': {
                            'text': ORGANISM_FOR_SPECIMEN[organism_accession]['sex']['text'],
                            'ontologyTerms': ORGANISM_FOR_SPECIMEN[organism_accession]['sex']['ontologyTerms']
                        }
                    }
                }
                tmp['breed'] = {
                    organism_accession: {
                        'breed': {
                            'text': ORGANISM_FOR_SPECIMEN[organism_accession]['breed']['text'],
                            'ontologyTerms': ORGANISM_FOR_SPECIMEN[organism_accession]['breed']['ontologyTerms']
                        }
                    }
                }

        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        doc_for_update.setdefault('organism', {})
        for field_name in ['organism', 'sex', 'breed']:
            values = list(tmp[field_name].keys())
            if len(values) == 1:
                doc_for_update['organism'].setdefault(field_name, {})
                doc_for_update['organism'][field_name]['text'] = tmp[field_name][values[0]][field_name]['text']
                doc_for_update['organism'][field_name]['ontologyTerms'] = \
                    tmp[field_name][values[0]][field_name]['ontologyTerms']
            else:
                doc_for_update['organism'].setdefault(field_name, {})
                doc_for_update['organism'][field_name]['text'] = ";".join(values)
        converted[accession] = doc_for_update
    import_into_es(converted, es_index_prefix, 'specimen', es)


def process_cell_lines(es, es_index_prefix) -> None:
    """
    Process the data for all cell line records
    :param es: Elasticsearch object
    :param es_index_prefix: the index prefix (build version)
    """
    converted = dict()
    for accession, item in CELL_LINE.items():
        doc_for_update = dict()
        relationships = parse_relationship(item)
        url = check_existence(item, 'culture protocol', 'text')
        if url:
            filename = get_filename_from_url(url, accession)
        else:
            # TODO To Alexey: why not just get_filename_from_url as others
            filename = None
        doc_for_update.setdefault('cellLine', {})
        doc_for_update['cellLine']['organism'] = {
            'text': check_existence(item, 'Organism', 'text'),
            'ontologyTerms': check_existence(item, 'Organism', 'ontologyTerms')
        }
        doc_for_update['cellLine']['sex'] = {
            'text': check_existence(item, 'Sex', 'text'),
            'ontologyTerms': check_existence(item, 'Sex', 'ontologyTerms')
        }
        doc_for_update['cellLine']['cellLine'] = check_existence(item, 'cell line', 'text')
        doc_for_update['cellLine']['biomaterialProvider'] = check_existence(item, 'biomaterial provider', 'text')
        doc_for_update['cellLine']['catalogueNumber'] = check_existence(item, 'catalogue number', 'text')
        doc_for_update['cellLine']['numberOfPassages'] = check_existence(item, 'number of passages', 'text')
        doc_for_update['cellLine']['dateEstablished'] = {
            'text': check_existence(item, 'date established', 'text'),
            'unit': check_existence(item, 'date established', 'unit')
        }
        doc_for_update['cellLine']['publication'] = check_existence(item, 'publication', 'text')
        doc_for_update['cellLine']['breed'] = {
            'text': check_existence(item, 'breed', 'text'),
            'ontologyTerms': check_existence(item, 'breed', 'ontologyTerms')
        }
        doc_for_update['cellLine']['cellType'] = {
            'text': check_existence(item, 'cell type', 'text'),
            'ontologyTerms': check_existence(item, 'cell type', 'ontologyTerms')
        }
        doc_for_update['cellLine']['cultureConditions'] = check_existence(item, 'culture conditions', 'text')
        doc_for_update['cellLine']['cultureProtocol'] = {
            'url': url,
            'filename': filename
        }
        doc_for_update['cellLine']['disease'] = {
            'text': check_existence(item, 'disease', 'text'),
            'ontologyTerms': check_existence(item, 'disease', 'ontologyTerms')
        }
        doc_for_update['cellLine']['karyotype'] = check_existence(item, 'karyotype', 'text')
        doc_for_update = populate_basic_biosample_info(doc_for_update, item)
        doc_for_update = extract_custom_field(doc_for_update, item, 'cell line')
        doc_for_update['cellType'] = {
            'text': check_existence(item, 'cell type', 'text'),
            'ontologyTerms': check_existence(item, 'cell type', 'ontologyTerms')
        }
        tmp_set = set()
        if 'derivedFrom' in relationships:

            derive_from_accession = relationships['derivedFrom'][0]
            doc_for_update['derivedFrom'] = derive_from_accession
            candidate = fetch_single_record(derive_from_accession)
            if candidate['characteristics']['Material'][0]['text'] != 'organism':
                if derive_from_accession in ALL_DERIVED_SPECIMEN:
                    tmp_set = set(ALL_DERIVED_SPECIMEN[derive_from_accession])
                tmp_set.add(derive_from_accession)
        ALL_DERIVED_SPECIMEN[accession] = list(tmp_set)
        doc_for_update['allDeriveFromSpecimens'] = ALL_DERIVED_SPECIMEN[accession]

        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        doc_for_update.setdefault('organism', {})
        for field_name in ['organism', 'sex', 'breed']:
            doc_for_update['organism'][field_name] = doc_for_update['cellLine'][field_name]
        converted[accession] = doc_for_update
    import_into_es(converted, es_index_prefix, 'specimen', es)


def check_existence(item, field_name, subfield):
    """
    Check that item has particular field_name and subfield
    :param item: item to check
    :param field_name: main field_name
    :param subfield: subfield of field_name
    :return: return value of this field if it exists and None otherwise
    """
    try:
        if subfield == 'text':
            return item['characteristics'][field_name][0]['text']
        elif subfield == 'unit':
            return item['characteristics'][field_name][0]['unit']
        elif subfield == 'ontologyTerms':
            return item['characteristics'][field_name][0]['ontologyTerms'][0]
    except KeyError:
        return None


def populate_basic_biosample_info(doc: Dict, item: Dict):
    """
    This function add common field to document, applies to both animal and samples
    :param doc: ES document to update with common fields
    :param item: source of information
    :return: updated ES document
    """
    doc['name'] = item['name']
    doc['biosampleId'] = item['accession']
    doc['etag'] = item['etag']
    doc['id_number'] = item['accession'][5:]  # remove SAMEA
    doc['description'] = check_existence(item, 'description', 'text')
    doc['releaseDate'] = parse_date(item['release'])
    doc['updateDate'] = parse_date(item['update'])
    doc['material'] = {
        "text": check_existence(item, 'Material', 'text'),
        "ontologyTerms": check_existence(item, 'Material', 'ontologyTerms')
    }
    doc['project'] = check_existence(item, 'project', 'text')
    doc['secondaryProject'] = check_existence(item, 'secondary project', 'text')
    doc['availability'] = check_existence(item, 'availability', 'text')
    for organization in item['organization']:
        # TODO logging to error if name or role or url do not exist
        organization.setdefault('Name', None)
        organization.setdefault('Role', None)
        organization.setdefault('URL', None)
        doc.setdefault('organization', [])
        doc['organization'].append(
            {
                'name': organization['Name'],
                'role': organization['Role'],
                'URL': organization['URL']
            }
        )
    return doc


def extract_custom_field(doc, item, material_type):
    """
    This function adds custom fields to document from item
    :param doc: document to update
    :param item: source of information
    :param material_type: type of document
    :return: updated document
    """
    characteristics = item['characteristics'].copy()
    if material_type not in known_columns:
        # to invoke cron job notification
        print(f"Please update known_columns constants in columns.py for missing material type {material_type}")
        sys.exit(0)
    for column in common_columns + known_columns[material_type]:
        if column in characteristics:
            characteristics.pop(column)
    customs = list()
    for k, v in characteristics.items():
        # TODO allow list rather than always first element
        if isinstance(v, list):
            to_parse = v[0]
        else:
            to_parse = v
        tmp = dict()
        tmp['name'] = k
        if isinstance(to_parse, dict):
            if 'text' in to_parse:
                tmp['value'] = to_parse['text']
            if 'value' in to_parse:
                tmp['value'] = to_parse['value']
            if 'unit' in to_parse:
                tmp['unit'] = to_parse['unit']
            if 'ontologyTerms' in to_parse:
                tmp['ontologyTerms'] = to_parse['ontologyTerms']
        else:
            tmp['value'] = to_parse
        customs.append(tmp)
    doc['customField'] = customs
    return doc


def get_health_status(item):
    """
    extract health status for document
    :param item: source of information
    :return: list with health statuses
    """
    health_status = list()
    if 'health status' in item['characteristics']:
        key = 'health status'
    elif 'health status at collection' in item['characteristics']:
        key = 'health status at collection'
    else:
        return health_status
    for status in item['characteristics'][key]:
        health_status.append(
            {
                'text': status['text'],
                'ontologyTerms': status['ontologyTerms'][0]
            }
        )
    return health_status


def parse_relationship(item):
    results = dict()
    if 'relationships' not in item:
        return results
    accession = item['accession']
    for relation in item['relationships']:
        relationship_type = relation['type']
        # non-directional
        if relationship_type == 'EBI equivalent BioSample' or relationship_type == 'same as':
            results.setdefault(relationship_type, {})
            results.setdefault(to_lower_camel_case(relationship_type), {})
            target = relation['target'] if relation['source'] == item['accession'] else relation['source']
            results[relationship_type].setdefault(target, 0)
            results[to_lower_camel_case(relationship_type)].setdefault(target, 0)
            results[relationship_type][target] += 1
            results[to_lower_camel_case(relationship_type)][target] += 1
        # in an animal-sample relationship target will be the animal
        # in a membership relationship target will be this record while source will be the group
        # therefore relationship having the accession as target should be ignored
        # this will end up that directional relationship
        # 1 for animal only child of will be kept in the result
        # 2 for specimen only derived from will be kept
        else:
            if relation['target'] != accession:
                results.setdefault(relationship_type, {})
                results.setdefault(to_lower_camel_case(relationship_type), {})
                target = relation['target']
                results[relationship_type].setdefault(target, 0)
                results[to_lower_camel_case(relationship_type)].setdefault(target, 0)
                results[relationship_type][target] += 1
                results[to_lower_camel_case(relationship_type)][target] += 1
    return results


def get_alternative_id(relationships):
    """
    This function gets alternative id
    :param relationships: source of information
    :return: list of alternative ids
    """
    results = list()
    if 'sameAs' in relationships:
        for acc in relationships['sameAs']:
            results.append(acc)
    if 'EBI equivalent BioSample' in relationships:
        for acc in relationships['EBI equivalent BioSample']:
            results.append(acc)
    return results


def add_organism_info_for_specimen(accession, item):
    """
    This function adds organism information to specimen
    :param accession: accession
    :param item: source of information
    """
    ORGANISM_FOR_SPECIMEN.setdefault(accession, {})
    ORGANISM_FOR_SPECIMEN[accession]['biosampleId'] = item['accession']
    ORGANISM_FOR_SPECIMEN[accession]['organism'] = {
        'text': check_existence(item, 'Organism', 'text'),
        'ontologyTerms': check_existence(item, 'Organism', 'ontologyTerms')
    }
    ORGANISM_FOR_SPECIMEN[accession]['sex'] = {
        'text': check_existence(item, 'Sex', 'text'),
        'ontologyTerms': check_existence(item, 'Sex', 'ontologyTerms')
    }
    ORGANISM_FOR_SPECIMEN[accession]['breed'] = {
        'text': check_existence(item, 'breed', 'text'),
        'ontologyTerms': check_existence(item, 'breed', 'ontologyTerms')
    }
    ORGANISM_FOR_SPECIMEN[accession]['healthStatus'] = get_health_status(item)


def import_into_es(data, index_prefix, my_type, es):
    """
    This function will update current index with new data
    :param data: data to update elasticsearch with
    :param index_prefix: combined with my_type to generate the actual index value to operate on
    :param my_type: name of index to update
    :param es: elasticsearch object
    :return: updates index or return error it it was impossible ot sample didn't go through validation
    """
    if my_type == 'organism':
        validator = validate_organism_record.ValidateOrganismRecord(data, RULESETS)
        validation_results = validator.validate()
    else:
        # validation_results = validate_total_sample_records(data, my_type, RULESETS)
        validator = validate_specimen_record.ValidateSpecimenRecord(data, RULESETS)
        validation_results = validator.validate()
    for biosample_id in sorted(list(data.keys())):
        INDEXED_SAMPLES[biosample_id] = 1
        es_doc = data[biosample_id]
        error_messages = list()
        status = ''
        for ruleset in RULESETS:
            if validation_results[ruleset]['detail'][biosample_id]['status'] == 'error':
                status = 'error'
                error_messages.append(f"error\t{ruleset}\t"
                                      f"{validation_results[ruleset]['detail'][biosample_id]['message']}")
            else:
                es_doc['standardMet'] = constants.STANDARDS[ruleset]
                status = validation_results[ruleset]['detail'][biosample_id]['status']
                error_messages.append(f"{status}\t{ruleset}\t"
                                      f"{validation_results[ruleset]['detail'][biosample_id]['message']}")
                break
        body = json.dumps(es_doc)
        insert_es_log(es, index_prefix, my_type, biosample_id, status, ";".join(error_messages))
        insert_into_es(es, index_prefix, my_type, biosample_id, body)


def clean_elasticsearch(index, es):
    """
    This function will delete all records that do not exist in biosamples anymore
    :param index: name of index to check
    :param es: elasticsearch object
    """
    data = es.search(index=index, size=100000, _source="_id,standardMet")
    for hit in data['hits']['hits']:
        if hit['_id'] not in INDEXED_SAMPLES:
            # Legacy (basic) data imported in import_from_ena_legacy, not here, so could not be cleaned
            to_be_cleaned = True
            if 'standardMet' in hit['_source'] and hit['_source']['standardMet'] == constants.STANDARD_BASIC:
                to_be_cleaned = False
            if to_be_cleaned:
                es.delete(index=index, doc_type='_doc', id=hit['_id'])


if __name__ == "__main__":
    main()
