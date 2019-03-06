from elasticsearch import Elasticsearch
import datetime
import requests
import sys
import re
import json
from validate_sample_record import *
from get_all_etags import fetch_biosample_ids
from columns import *
from misc import *

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
RULESETS = ["FAANG Samples", "FAANG Legacy Samples"]
STANDARDS = {
    'FAANG Samples': 'FAANG',
    'FAANG Legacy Samples': 'Legacy'
}
TOTAL_RECORDS_TO_UPDATE = 0


# TODO check single or double quotes
def main():
    ruleset_version = get_ruleset_version()
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])

    print(f"The program starts at {datetime.datetime.now()}")

    etags = get_existing_etags()

    print(f"There are {len(etags)} records with etags in ES")
    print(f"Finish retrieving existing etags at {datetime.datetime.now()}")
    print("Importing FAANG data")

    if len(etags) == 0 or len(fetch_biosample_ids())/len(etags) > 2:
        fetch_records_by_project()
    else:
        fetch_records_by_project_via_etag(etags)

    if TOTAL_RECORDS_TO_UPDATE == 0:
        print("Did not obtain any records from BioSamples")
        sys.exit(0)

    print(f"Indexing organism starts at {datetime.datetime.now()}")
    process_organisms(es)

    print(f"Indexing specimen from organism starts at {datetime.datetime.now()}")
    process_specimens(es)

    print(f"Indexing cell specimens starts at {datetime.datetime.now()}")
    process_cell_specimens(es)

    print(f"Indexing cell culture starts at {datetime.datetime.now()}")
    process_cell_cultures(es)

    print(f"Indexing pool of specimen starts at {datetime.datetime.now()}")
    process_pool_specimen(es)

    print(f"Indexing cell line starts at {datetime.datetime.now()}")
    process_cell_lines(es)

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
        # TODO add logging
        if union[acc]['count'] == 1:
            print(f"{acc} only in source {union[acc]['source']}")
    clean_elasticsearch('specimen')
    clean_elasticsearch('organism')
    print(f"Program ends at {datetime.datetime.now()}")


def get_existing_etags():
    """
    Function gets etags from organisms and specimens in elastisearch
    :return: list of etags
    """
    url_schema = 'http://wp-np3-e2.ebi.ac.uk:9200/{}/_search?_source=biosampleId,etag&sort=biosampleId&size=100000'
    results = dict()
    for item in ("organism", "specimen"):
        url = url_schema.format(item)
        response = requests.get(url).json()
        for result in response['hits']['hits']:
            if 'etag' in result['_source']:
                results[result['_source']['biosampleId']] = result['_source']['etag']
    return results


def fetch_records_by_project_via_etag(etags):
    global TOTAL_RECORDS_TO_UPDATE
    hash = dict()
    with open("etag_list_2019-03-05.txt", 'r') as f:
        for line in f:
            line = line.rstrip()
            data = line.split("\t")
            if data[0] in etags and etags[data[0]] == data[1]:
                INDEXED_SAMPLES[data[0]] = 1
                continue
            else:
                single = fetch_single_record(data[0])
                single['etag'] = data[1]
                if not check_is_faang(single):
                    continue
                material =single['characteristics']['Material'][0]['text']
                if material == 'organism':
                    ORGANISM[data[0]] = single
                elif material == 'specimen from organism':
                    SPECIMEN_FROM_ORGANISM[data[0]] = single
                elif material == 'cell specimen':
                    CELL_SPECIMEN[data[0]] = single
                elif material == 'cell culture':
                    CELL_CULTURE[data[0]] = single
                elif material == 'cell line':
                    CELL_LINE[data[0]] = single
                elif material == 'pool of specimens':
                    POOL_SPECIMEN[data[0]] = single
                hash.setdefault(material, 0)
                hash[material] += 1
    for k, v in hash.items():
        TOTAL_RECORDS_TO_UPDATE += v
        print(f"There are {v} {k} records needing update")
    if TOTAL_RECORDS_TO_UPDATE == 0:
        print("All records have not been modified since last importation.")
        print(f"Exit program at {datetime.datetime.now()}")
        sys.exit(0)
    if TOTAL_RECORDS_TO_UPDATE <=20:
        for item in ORGANISM, SPECIMEN_FROM_ORGANISM, CELL_SPECIMEN, CELL_CULTURE, CELL_LINE, POOL_SPECIMEN:
            for k in item:
                print(f"To be updated: {k}")
    print(f"The sum is {TOTAL_RECORDS_TO_UPDATE}")
    print(f"Finish comparing etags and retrieving necessary records at {datetime.datetime.now()}")


def fetch_records_by_project():
    global TOTAL_RECORDS_TO_UPDATE
    biosamples = list()
    hash = dict()
    url = 'https://www.ebi.ac.uk/biosamples/samples?size=1000&filter=attr%3Aproject%3AFAANG'
    while url:
        response = requests.get(url).json()
        if 'next' in response['_links']:
            url = response['_links']['next']['href']
            for biosample in response['_embedded']['samples']:
                biosamples.append(deal_with_decimal_degrees(biosample))
        else:
            url = ''
    for i,biosample in enumerate(biosamples):
        if not check_is_faang(biosample):
            continue
        material = biosample['characteristics']['Material'][0]['text']
        if material == 'organism':
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
        hash.setdefault(material, 0)
        hash[material] += 1
    for k, v in hash.items():
        TOTAL_RECORDS_TO_UPDATE += v
        print(f"There are {v} {k} records needing update")
    print(f"The sum is {TOTAL_RECORDS_TO_UPDATE}")


def fetch_single_record(biosampleId):
    """
    Function returns json file of single record from biosamples
    :param biosampleId: accession id or record to return
    :return: json file of sample with biosampleId
    """
    url_schema = 'https://www.ebi.ac.uk/biosamples/samples/{}.json?curationdomain=self.FAANG_DCC_curation'
    url = url_schema.format(biosampleId)
    return requests.get(url).json()


def check_is_faang(item):
    """
    Function checks that item has faang project
    :param item: item to check
    :return: True if item has faang project and False otherwise
    """
    if 'characteristics' in item and 'project' in item['characteristics']:
        for project in item['characteristics']['project']:
            if 'text' in project and project['text'].lower() == 'faang':
                return True
    return False


def deal_with_decimal_degrees(item):
    if item['characteristics']['Material'][0]['text'] == 'organism':
        try:
            if item['characteristics']['birth location latitude'][0]['unit'] == 'decimal degree' or \
                    item['characteristics']['birth location longitude'][0]['unit'] == 'decimal degree':
                url = "https://www.ebi.ac.uk/biosamples/samples/{}.json?curationdomain=self.FAANG_DCC_curation".format(
                    item['accession'])
                return requests.get(url).json()
            else:
                return item
        except KeyError:
            return item
    else:
        return item


def process_organisms(es):
    """
    Function prepares json file that should be inserted inside elasticsearch
    :return: dictionary with data that should be inserted inside elasticsearch
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
        relationships = parse_relationship(item)
        if 'childOf' in relationships:
            doc_for_update['childOf'] = relationships.keys()
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
    insert_into_es(converted, 'organism', es)


def process_specimens(es):
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
            for health_status in item['characteristics']['health status at collection']:
                doc_for_update['specimenFromOrganism']['healthStatusAtCollection'].append(
                    {
                        'text': health_status['text'],
                        'ontologyTerms': health_status['ontologyTerms'][0]
                    }
                )
        if organism_accession not in ORGANISM_FOR_SPECIMEN:
            add_organism_info_for_specimen(organism_accession, fetch_single_record(organism_accession))

        doc_for_update['organism'] = ORGANISM_FOR_SPECIMEN[organism_accession]
        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
        ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
        converted[accession] = doc_for_update
    insert_into_es(converted, 'specimen', es)


def process_cell_specimens(es):
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
        if organism_accession not in ORGANISM_FOR_SPECIMEN:
            add_organism_info_for_specimen(organism_accession, fetch_single_record(organism_accession))
        doc_for_update['organism'] = ORGANISM_FOR_SPECIMEN[organism_accession]
        ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
        ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
        converted[accession] = doc_for_update
    insert_into_es(converted, 'specimen', es)


def process_cell_cultures(es):
    converted = dict()
    for accession, item in CELL_CULTURE.items():
        doc_for_update = dict()
        relationships = parse_relationship(item)
        url = check_existence(item, 'cell culture protocol', 'text')
        filename = get_filename_from_url(url, accession)
        derived_from_accession = list(relationships['derivedFrom'].keys())[0]
        organism_accession = ''
        if derived_from_accession in SPECIMEN_ORGANISM_RELATIONSHIP:
            organism_accession = SPECIMEN_ORGANISM_RELATIONSHIP[derived_from_accession]
        else:
            tmp = parse_relationship(fetch_single_record(derived_from_accession))
            if 'derivedFrom' in tmp:
                organism_accession = list(tmp['derivedFrom'].keys())[0]
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
        if organism_accession not in ORGANISM_FOR_SPECIMEN:
            add_organism_info_for_specimen(organism_accession, fetch_single_record(organism_accession))
        doc_for_update['organism'] = ORGANISM_FOR_SPECIMEN[organism_accession]
        ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
        ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
        converted[accession] = doc_for_update
    insert_into_es(converted, 'specimen', es)


def process_pool_specimen(es):
    converted = dict()
    for accession, item in POOL_SPECIMEN.items():
        doc_for_update = dict()
        relationships = parse_relationship(item)
        url = check_existence(item, 'pool creation protocol', 'text')
        filename = get_filename_from_url(url, accession)
        doc_for_update.setdefault('poolOfSpecimens', {})
        doc_for_update['poolOfSpecimens']['poolCreationDate'] = {
            'text': check_existence(item, 'pool creation date', 'text'),
            'unit': check_existence(item, 'pool creation date', 'unit')
        }
        doc_for_update['poolOfSpecimens']['poolCreationProtocol'] = {
            'ulr': url,
            'filename': filename
        }
        doc_for_update['poolOfSpecimens']['specimenVolume'] = {
            'text': check_existence(item, 'specimen volume', 'text'),
            'unit': check_existence(item, 'specimen volume', 'unit')
        }
        doc_for_update['poolOfSpecimens']['specimenSize'] = {
            'text': check_existence(item, 'specimen size', 'text'),
            'unit': check_existence(item, 'specimen size', 'unit')
        }
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
            for acc in derived_from:
                if acc in SPECIMEN_ORGANISM_RELATIONSHIP:
                    organism_accession = SPECIMEN_ORGANISM_RELATIONSHIP[acc]
                    ORGANISM_REFERRED_BY_SPECIMEN.setdefault(organism_accession, 0)
                    ORGANISM_REFERRED_BY_SPECIMEN[organism_accession] += 1
                    if organism_accession not in ORGANISM_FOR_SPECIMEN:
                        add_organism_info_for_specimen(organism_accession, fetch_single_record(organism_accession))
                    tmp['organism'] = {
                        ORGANISM_FOR_SPECIMEN[organism_accession] : {
                            'organism': {
                                'text': ORGANISM_FOR_SPECIMEN[organism_accession]['organism']['ontologyTerms']
                            }
                        }
                    }
                    tmp['sex'] = {
                        ORGANISM_FOR_SPECIMEN[organism_accession]: {
                            'sex': {
                                'text': ORGANISM_FOR_SPECIMEN[organism_accession]['sex']['ontologyTerms']
                            }
                        }
                    }
                    tpm['breed'] = {
                        ORGANISM_FOR_SPECIMEN[organism_accession]: {
                            'breed': {
                                'text': ORGANISM_FOR_SPECIMEN[organism_accession]['breed']['ontologyTerms']
                            }
                        }
                    }
                else:
                    # TODO error logging
                    print(f"No organism found for specimen {acc}")
        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        for type in ['organism', 'sex', 'breed']:
            values = list(tmp[type].keys())
            if len(values) == 1:
                doc_for_update['organism'].setdefault(type, {})
                doc_for_update['organism'][type]['text'] = values[0]
                doc_for_update['organism'][type]['ontologyTerms'] = tmp[type][values[0]]
            else:
                doc_for_update['organism'].setdefault(type, {})
                doc_for_update['organism'][type]['text'] = ";".join(values)
        converted[accession] = doc_for_update
    insert_into_es(converted, 'specimen', es)


def process_cell_lines(es):
    converted = dict()
    for accession, item in CELL_LINE.items():
        doc_for_update = dict()
        relationships = parse_relationship(item)
        url = check_existence(item, 'culture protocol', 'text')
        if url:
            filename = get_filename_from_url(url, accession)
        else:
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
        if 'derivedFrom' in relationships:
            doc_for_update['derivedFrom'] = relationships['derivedFrom'][0]
        doc_for_update['alternativeId'] = get_alternative_id(relationships)
        doc_for_update.setdefault('organism', {})
        for type in ['organism', 'sex', 'breed']:
            doc_for_update['organism'][type] = doc_for_update['cellLine'][type]
        converted[accession] = doc_for_update
    insert_into_es(converted, 'specimen', es)


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


def populate_basic_biosample_info(doc, item):
    """
    This function add common field to document
    :param doc: documen to update with common fields
    :param item: source of information
    :return: updated document
    """
    doc['name'] = item['name']
    doc['biosampleId'] = item['accession']
    doc['etag'] = item['etag']
    doc['id_number'] = item['accession'][5:]
    doc['description'] = check_existence(item, 'description', 'text')
    doc['releaseDate'] = parse_date(item['release'])
    doc['updateDate'] = parse_date(item['update'])
    doc['material'] = {
        "text": check_existence(item, 'Material', 'text'),
        "ontologyTerms": check_existence(item, 'Material', 'ontologyTerms')
    }
    doc['project'] = check_existence(item, 'project', 'text')
    doc['availability'] = check_existence(item, 'availability', 'text')
    for organization in item['organization']:
        # TODO logging to error if name or role or url do not exist
        doc.setdefault('organization', [])
        doc['organization'].append(
            {
                'name': organization['Name'],
                'role': organization['Role'],
                'URL': organization['URL']
            }
        )
    return doc


def extract_custom_field(doc, item, type):
    """
    This function adds custom fields to document from item
    :param doc: document to update
    :param item: source of information
    :param type: type of document
    :return: updated document
    """
    characteristics = item['characteristics'].copy()
    if type not in known_columns:
        # TODO logging to error
        sys.exit(0)
    for column in common_columns + known_columns[type]:
        if column in characteristics:
            characteristics.pop(column)
    customs = list()
    for k, v in characteristics.items():
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
    extract health status for documen
    :param item: source of information
    :return: list with health statuses
    """
    health_status = list()
    if 'health status' in item['characteristics']:
        key = 'health status'
    elif 'health status at collection' in item['characteristics']:
        key = 'health status at collection'
    else:
        # TODO logging
        print("Health status was not provided")
        print(item['characteristics'])
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
        type = relation['type']
        results.setdefault(type, {})
        results.setdefault(to_lower_camel_case(type), {})
        if type == 'EBI equivalent BioSample' or type == 'same as':
            target = relation['target'] if relation['source'] == item['accession'] else relation['source']
            results[type].setdefault(target, 0)
            results[to_lower_camel_case(type)].setdefault(target, 0)
            results[type][target] += 1
            results[to_lower_camel_case(type)][target] += 1
        else:
            if relation['target'] != accession:
                target = relation['target']
                results[type].setdefault(target, 0)
                results[to_lower_camel_case(type)].setdefault(target, 0)
                results[type][target] += 1
                results[to_lower_camel_case(type)][target] += 1
    return results


def get_alternative_id(relationships):
    """
    This function gets alternative is
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


def parse_date(date):
    """
    This function parses date
    :param date: date to parse
    :return: parsed date
    """
    # TODO logging to error if date doesn't exist
    parsed_date = re.search("(\d+-\d+-\d+)T", date)
    if parsed_date:
        date = parsed_date.groups()[0]
    return date


def insert_into_es(data, my_type, es):
    """
    This function will update current index with new data
    :param data: data to update elasticsearch with
    :param my_type: name of index to update
    :param es: elasticsearch object
    :return: updates index or return error it it was impossible ot sample didn't go through validation
    """
    validation_results = validate_total_sample_records(data, my_type, RULESETS)
    for biosample_id in sorted(list(data.keys())):
        INDEXED_SAMPLES[biosample_id] = 1
        es_doc = data[biosample_id]
        for ruleset in RULESETS:
            if validation_results[ruleset]['detail'][biosample_id]['status'] == 'error':
                # TODO logging to error
                print(f"{biosample_id}\t{validation_results[ruleset]['detail'][biosample_id]['type']}\t{'error'}\t"
                      f"{validation_results[ruleset]['detail'][biosample_id]['message']}")
            else:
                es_doc['standardMet'] = STANDARDS[ruleset]
                break
        body = {
            "doc": json.dumps(es_doc)
        }
        try:
            es.update(index=my_type, doc_type="_doc", id=biosample_id, body=body)
        except:
            # TODO logging error
            print("Error when try to update elasticsearch index")


def clean_elasticsearch(type):
    pass


if __name__ == "__main__":
    main()
