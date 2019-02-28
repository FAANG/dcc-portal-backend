from elasticsearch import Elasticsearch
import datetime
import requests
import sys
import re
from validate_sample_record import *
from get_all_etags import fetch_biosample_ids

INDEXED_SAMPLES = dict()
ORGANISM = dict()
SPECIMEN_FROM_ORGANISM = dict()
CELL_SPECIMEN = dict()
CELL_CULTURE = dict()
CELL_LINE = dict()
POOL_SPECIMEN = dict()
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
    process_organisms()

    print(f"Indexing specimen from organism starts at {datetime.datetime.now()}")
    process_specimens()

    print(f"Indexing cell specimens starts at {datetime.datetime.now()}")
    process_cell_specimens()

    print(f"Indexing cell culture starts at {datetime.datetime.now()}")
    process_cell_cultures()

    print(f"Indexing pool of specimen starts at {datetime.datetime.now()}")
    process_pool_specimen()

    print(f"Indexing cell line starts at {datetime.datetime.now()}")
    process_cell_lines()


def get_existing_etags():
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
    with open("etag_list_2019-02-26.txt", 'r') as f:
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
    url_schema = 'https://www.ebi.ac.uk/biosamples/samples/{}.json?curationdomain=self.FAANG_DCC_curation'
    url = url_schema.format(biosampleId)
    return requests.get(url).json()

def check_is_faang(item):
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

def process_organisms():
    doc_for_update = dict()
    for accession, item in ORGANISM.items():
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
        print(doc_for_update)
        sys.exit(0)

def process_specimens():
    pass

def process_cell_specimens():
    pass

def process_cell_cultures():
    pass

def process_pool_specimen():
    pass

def process_cell_lines():
    pass

def check_existence(item, field_name, subfield):
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

def parse_date(date):
    # TODO logging to error if date doesn't exist
    parsed_date = re.search("(\d+-\d+-\d+)T", date)
    if parsed_date:
        date = parsed_date.groups()[0]
    return date

if __name__ == "__main__":
    main()
