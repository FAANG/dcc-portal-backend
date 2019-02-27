from elasticsearch import Elasticsearch
import datetime
import requests
import sys
from validate_sample_record import *

INDEXED_SAMPLES = dict()
ORGANISM = dict()
SPECIMEN_FROM_ORGANISM = dict()
CELL_SPECIMEN = dict()
CELL_CULTURE = dict()
CELL_LINE = dict()
POOL_SPECIMEN = dict()
TOTAL_RECORDS_TO_UPDATE = 0


def main():
    ruleset_version = get_ruleset_version()
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    print(f"The program starts at {datetime.datetime.now()}")
    etags = get_existing_etags()
    print(f"There are {len(etags)} records with etags in ES")
    print(f"Finish retrieving existing etags at {datetime.datetime.now()}")
    print("Importing FAANG data")
    if len(etags) > 0:
        fetch_records_by_project_via_etag(etags)
    else:
        fetch_records_by_project()

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
                is_faang_labeled = check_is_faang(single)
                if not is_faang_labeled:
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
    pass

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

if __name__ == "__main__":
    main()
