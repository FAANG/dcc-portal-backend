from elasticsearch import Elasticsearch
import requests
import sys

TECHNOLOGIES = {
    'ATAC-seq': 'ATAC-seq',
    'methylation profiling by high throughput sequencing': 'BS-seq',
    'ChIP-seq': 'ChIP-seq',
    'DNase-Hypersensitivity seq': 'DNase-seq',
    'Hi-C': 'Hi-C',
    'microRNA profiling by high throughput sequencing': 'RNA-seq',
    'RNA-seq of coding RNA': 'RNA-seq',
    'RNA-seq of non coding RNA': 'RNA-seq',
    'transcription profiling by high throughput sequencing': 'RNA-seq',
    'whole genome sequencing assay': 'WGS'
}


def main():
    """
    Main function that will import data from ena
    :return:
    """
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    data = get_ena_data()
    biosample_ids = get_all_specimen_ids()
    if not biosample_ids:
        # TODO log to error
        print('BioSample IDs were not imported')
        sys.exit(0)
    known_errors = get_known_errors()
    print(known_errors)


def get_ena_data():
    """
    This function will fetch data from ena
    :return: json representation of data from ena
    """
    response = requests.get('https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0'
                            '&dataPortal=faang&fields=all').json()
    return response


def get_all_specimen_ids():
    """
    This function return dict with all information from specimens
    :return: json representation of data from specimens
    """
    results = dict()
    response = requests.get('http://wp-np3-e2.ebi.ac.uk:9200/specimen/_search?size=100000').json()
    for item in response['hits']['hits']:
        results[item['_id']] = item['_source']
    return results


def get_known_errors():
    """
    This function will read file with associtation from study to biosample
    :return: dictionary with study as a key and biosample as a values
    """
    known_errors = dict()
    with open('ena_not_in_biosample.txt', 'r') as f:
        for line in f:
            line = line.rstrip()
            study, biosample = line.split("\t")
            known_errors.setdefault(study, {})
            known_errors[study][biosample] = 1
    return known_errors


if __name__ == "__main__":
    main()
