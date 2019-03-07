from elasticsearch import Elasticsearch
import requests
import sys

from validate_sample_record import *

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

RULESETS = ["FAANG Experiments", "FAANG Legacy Experiments"]
STANDARDS = {
    'FAANG Experiments': 'FAANG',
    'FAANG Legacy Experiments': 'Legacy'
}
DATA_SOURCES = ['fastq', 'sra', 'cram_index']
DATA_TYPES = ['ftp', 'galaxy', 'aspera']


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
    ruleset_version = get_ruleset_version()
    indexed_files = dict()
    datasets = dict()
    experiments = dict()
    files = dict()
    studies_from_api = dict()
    exps_in_dataset = dict()
    for record in data:
        studies_from_api.setdefault(record['study_accession'], 0)
        studies_from_api[record['study_accession']] += 1
        library_strategy = record['library_strategy']
        assay_type = record['assay_type']
        experiment_target = record['experiment_target']
        if assay_type == '':
            if library_strategy == 'Bisulfite-Seq':
                assay_type = 'methylation profiling by high throughput sequencing'
            elif library_strategy == 'DNase-Hypersensitivity':
                assay_type = 'DNase-Hypersensitivity seq'
        if assay_type == 'whole genome sequencing':
            assay_type == 'whole genome sequencing assay'
        if assay_type == 'ATAC-seq':
            if not len(experiment_target) > 0:
                experiment_target = 'open_chromatin_region'
        elif assay_type == '"methylation profiling by high throughput sequencing':
            if not len(experiment_target) > 0:
                experiment_target = 'DNA methylation'
        elif assay_type == 'DNase-Hypersensitivity seq':
            if not len(experiment_target) > 0:
                experiment_target = 'open_chromatin_region'
        elif assay_type == 'Hi-C':
            if not len(experiment_target) > 0:
                experiment_target = 'chromatin'
        elif assay_type == 'whole genome sequencing assay':
            if not len(experiment_target) > 0:
                experiment_target = 'input DNA'
        file_type = ''
        source_type = ''
        try:
            for data_source in DATA_SOURCES:
                for my_type in DATA_TYPES:
                    key_to_check = f"{data_source}_{my_type}"
                    if key_to_check in record and record[key_to_check] != '':
                        file_type = my_type
                        source_type = data_source
                        raise BreakIt
        except:
            pass
        if file_type == '':
            continue
        if source_type == 'fastq':
            archive = 'ENA'
        elif source_type == 'cram_index':
            archive = 'CRAM'
        else:
            archive = 'SRA'
        files = record[f"{source_type}_{file_type}"].split(";")
        types = record['submitted_format'].split(";")
        sizes = record[f"{source_type}_bytes"].split(";")
        checksums = record[f"{source_type}_md5"].split(";")
        for index, file in enumerate(files):
            pass


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
