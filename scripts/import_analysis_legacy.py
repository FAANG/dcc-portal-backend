import click
from constants import STANDARDS, STAGING_NODE1, STANDARD_LEGACY, STANDARD_FAANG
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, create_logging_instance, insert_into_es, get_datasets, \
    convert_analysis
import requests
import json
import validate_analysis_record
from typing import List, Dict

RULESETS = ["FAANG Legacy Analyses"]
EVA_SPECIES = ['Chicken', 'Cow', 'Goat', 'Horse', 'Pig', 'Sheep']
FIELD_LIST = [
    "analysis_accession", "study_accession", "sample_accession", "analysis_title", "analysis_type", "center_name",
    "first_public", "last_updated", "study_title", "tax_id", "scientific_name", "analysis_alias", "submitted_bytes",
    "submitted_md5", "submitted_ftp", "submitted_aspera", "submitted_galaxy", "submitted_format",
    "broker_name", "pipeline_name", "pipeline_version", "assembly_type", "accession", "description", "germline"
]


logger = create_logging_instance('import_analysis_legacy')

@click.command()
@click.option(
    '--es_hosts',
    default=STAGING_NODE1,
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
# TODO check single or double quotes
def main(es_hosts, es_index_prefix):
    """
    Main function that will import analysis data from ena
    :param es_hosts: elasticsearch hosts where the data import into
    :param es_index_prefix: the index prefix points to a particular version of data
    :return:
    """
    hosts = es_hosts.split(";")
    logger.info("Command line parameters")
    logger.info("Hosts: "+str(hosts))
    es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
    if es_index_prefix:
        logger.info("Index_prefix:"+es_index_prefix)

    es = Elasticsearch(hosts)

    eva_datasets = get_eva_dataset_list()
    field_str = ",".join(FIELD_LIST)
    analyses = dict()
    existing_datasets = get_datasets(hosts[0], es_index_prefix, only_faang=False)
    for study_accession in eva_datasets:
        url = f"http://www.ebi.ac.uk/eva/webservices/rest/v1/studies/{study_accession}/summary"
        # expect always to have data from EVA as the list is retrieved live
        # get EVA summary
        eva_summary = requests.get(url).json()['response'][0]['result'][0]

        url = f"https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&" \
            f"query=study_accession%3D%22{study_accession}%22&fields={field_str}"
        response = requests.get(url)
        if response.status_code == 204:  # 204 is the status code for no content => the current term does not have match
            continue
        data = response.json()
        for record in data:
            es_doc = convert_analysis(record, existing_datasets)
            if not es_doc:
                continue
            # in ENA api, it is description, different to analysis_description in FAANG portal
            es_doc['description'] = record['description']
            # es_doc['']
            analyses[record['analysis_accession']] = es_doc
        # end of analysis list for one study loop
    # end of all studies loop

    logger.info(len(analyses))
    validator = validate_analysis_record.validate_analysis_record(analyses, RULESETS)
    validation_results = validator.validate()
    analysis_validation = dict()
    for analysis_accession, analysis_es in analyses.items():
        for ruleset in RULESETS:
            if validation_results[ruleset]['detail'][analysis_accession]['status'] == 'error':
                logger.info(f"{analysis_accession}\tAnalysis\terror\t"
                            f"{validation_results[ruleset]['detail'][analysis_accession]['message']}")
            else:
                # only indexing when meeting standard
                analysis_validation[analysis_accession] = STANDARDS[ruleset]
                analysis_es['standardMet'] = STANDARDS[ruleset]
                if analysis_es['standardMet'] == STANDARD_FAANG:
                    analysis_es['versionLastStandardMet'] = validator.get_ruleset_version()

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
                # index into ES so break the loop
                break


def get_eva_dataset_list():
    species_str = ",".join(EVA_SPECIES)
    logger.info(f"Species to retrieve from EVA: {species_str}")
    url = f"http://www.ebi.ac.uk/eva/webservices/rest/v1/meta/studies/all?species={species_str}"
    data = requests.get(url).json()
    logger.info(f"Total number of datasets in EVA: {data['response'][0]['numResults']}")
    eva_datasets = list()
    for record in data['response'][0]['result']:
        eva_datasets.append(record['id'])
    return eva_datasets


if __name__ == "__main__":
    main()
