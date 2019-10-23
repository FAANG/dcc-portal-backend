import click
from constants import STAGING_NODE1
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, create_logging_instance, insert_into_es, get_datasets, \
    convert_analysis, generate_ena_api_endpoint, process_validation_result
import requests
import validate_analysis_record

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
    try:
        existing_datasets = get_datasets(hosts[0], es_index_prefix, only_faang=False)
    except KeyError:
        logger.error("No existing datasets retrieved from Elastic Search")
        logger.error("Possible causes:")
        logger.error("1 missing/wrong value for parameter es_index_prefix")
        logger.error("2 ES server has connection issue, index does not exist etc.")
        exit()

    for study_accession in eva_datasets:
        url = f"http://www.ebi.ac.uk/eva/webservices/rest/v1/studies/{study_accession}/summary"
        # expect always to have data from EVA as the list is retrieved live
        # get EVA summary
        eva_summary = requests.get(url).json()['response'][0]['result'][0]

        # f"https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&" \
        #    f"query=study_accession%3D%22{study_accession}%22&fields={field_str}"
        # extra constraint based on study accession
        optional_str = f"query=study_accession%3D%22{study_accession}%22"
        url = generate_ena_api_endpoint('analysis', 'ena', field_str, optional_str)
        response = requests.get(url)
        if response.status_code == 204:  # 204 is the status code for no content => the current term does not have match
            continue
        data = response.json()
        displayed = set()
        for record in data:
            analysis_accession = record['analysis_accession']
            if analysis_accession in analyses:
                es_doc = analyses[analysis_accession]
            else:
                es_doc = convert_analysis(record, existing_datasets)
                if not es_doc:
                    continue
                # in ENA api, it is description in ena result, different to analysis_description in faang result portal
                es_doc['description'] = record['description']
                if eva_summary['experimentType'] != '-':
                    es_doc['experimentType'] = eva_summary['experimentType'].split(', ')
                # es_doc['program'] = eva_summary['program']
                if eva_summary['platform'] != '-':
                    es_doc['platform'] = eva_summary['platform'].split(', ')
                # imputation has not been exported in the ENA warehouse
                # use PRJEB22988 (non farm animal) as example being both imputation and phasing project
                # es_doc['imputation'] = record['imputation']
            es_doc['sampleAccessions'].append(record['sample_accession'])
            analyses[analysis_accession] = es_doc
            count = len(analyses)
            if count % 50 == 0 and str(count) not in displayed:
                displayed.add(str(count))
                logger.info(f"Processed {count} analysis records")
        # end of analysis list for one study loop
    # end of all studies loop

    logger.info("Total analyses to be validated: " + str(len(analyses)))
    validator = validate_analysis_record.ValidateAnalysisRecord(analyses, RULESETS)
    validation_results = validator.validate()
    ruleset_version = validator.get_ruleset_version()
    process_validation_result(analyses, es, es_index_prefix, validation_results, ruleset_version, RULESETS, logger)


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
