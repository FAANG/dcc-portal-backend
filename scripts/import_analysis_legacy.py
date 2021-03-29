import click
from constants import STAGING_NODE1
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, write_system_log, get_line_number, get_record_ids, \
    convert_analysis, generate_ena_api_endpoint, process_validation_result
import requests
import validate_analysis_record
import pandas as pd

SCRIPT_NAME = 'import_analysis_legacy'

RULESETS = ["FAANG Legacy Analyses"]
EVA_SPECIES = ['Chicken', 'Cow', 'Goat', 'Horse', 'Pig', 'Sheep']
FIELD_LIST = [
    "analysis_accession", "study_accession", "sample_accession", "analysis_title", "analysis_type", "center_name",
    "first_public", "last_updated", "study_title", "tax_id", "scientific_name", "analysis_alias", "submitted_bytes",
    "submitted_md5", "submitted_ftp", "submitted_aspera", "submitted_galaxy", "submitted_format",
    "broker_name", "pipeline_name", "pipeline_version", "assembly_type", "accession", "description", "germline"
]

to_es_flag = True
es = None
analyses = dict()
displayed = set()
existing_datasets = list()

@click.command()
@click.option(
    '--es_hosts',
    default=STAGING_NODE1,
    help='Specify the Elastic Search server(s) (port could be included), e.g. wp-np3-e2:9200. '
         'If multiple servers are provided, please use ";" to separate them, e.g. "wp-np3-e2;wp-np3-e3"'
)
@click.option(
    '--es_index_prefix',
    default="faang_build_3",
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
# TODO check single or double quotes
def main(es_hosts, es_index_prefix, to_es: str):
    """
    Main function that will import analysis data from ena
    :param es_hosts: elasticsearch hosts where the data import into
    :param es_index_prefix: the index prefix points to a particular version of data
    :param to_es: determine whether to output log to Elasticsearch (True) or terminal (False, printing)
    :return:
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
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Start importing analysis legacy', to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Command line parameters', to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), f'Hosts: {str(hosts)}', to_es_flag)
    es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
    if es_index_prefix:
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                         f'Index_prefix: {es_index_prefix}', to_es_flag)

    es = Elasticsearch(hosts)

    eva_datasets = get_eva_dataset_list()
    field_str = ",".join(FIELD_LIST)
    try:
        existing_datasets = get_record_ids(hosts[0], es_index_prefix, 'dataset', only_faang=False)
    except KeyError:
        write_system_log(es, SCRIPT_NAME, 'error', get_line_number(),
                         'No existing datasets retrieved from Elastic Search', to_es_flag)
        write_system_log(es, SCRIPT_NAME, 'error', get_line_number(), 'Possible causes:', to_es_flag)
        write_system_log(es, SCRIPT_NAME, 'error', get_line_number(),
                         '1 missing/wrong value for parameter es_index_prefix', to_es_flag)
        write_system_log(es, SCRIPT_NAME, 'error', get_line_number(),
                         '2 ES server has connection issue, index does not exist etc.', to_es_flag)
        exit()

    for study_accession in eva_datasets:
        # expect always to have data from EVA as the list is retrieved live
        # get EVA summary
        url = f"http://www.ebi.ac.uk/eva/webservices/rest/v1/studies/{study_accession}/summary"
        eva_summary = requests.get(url).json()['response'][0]['result'][0]

        # f"https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&" \
        #    f"query=study_accession%3D%22{study_accession}%22&fields={field_str}"
        # extra constraint based on study accession
        optional_str = f"query=study_accession%3D%22{study_accession}%22"
        url = generate_ena_api_endpoint('analysis', 'ena', field_str, optional_str)
        
        # get analyses data associated with study accession
        response = requests.get(url)
        if response.status_code == 204:  # 204 no content => the current term does not have match
            continue
        data = response.json()

        # process analyses data associated with the study_accession
        df = pd.DataFrame(data)
        df.apply(lambda record: get_processed_analyses_data(record, eva_summary), axis=1)

    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                     f'Total analyses to be validated: {str(len(analyses))}', to_es_flag)
    
    # validate analyses
    validator = validate_analysis_record.ValidateAnalysisRecord(analyses, RULESETS)
    validation_results = validator.validate()
    ruleset_version = validator.get_ruleset_version()
    
    # import valid analyses
    process_validation_result(analyses, es, es_index_prefix, validation_results, ruleset_version, RULESETS, to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Finish importing analysis legacy', to_es_flag)


def get_eva_dataset_list():
    '''
    Returns list of datasets (study_accession) in EVA
    '''
    species_str = ",".join(EVA_SPECIES)
    write_system_log(es, 'import_analysis_legacy', 'info', get_line_number(),
                     f'Species to retrieve from EVA: {species_str}', to_es_flag)
    url = f'http://www.ebi.ac.uk/eva/webservices/rest/v1/meta/studies/all?species={species_str}'
    data = requests.get(url).json()
    write_system_log(es, 'import_analysis_legacy', 'info', get_line_number(),
                     f"Total number of datasets in EVA: {data['response'][0]['numResults']}", to_es_flag)
    eva_datasets = list()
    for record in data['response'][0]['result']:
        eva_datasets.append(record['id'])
    return eva_datasets


def get_processed_analyses_data(record, eva_summary):
    '''
    Return processed data for the analysis record
    '''
    analysis_accession = record['analysis_accession']
    if analysis_accession in analyses:
        es_doc = analyses[analysis_accession]
    else:
        es_doc = convert_analysis(record, existing_datasets)
        if not es_doc:
            return
        # in ENA api, it is description in ena result, different to analysis_description in faang result portal
        es_doc['description'] = record['description']
        if eva_summary['experimentType'] != '-':
            es_doc['experimentType'] = eva_summary['experimentType'].split(', ')
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
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(),
                            f'Processed {count} analysis records:', to_es_flag)


if __name__ == "__main__":
    main()
