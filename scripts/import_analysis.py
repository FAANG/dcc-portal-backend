import click
from constants import STAGING_NODE1
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, write_system_log, get_line_number, get_record_ids, \
    convert_analysis, generate_ena_api_endpoint, process_validation_result
from misc import get_filename_from_url
import requests
import validate_analysis_record
import pandas as pd

SCRIPT_NAME = 'import_analysis'

RULESETS = ["FAANG Analyses", "FAANG Legacy Analyses"]

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
    to_es_flag = True
    if to_es.lower() == 'false':
        to_es_flag = False
    elif to_es.lower() == 'true':
        pass
    else:
        print('to_es parameter can only accept value of true or false')
        exit(1)

    hosts = es_hosts.split(";")
    es = Elasticsearch(hosts)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Start importing analysis', to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Command line parameters', to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), f'Hosts: {str(hosts)}', to_es_flag)
    es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
    if es_index_prefix:
        write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), f'Index_prefix: {es_index_prefix}', to_es_flag)

    # "https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&fields=all&dataPortal=faang"
    url = generate_ena_api_endpoint('analysis', 'faang', 'all')
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), f'Getting data from {url}', to_es_flag)
    data = requests.get(url).json()
    df = pd.DataFrame(data)
    analyses = dict()
    existing_datasets = get_record_ids(hosts[0], es_index_prefix, 'dataset', only_faang=False)

    # get analyses
    df.apply(lambda record: get_analyses(record, analyses, existing_datasets), axis=1)

    # validate analyses
    validator = validate_analysis_record.ValidateAnalysisRecord(analyses, RULESETS)
    validation_results = validator.validate()
    ruleset_version = validator.get_ruleset_version()

    # import valid analyses
    process_validation_result(analyses, es, es_index_prefix, validation_results, ruleset_version, RULESETS, to_es_flag)
    write_system_log(es, SCRIPT_NAME, 'info', get_line_number(), 'Finish importing analysis', to_es_flag)

def get_analyses(record, analyses, existing_datasets):
    '''
    This function gets analyses information from each record
    and creates analyses documents to import into elasticsearch
    '''
    analysis_accession = record['analysis_accession']
    if analysis_accession in analyses:
        es_doc = analyses[analysis_accession]
    else:
        es_doc = convert_analysis(record, existing_datasets)
        if not es_doc:
            return
        es_doc['project'] = record['project_name']
        es_doc['secondaryProject'] = record['secondary_project']
        es_doc['experimentAccessions'] = record['experiment_accession'].split(';')
        es_doc['runAccessions'] = record['run_accession'].split(';')
        es_doc['analysisAccessions'] = record['related_analysis_accession'].split(';')
        es_doc['description'] = record['analysis_description']
        es_doc['assayType'] = record['assay_type']
        protocol = record['analysis_protocol']
        es_doc.setdefault('analysisProtocol', dict())
        es_doc['analysisProtocol']['url'] = protocol
        es_doc['analysisProtocol']['filename'] = get_filename_from_url(protocol, record['analysis_accession'])
        es_doc['referenceGenome'] = record['reference_genome']

        analysis_date = record['analysis_date']
        if analysis_date and not isinstance(analysis_date, str):
            es_doc.setdefault('analysisDate', dict())
            es_doc['analysisDate']['text'] = analysis_date
            es_doc['analysisDate']['unit'] = 'YYYY-MM-DD'
        es_doc['analysisCodeRepository'] = record['analysis_code_repository']

    es_doc['sampleAccessions'].append(record['sample_accession'])
    analyses[record['analysis_accession']] = es_doc

if __name__ == "__main__":
    main()
