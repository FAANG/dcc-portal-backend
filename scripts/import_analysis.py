import click
from constants import STAGING_NODE1
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, create_logging_instance, get_datasets, \
    convert_analysis, generate_ena_api_endpoint, process_validation_result
from misc import get_filename_from_url
import requests
import validate_analysis_record

RULESETS = ["FAANG Analyses", "FAANG Legacy Analyses"]

logger = create_logging_instance('import_analysis')


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

    # "https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&fields=all&dataPortal=faang"
    url = generate_ena_api_endpoint('analysis', 'faang', 'all')
    logger.info(f"Getting data from {url}")
    data = requests.get(url).json()
    analyses = dict()
    existing_datasets = get_datasets(hosts[0], es_index_prefix, only_faang=False)
    for record in data:
        analysis_accession = record['analysis_accession']
        if analysis_accession in analyses:
            es_doc = analyses[analysis_accession]
        else:
            es_doc = convert_analysis(record, existing_datasets)
            if not es_doc:
                continue
            es_doc['project'] = record['project_name']
            es_doc['secondaryProject'] = record['secondary_project']
            es_doc.setdefault('experimentAccessions', list())
            es_doc.setdefault('runAccessions', list())
            es_doc.setdefault('analysisAccessions', list())
            for elmt in record['experiment_accession'].split(' '):
                es_doc['experimentAccessions'].append(elmt)
            for elmt in record['run_accession'].split(' '):
                es_doc['runAccessions'].append(elmt)
            for elmt in record['related_analysis_accession'].split(' '):
                es_doc['analysisAccessions'].append(elmt)

            es_doc['description'] = record['analysis_description']
            es_doc['assayType'] = record['assay_type']
            protocol = record['analysis_protocol']
            es_doc.setdefault('analysisProtocol', dict())
            es_doc['analysisProtocol']['url'] = protocol
            es_doc['analysisProtocol']['filename'] = get_filename_from_url(protocol, record['analysis_accession'])
            es_doc['referenceGenome'] = record['reference_genome']

            analysis_date = record['analysis_date']
            if analysis_date:
                es_doc.setdefault('analysisDate', dict())
                es_doc['analysisDate']['text'] = analysis_date
                es_doc['analysisDate']['unit'] = 'YYYY-MM-DD'
            es_doc['analysisCodeRepository'] = record['analysis_code_repository']

        es_doc['sampleAccessions'].append(record['sample_accession'])
        analyses[record['analysis_accession']] = es_doc

    validator = validate_analysis_record.ValidateAnalysisRecord(analyses, RULESETS)
    validation_results = validator.validate()
    ruleset_version = validator.get_ruleset_version()
    process_validation_result(analyses, es, es_index_prefix, validation_results, ruleset_version, RULESETS, logger)


if __name__ == "__main__":
    main()
