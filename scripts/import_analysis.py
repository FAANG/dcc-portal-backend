import click
from constants import STANDARDS, STAGING_NODE1, STANDARD_LEGACY, STANDARD_FAANG
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, create_logging_instance, insert_into_es, get_datasets, \
    convert_analysis
from misc import get_filename_from_url
import requests
import json
import validate_analysis_record
from typing import List, Dict

RULESETS = ["FAANG Analyses", "FAANG Legacy Analyses"]
FILE_SERVER_TYPES = ['ftp', 'galaxy', 'aspera']

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

    url = "https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&fields=all&dataPortal=faang"
    data = requests.get(url).json()
    analyses = dict()
    existing_datasets = get_datasets(hosts[0], es_index_prefix, only_faang=False)
    for record in data:
        es_doc = convert_analysis(record, existing_datasets)
        if not es_doc:
            continue

        es_doc['project'] = record['project_name']


        es_doc.setdefault('experimentAccessions', list())
        es_doc.setdefault('runAccessions', list())
        for elmt in record['experiment_accession'].split(','):
            if elmt:
                es_doc['experimentAccessions'].append(elmt)
        for elmt in record['run_accession'].split(','):
            if elmt:
                es_doc['runAccessions'].append(elmt)

        es_doc['description'] = record['analysis_description']
        es_doc['assayType'] = record['assay_type']
        protocol = record['analysis_protocol']
        es_doc.setdefault('analysisProtocol', dict())
        es_doc['analysisProtocol']['url'] = protocol
        es_doc['analysisProtocol']['filename'] = get_filename_from_url(protocol, record['analysis_accession'])
        es_doc['referenceGenome'] = record['reference_genome']
        # es_doc['analysisLink'] = record['analysis_alias']
        # es_doc['analysisCodeRepository'] = record['analysis_alias']
        analyses[record['analysis_accession']] = es_doc

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


if __name__ == "__main__":
    main()
