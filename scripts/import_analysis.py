import click
from constants import STANDARDS, STAGING_NODE1, STANDARD_LEGACY, STANDARD_FAANG
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, create_logging_instance, insert_into_es
import requests
import json
import validate_analysis_record

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

    url = "https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&fields=all&dataPortal=faang"
    data = requests.get(url).json()
    analyses = dict()
    for record in data:
        file_server_type = ''
        for tmp in FILE_SERVER_TYPES:
            key_to_check = f"submitted_{tmp}"
            if key_to_check in record and record[key_to_check] != '':
                file_server_type = tmp
                break
        if len(file_server_type) == 0:
            continue

        es_doc = dict()
        files = record[f"submitted_{file_server_type}"].split(";")
        sizes = record["submitted_bytes"].split(";")
        # for ENA, it is fixed to MD5 as the checksum method
        checksums = record["submitted_md5"].split(";")
        if len(files) != len(checksums) or len(files) != len(sizes) or len(files) == 0:
            continue
        for i, file in enumerate(files):
            fullname = file.split("/")[-1]
            # filename = fullname.split(".")[0]
            suffix = fullname.split(".")[-1]
            if suffix != 'md5':
                es_doc.setdefault('fileNames', list())
                es_doc.setdefault('fileTypes', list())
                es_doc.setdefault('fileSizes', list())
                es_doc.setdefault('checksumMethods', list())
                es_doc.setdefault('checksums', list())
                es_doc['fileNames'].append(fullname)
                es_doc['fileTypes'].append(suffix)
                es_doc['fileSizes'].append(sizes[i])
                es_doc['checksumMethods'].append('md5')
                es_doc['checksums'].append(checksums[i])
        es_doc['accession'] = record['analysis_accession']
        es_doc['title'] = record['analysis_title']
        es_doc['alias'] = record['analysis_alias']
        es_doc['releaseDate'] = record['first_public']
        es_doc['updateDate'] = record['last_updated']
        es_doc['project'] = record['project_name']
        es_doc.setdefault('organism', dict())
        es_doc['organism']['text'] = record['scientific_name']
        es_doc['organism']['ontologyTerms'] = f"http://purl.obolibrary.org/obo/NCBITaxon_{record['tax_id']}"

        es_doc['datasetAccession'] = record['study_accession']
        es_doc.setdefault('sampleAccessions', list())
        es_doc['sampleAccessions'] = record['sample_accession']

        es_doc.setdefault('experimentAccessions', list())
        es_doc.setdefault('runAccessions', list())
        # es_doc['experimentAccessions'].append()
        # es_doc['runAccessions'].append()

        # es_doc['description'] = record['analysis_alias']
        # es_doc['analysisDate'] = record['analysis_alias']
        es_doc['analysisCenter'] = record['center_name']
        # es_doc['assayType'] = record['center_name']
        # es_doc.setdefault('analysisProtocol', dict())
        # es_doc['analysisProtocol']['url'] = record['']
        # es_doc['analysisProtocol']['filename'] = record['']
        es_doc['analysisType'] = record['analysis_type']
        # es_doc['referenceGenome'] = record['analysis_alias']
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
                body = json.dumps(analysis_es)
                insert_into_es(es, es_index_prefix, 'analysis', analysis_accession, body)
                # index into ES so break the loop
                break


if __name__ == "__main__":
    main()
