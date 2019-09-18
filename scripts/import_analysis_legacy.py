import click
from constants import STANDARDS, STAGING_NODE1, STANDARD_LEGACY, STANDARD_FAANG
from elasticsearch import Elasticsearch
from utils import remove_underscore_from_end_prefix, create_logging_instance, insert_into_es
from misc import convert_readable, get_filename_from_url
import requests
import json
import validate_analysis_record
from typing import List, Dict

RULESETS = ["FAANG Legacy Analyses"]
FILE_SERVER_TYPES = ['ftp', 'galaxy', 'aspera']
EVA_SPECIES = ['Chicken', 'Cow', 'Goat', 'Horse', 'Pig', 'Sheep']
FIELD_LIST = [
    "analysis_accession", "study_accession", "sample_accession", "analysis_title", "analysis_type", "center_name",
    "first_public", "last_updated", "study_title", "tax_id", "scientific_name", "analysis_alias", "submitted_bytes",
    "submitted_md5", "submitted_ftp", "submitted_aspera", "submitted_galaxy", "broker_name", "pipeline_name",
    "pipeline_version", "assembly_type", "accession", "description", "germline"
]


logger = create_logging_instance('import_analysis_legacy', to_file=False)

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
    for study_accession in eva_datasets:
        url = f"https://www.ebi.ac.uk/ena/portal/api/search/?result=analysis&format=JSON&limit=0&" \
            f"query=study_accession%3D%22{study_accession}%22&fields={field_str}"
        print(url)
        response = requests.get(url)
        if response.status_code == 204:  # 204 is the status code for no content => the current term does not have match
            continue
        data = response.json()
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
                    es_doc.setdefault('urls', list())
                    es_doc['fileNames'].append(fullname)
                    es_doc['fileTypes'].append(suffix)
                    es_doc['fileSizes'].append(convert_readable(sizes[i]))
                    es_doc['checksumMethods'].append('md5')
                    es_doc['checksums'].append(checksums[i])
                    es_doc['urls'].append(file)
            es_doc['accession'] = record['analysis_accession']
            es_doc['title'] = record['analysis_title']
            es_doc['alias'] = record['analysis_alias']
            es_doc['releaseDate'] = record['first_public']
            es_doc['updateDate'] = record['last_updated']
            es_doc.setdefault('organism', dict())
            es_doc['organism']['text'] = record['scientific_name']
            es_doc['organism']['ontologyTerms'] = f"http://purl.obolibrary.org/obo/NCBITaxon_{record['tax_id']}"

            es_doc['datasetAccession'] = study_accession
            es_doc.setdefault('sampleAccessions', list())
            es_doc['sampleAccessions'] = record['sample_accession']

            es_doc['description'] = record['description']
            # es_doc['analysisDate'] = record['analysis_alias']
            es_doc['analysisCenter'] = record['center_name']
            es_doc['analysisType'] = record['analysis_type']
            analyses[record['analysis_accession']] = es_doc
        # end of analysis list for one study loop
        break
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
