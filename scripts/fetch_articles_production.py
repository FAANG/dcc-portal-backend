"""
The script is based on the API provided by europe PMC http://europepmc.org/
ePMC treats the full text as free text, so when the accessions are mentioned in the full text, they will be indexed.
Due to the factor that it is much more likely to mention the dataset accession as a whole than each individual records,
e.g. paper PMC6500009 mentions several dataset accessions PRJNA526824 etc and the sample range SAMN11119414-SAMN11119461
After retrieving dataset-article relationships, the same relationships will be extended to all samples/files under the
dataset
Using the example above, each individual sample between SAMN11119414-SAMN11119461 will have the article PMC6500009
"""
import os

import requests
import copy
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from utils_fetch_articles import get_record_ids, get_record_details, insert_into_es
from constants import STANDARD_FAANG
from typing import Dict, Set, List

SCRIPT_NAME = 'fetch_article'

ARTICLE_MAPPING = {
    'pmcId': 'pmcid',
    'pubmedId': 'pmid',
    'doi': 'doi',
    'title': 'title',
    'authorString': 'authorString',
    'journal': 'journalTitle',
    'issue': 'issue',
    'volume': 'journalVolume',
    'year': 'pubYear',
    'pages': 'pageInfo',
    'isOpenAccess': 'isOpenAccess',
    'source': 'source'
}
ARTICLE_BASIC_FIELDS = {'title', 'year', 'journal', 'source'}

# read from k8s secrets
ES_USERNAME = os.getenv('es_user')
ES_PASSWORD = os.getenv('es_password')

ES_HOST = os.environ.get("ES_NODE")

es = None


def main():
    """
    Main function that will import publications for all entities
    :return:
    """

    global es

    es = Elasticsearch([ES_HOST],
                       connection_class=RequestsHttpConnection,
                       http_auth=(ES_USERNAME, ES_PASSWORD))

    print('Start fetching articles')

    print('Index: article')
    # get existing dataset (to work out articles in file and specimen and existing specimen to calculate organism
    datasets = get_record_details(es, 'dataset',
                                  ['standardMet', 'secondaryProject', 'species',
                                   'specimen.biosampleId', 'file.fileId'])

    specimens = get_record_details(es, 'specimen', ['organism.biosampleId'])
    # get existing articles
    existing_articles = get_record_ids(es, 'article', only_faang=False)

    print(f'The number of existing datasets: {str(len(datasets))}')
    print(f'The number of existing articles: {str(len(existing_articles))}')

    # detailed article information which is used to insert into article ES index, keys are article id
    article_details = dict()
    # basic article information which is used in related records, e.g. specimen, dataset etc, keys are article id
    article_basics = dict()
    # record article dataset relationship, keys are article id
    article_datasets: Dict[str, Set] = dict()
    # for progress purpose
    dataset_count = 0
    # one dataset could have multiple articles, keys are dataset id, same naming pattern for other record type
    article_for_datasets: Dict[str, Set] = dict()

    # for all datasets existing in the Elasticsearch, search for the publications based on the dataset accession
    for dataset_id in datasets.keys():
        # logging progress, not related to the main algorithm
        dataset_count = dataset_count + 1
        if dataset_count % 200 == 0:
            print(f'Processed {dataset_count} datasets')
        # get dataset related publication using europe PMC search API
        url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={dataset_id}&format=json"
        epmc_result = requests.get(url).json()
        epmc_hits = epmc_result['resultList']['result']

        manual_hits = list()
        # if article not found directly from EuropePMC, use the information annotated in the ENA
        if not epmc_hits:
            xref_results = get_article_from_xref(dataset_id)
            for xref_result in xref_results:
                url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={xref_result}&format=json"
                manual_result = requests.get(url).json()
                manual_hits.append(manual_result['resultList']['result'][0])

        for hit in epmc_hits + manual_hits:
            # determine the article id which will be used in ES, PMC id is preferred, because
            # 1) it has PMC prefix rather than a string of digits
            # 2) PMC guarantees open access, more likely to have dataset accession linked
            article_id = determine_article_id(hit)
            if len(article_id) == 0:
                print(f'Study {dataset_id} has related article without Identifier')
                continue
            # new article
            if article_id not in article_details:
                es_article = dict()
                for k, v in ARTICLE_MAPPING.items():
                    es_article = parse_field(es_article, hit, k, v)
                article_details[article_id] = es_article
                article_basic_info = dict()
                # the article information displayed in other entities
                article_basic_info['articleId'] = article_id
                for k in ARTICLE_BASIC_FIELDS:
                    article_basic_info = parse_field(article_basic_info, hit, k, ARTICLE_MAPPING[k])
                article_basics[article_id] = article_basic_info

            article_datasets.setdefault(article_id, set())
            article_datasets[article_id].add(dataset_id)
            article_for_datasets.setdefault(dataset_id, set())
            article_for_datasets[dataset_id].add(article_id)

    print(f'Retrieved {len(article_details)} articles from all datasets')

    # deal with article index
    for article_id in article_details:
        es_article = article_details[article_id]
        all_faang_datasets_flag = 'FAANG only'
        es_datasets = list()
        secondary_projects = set()
        for dataset_id in article_datasets[article_id]:
            tmp = dict()
            tmp['accession'] = dataset_id
            tmp['standardMet'] = datasets[dataset_id]['standardMet']
            if tmp['standardMet'] != STANDARD_FAANG:
                all_faang_datasets_flag = "All"
            species = list()
            for tmp_sp in datasets[dataset_id]['species']:
                species.append(tmp_sp['text'])
            tmp['species'] = species
            if 'secondaryProject' in datasets[dataset_id]:
                for tmp_2nd_project in datasets[dataset_id]['secondaryProject']:
                    secondary_projects.add(tmp_2nd_project)
            es_datasets.append(tmp)
        es_article['relatedDatasets'] = es_datasets
        es_article['datasetSource'] = all_faang_datasets_flag
        es_article['secondaryProject'] = list(secondary_projects)
        insert_into_es(es, 'article', article_id, es_article)
        if article_id in existing_articles:
            existing_articles.remove(article_id)

    for not_needed_article_id in existing_articles:
        es.delete(index='article', doc_type="_doc", id=not_needed_article_id)
        print(f"Deleted {not_needed_article_id} from article index")


    print('Update articles within dataset index')
    update_article_info(article_basics, article_for_datasets, 'dataset')

    print('Update articles within specimen index')
    # update specimen, 'specimen' 'biosampleId' are referenced to the parameters used in datasets = get_record_details
    article_for_specimens: Dict[str, Set] = extract_article_from_related_entity(datasets, article_for_datasets,
                                                                                'specimen', 'biosampleId')
    specimen_with_publications = get_records_with_publications(es, 'specimen')
    print('Start to update the specimen ES')
    update_article_info(article_basics, article_for_specimens, 'specimen', specimen_with_publications)

    print('Update articles within file index')
    article_for_files: Dict[str, Set] = extract_article_from_related_entity(datasets, article_for_datasets,
                                                                            'file', 'fileId')
    file_with_publications = get_records_with_publications(es, 'file')
    print('Start to update the file ES')
    update_article_info(article_basics, article_for_files, 'file', file_with_publications)

    print('Update articles within organism index')
    article_for_organisms: Dict[str, Set] = extract_article_from_related_entity(specimens, article_for_specimens,
                                                                                'organism', 'biosampleId')
    organism_with_publications = get_records_with_publications(es, 'organism')
    print('Start to update the organism ES')
    update_article_info(article_basics, article_for_organisms, 'organism',
                        organism_with_publications)

    print('Finishing importing article')



def extract_article_from_related_entity(source_data, source_article_data,
                                        relationship_key, relationship_secondary_key=''):
    """
    Different types of records are linked, e.g. specimen to organism
    This function assigns the article information from the related record to the target record ,
    e.g. article for specimen (target) from dataset (source / related)
    :param source_data: the search result from ES for the related record
    :param source_article_data: the article information for the related record
    :param relationship_key: the ES field/section name
    :param relationship_secondary_key: the field name in the section if needed,
    e.g. biosampleId in organism section in specimen ES
    :return: articles for the target record
    """
    result: Dict[str, Set] = dict()
    for source_id in source_data:
        # only deal with source data having articles
        if source_id in source_article_data and source_id in source_data and relationship_key in source_data[source_id]:
            # unify to be list, in ES, it is only defined as keyword, could be list or string, files in dataset is list
            # while organism in specimen is a string
            tmp = list()
            if type(source_data[source_id][relationship_key]) == list:
                tmp = source_data[source_id][relationship_key]
            else:
                tmp.append(source_data[source_id][relationship_key])
            for record in tmp:
                if len(relationship_secondary_key) == 0:
                    target_id = record
                else:
                    target_id = record[relationship_secondary_key]
                result.setdefault(target_id, set())
                for article_id in source_article_data[source_id]:
                    result[target_id].add(article_id)
    return result


def update_article_info(article_basics, article_for_others, record_type,
                        records_with_publication=None) -> None:
    """
    update article information in other Elasticsearch indices
    :param article_basics: the collection of article information which is displayed in the other type record detail page
    :param article_for_others: the list of records need to be updated with the articles
    :param record_type: the type of the record
    :param records_with_publication: the existing publication information within the records, optional
    :return:
    """
    for record_id in article_for_others:
        need_update = False

        # Check if record exist first before updating
        # For example, not all bioSampleIds in dataset['specimen'] exist in the specimen index
        record_exists = es.search(index=record_type, q=f'_id:{record_id}', track_total_hits=True)
        if int(record_exists['hits']['total']['value']) == 0:
            print(f"{record_id} does not exist in index {record_type}")
            continue

        # compare the articles already linked to the record with the newly calculated one
        # if they are identical, that record does not need to be updated for article information
        if records_with_publication and record_id in records_with_publication:
            if 'publishedArticles' in records_with_publication[record_id]:
                existing = set()

                for article in records_with_publication[record_id]['publishedArticles']:
                    if 'articleId' in article:
                        existing.add(article['articleId'])

                for article_id in article_for_others[record_id]:
                    if article_id in existing:
                        existing.remove(article_id)
                    else:
                        need_update = True
                        break
                if len(existing) != 0:
                    need_update = True
            else:
                need_update = True

        # if no existing data provided or record id not in the existing data
        else:
            need_update = True

        if need_update:
            print(f"updating {record_id} in index {record_type}")
            publications: List = list()
            preprints: List = list()
            for article_id in article_for_others[record_id]:
                article_source = article_basics[article_id]['source']

                # no need to store source in ES
                updated_article_basics = copy.deepcopy(article_basics)
                updated_article_basics[article_id].pop('source', None)

                if article_source == 'PPR':
                    preprints.append(updated_article_basics[article_id])
                else:
                    publications.append(updated_article_basics[article_id])

            body = {}

            if publications:
                body.update({'paperPublished': "true"})
                body.update({'publishedArticles': publications})

            if preprints:
                body.update({'preprintArticles': preprints})
                print(f"Index: {record_type} -- Record Id: {record_id} -- PREPRINT Article: {article_for_others[record_id]}")


            try:
                es.update(index=record_type, id=record_id, doc=body)

            except Exception as e:
                print(f"Error when try to update {record_id} in index {record_type}: " + str(e.args))
                continue

def get_records_with_publications(es_host: str, record_type: str):
    """
    Retrieve only records of specified type having publications
    :param es_host: the Elastic Search server address
    :param record_type: the type of the records
    :return: the records with publications
    """
    records = get_record_details(es_host, record_type, ['paperPublished', 'publishedArticles'])
    results = dict()
    for record_id in records.keys():
        if 'paperPublished' in records[record_id]:
            published = records[record_id]['paperPublished']
            if published == 'true' or published == 'yes':
                results[record_id] = records[record_id]
    return results


def parse_field(es_article, hit, es_key, api_key):
    """
    update the record with the value in the hit, if the key not found, then the record does not change
    :param es_article: the record data which will be inserted into ElasticSearch
    :param hit: the input data
    :param es_key: the key in the elastic search record
    :param api_key: the key to search the input data
    :return: the updated record
    """
    if api_key in hit:
        es_article[es_key] = hit[api_key]
    return es_article


def determine_article_id(epmc_hit):
    """
    Determine the article id to be used in ElasticSearch from the ePMC record
    The priority is pmcid (Pubmed Central id which provides open access), pmid (pubmed id), doi and id used by ePMC
    If none of those values is available, return empty string, a sign of error to be dealt with in the caller
    :param epmc_hit: the ePMC record from their API
    :return: the determined id
    """
    if 'pmcid' in epmc_hit:
        return epmc_hit['pmcid']
    elif 'pmid' in epmc_hit:
        return epmc_hit['pmid']
    elif 'doi' in epmc_hit:
        return epmc_hit['doi'].replace('/', '_')
    elif 'id' in epmc_hit:
        return epmc_hit['id']
    else:
        return ""


def get_article_from_xref(study_accession: str):
    url = f'https://www.ebi.ac.uk/ena/xref/rest/json/search?accession={study_accession}'
    results = list()
    query_results = requests.get(url).json()
    for result in query_results:
        if result['Source'] == 'PubMed' or result['Source'] == 'EuropePMC':
            results.append(result['Source Primary Accession'])
    return results


if __name__ == '__main__':
    main()
    print("END - fetch_articles.py script has finished loading!")
