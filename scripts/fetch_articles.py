import requests
from elasticsearch import Elasticsearch
import click
from utils import create_logging_instance, remove_underscore_from_end_prefix, get_record_ids, insert_into_es
from constants import STAGING_NODE1, DEFAULT_PREFIX
from typing import Dict, Set
import pprint
logger = create_logging_instance('fetch_articles', to_file=False)


@click.command()
@click.option(
    '--es_hosts',
    default=STAGING_NODE1,
    help='Specify the Elastic Search server(s) (port could be included), e.g. wp-np3-e2:9200. '
         'If multiple servers are provided, please use ";" to separate them, e.g. "wp-np3-e2;wp-np3-e3"'
)
@click.option(
    '--es_index_prefix',
    default=DEFAULT_PREFIX,
    help='Specify the Elastic Search index prefix, e.g. '
         'faang_build_1 then the index to work on will be faang_build_1_article.'
)
def main(es_hosts, es_index_prefix):
    hosts = es_hosts.split(";")
    logger.info("Command line parameters")
    logger.info("Hosts: "+str(hosts))

    es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
    logger.info(f"Index: {es_index_prefix}_article")
    datasets = get_record_ids(hosts[0], es_index_prefix, 'dataset', only_faang=False)
    existing_articles = get_record_ids(hosts[0], es_index_prefix, 'article', only_faang=False)
    logger.info("The number of existing datasets: " + str(len(datasets)))
    logger.info("The number of existing articles: " + str(len(existing_articles)))
    article_details = dict()
    article_datasets: Dict[str, Set] = dict()
    for dataset_id in datasets:
        url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={dataset_id}&format=json"
        epmc_result = requests.get(url).json()
        hit_count = epmc_result['hitCount']
        if hit_count != 0:
            epmc_hits = epmc_result['resultList']['result']
            for hit in epmc_hits:
                # ignore preprints determined by two fields pubType and source
                if 'pubType' in hit and hit['pubType'] == 'preprint':
                    continue
                if 'source' in hit and hit['source'] == 'PPR':
                    continue
                article_id = determine_article_id(hit)
                if len(article_id) == 0:
                    logger.error(f"Study {dataset_id} has related article without Identifier")
                    continue
                # new article
                if article_id not in article_details:
                    es_article = dict()
                    es_article = parse_field(es_article, hit, 'pmcId', 'pmcid')
                    es_article = parse_field(es_article, hit, 'pubmedId', 'pmid')
                    es_article = parse_field(es_article, hit, 'doi', 'doi')
                    es_article = parse_field(es_article, hit, 'title', 'title')
                    es_article = parse_field(es_article, hit, 'authorString', 'authorString')
                    es_article = parse_field(es_article, hit, 'journal', 'journalTitle')
                    es_article = parse_field(es_article, hit, 'issue', 'issue')
                    es_article = parse_field(es_article, hit, 'volume', 'journalVolume')
                    es_article = parse_field(es_article, hit, 'year', 'pubYear')
                    es_article = parse_field(es_article, hit, 'pages', 'pageInfo')
                    es_article = parse_field(es_article, hit, 'isOpenAccess', 'isOpenAccess')
                    article_details[article_id] = es_article

                article_datasets.setdefault(article_id, set())
                article_datasets[article_id].add(dataset_id)

    logger.info(f'Retrieved {len(article_details.keys())} articles')
    es = Elasticsearch(hosts)
    for article_id in article_details.keys():
        es_article = article_details[article_id]
        datasets = article_datasets[article_id]
        es_article['relatedDatasets'] = list(datasets)
        insert_into_es(es, es_index_prefix, 'article', article_id, es_article)
        if article_id in existing_articles:
            existing_articles.remove(article_id)

    for not_needed_article_id in existing_articles:
        es.delete(index=f'{es_index_prefix}_article', doc_type="_doc", id=not_needed_article_id)

    logger.info("Finishing importing article")


def parse_field(es_article, hit, es_key, api_key):
    if api_key in hit:
        es_article[es_key] = hit[api_key]
    return es_article


def determine_article_id(epmc_hit):
    '''
    Determine the article id to be used in ElasticSearch from the ePMC record
    The priority is pmcid (Pubmed Central id which provides open access), pmid (pubmed id), doi and id used by ePMC
    If none of those values is available, return empty string, a sign of error to be dealt with in the caller
    :param epmc_hit: the ePMC record from their API
    :return: the determined id
    '''
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


if __name__ == '__main__':
    main()
