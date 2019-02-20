import requests
from elasticsearch import Elasticsearch
import asyncio
import aiohttp
import time

# Global sets for ids
ORGANISMS = {}
SPECIMENS = {}
DATASETS = {}
FILES = {}
ARTICLES = {}


def main(es):
    organisms_id = retrieve_ids('organism', es)
    specimens_id = retrieve_ids('specimen', es)
    datasets_id = retrieve_ids('dataset', es)

    print("Starting to fetch articles for organisms...")
    asyncio.get_event_loop().run_until_complete(fetch_all_articles(organisms_id, 'organism'))

    print("Starting to fetch articles for specimens...")
    asyncio.get_event_loop().run_until_complete(fetch_all_articles(specimens_id, 'specimen'))

    print("Starting to fetch articles for datasets..")
    asyncio.get_event_loop().run_until_complete(fetch_all_articles(datasets_id, 'dataset'))

    print("Starting to check specimens for additional organisms...")
    asyncio.get_event_loop().run_until_complete(fetch_all_articles(SPECIMENS, 'check_specimen_for_organism'))

    print("Starting to check specimens for additional datasets...")
    asyncio.get_event_loop().run_until_complete(fetch_all_articles(SPECIMENS, 'check_specimen_for_dataset'))


def retrieve_ids(index_name, es):
    print("Fetching ids from {}...".format(index_name))
    ids = []
    data = es.search(index=index_name, size=100000, _source="_id")
    for hit in data['hits']['hits']:
        ids.append(hit['_id'])
    return ids


async def fetch_all_articles(ids, my_type):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for my_id in ids:
            if my_type == 'check_specimen_for_organism':
                task = asyncio.ensure_future(check_specimen_for_organism(session, my_id, ids[my_id]))
            elif my_type == 'check_specimen_for_dataset':
                task = asyncio.ensure_future(check_specimen_for_dataset(session, my_id, ids[my_id]))
            else:
                task = asyncio.ensure_future(fetch_article(session, my_id, my_type))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)


async def fetch_article(session, my_id, my_type):
    url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={}&format=json".format(my_id)
    async with session.get(url) as response:
        results = await response.json()
        results = results['resultList']['result']
        if len(results) != 0:
            articles_list = [result['pmcid'] for result in results]
            if my_type == 'organism':
                add_new_pair(ORGANISMS, my_id, articles_list)

                # Add specimens
                response = requests.get(
                    "http://data.faang.org/api/specimen/_search/?q=organism.biosampleId:{}&size=100000".format(my_id)
                ).json()
                for item in response['hits']['hits']:
                    add_new_pair(SPECIMENS, item['_id'], articles_list)

                # Add files
                response = requests.get(
                    "http://data.faang.org/api/file/_search/?q=organism:{}&size=100000".format(my_id)).json()
                for item in response['hits']['hits']:
                    # FILES.add(item['_id'])
                    add_new_pair(FILES, item['_id'], articles_list)

            elif my_type == 'specimen':
                add_new_pair(SPECIMENS, my_id, articles_list)

                # Add organisms
                response = requests.get("http://data.faang.org/api/specimen/{}".format(my_id)).json()
                if 'biosampleId' in response['hits']['hits'][0]['_source']['organism']:
                    organism_id = response['hits']['hits'][0]['_source']['organism']['biosampleId']
                    add_new_pair(ORGANISMS, organism_id, articles_list)

                # Add datasets
                response = requests.get(
                    "http://data.faang.org/api/dataset/_search/?q=specimen.biosampleId:{}&size=100000".format(my_id)
                ).json()
                for item in response['hits']['hits']:
                    add_new_pair(DATASETS, item['_id'], articles_list)

                # Add files
                response = requests.get(
                    "http://data.faang.org/api/file/_search/?q=specimen:{}&size=100000".format(my_id)).json()
                for item in response['hits']['hits']:
                    add_new_pair(FILES, item['_id'], articles_list)

            elif my_type == 'dataset':
                add_new_pair(DATASETS, my_id, articles_list)

                # Add specimens
                response = requests.get("http://data.faang.org/api/dataset/{}".format(my_id)).json()
                for item in response['hits']['hits'][0]['_source']['specimen']:
                    add_new_pair(SPECIMENS, item['biosampleId'], articles_list)

                # Add files
                for item in response['hits']['hits'][0]['_source']['file']:
                    add_new_pair(FILES, item['fileId'], articles_list)


async def check_specimen_for_organism(session, my_id, articles_list):
    url = "http://data.faang.org/api/specimen/{}".format(my_id)
    async with session.get(url) as response:
        results = await response.json()
        if 'biosampleId' in results['hits']['hits'][0]['_source']['organism']:
            organism_id = results['hits']['hits'][0]['_source']['organism']['biosampleId']
            add_new_pair(ORGANISMS, organism_id, articles_list)


async def check_specimen_for_dataset(session, my_id, articles_list):
    url = "http://data.faang.org/api/dataset/_search/?q=specimen.biosampleId:{}&size=100000".format(my_id)
    async with session.get(url) as response:
        results = await response.json()
        for item in results['hits']['hits']:
            add_new_pair(DATASETS, item['_id'], articles_list)


def update_records(records_dict, array_type, es):

    print("Starting to update {} records:".format(array_type))
    for index, item_id in enumerate(records_dict):
        if index % 100 == 0:
            ratio = round(index / len(records_dict) * 100)
            print(f"{ratio} % is ready...")
        body = {"doc": {"paperPublished": "true", "publishedArticles": [
            {'pubmedId': pubmedId} for pubmedId in records_dict[item_id]]}}

        try:
            es.update(index=array_type, doc_type="_doc", id=item_id, body=body)
        except ValueError:
            print("ValueError {}".format(item_id))
            continue


def add_new_pair(target_dict, id_to_check, target_list):
    if id_to_check not in target_dict:
        target_dict[id_to_check] = set(target_list)
    else:
        target_dict[id_to_check].update(target_list)


if __name__ == "__main__":
    start_time = time.time()

    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    main(es)
    update_records(SPECIMENS, 'specimen', es)
    update_records(ORGANISMS, 'organism', es)
    update_records(DATASETS, 'dataset', es)
    update_records(FILES, 'file', es)

    duration = time.time() - start_time
    print(f"Done in {round(duration / 60)} minutes")
