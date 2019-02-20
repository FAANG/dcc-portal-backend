import requests
from elasticsearch import Elasticsearch
import asyncio
import aiohttp
import time

# Global sets for ids
ORGANISMS = set()
SPECIMENS = set()
DATASETS = set()
FILES = set()
ARTICLES = []


def main():
    es = Elasticsearch([NODE1, NODE2])
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
                task = asyncio.ensure_future(check_specimen_for_organism(session, my_id))
            elif my_type == 'check_specimen_for_dataset':
                task = asyncio.ensure_future(check_specimen_for_dataset(session, my_id))
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
            if my_type == 'organism':
                ORGANISMS.add(my_id)

                # Add specimens
                response = requests.get(
                    "http://data.faang.org/api/specimen/_search/?q=organism.biosampleId:{}&size=100000".format(my_id)
                ).json()
                for item in response['hits']['hits']:
                    SPECIMENS.add(item['_id'])

                # Add files
                response = requests.get(
                    "http://data.faang.org/api/file/_search/?q=organism:{}&size=100000".format(my_id)).json()
                for item in response['hits']['hits']:
                    FILES.add(item['_id'])

            elif my_type == 'specimen':
                SPECIMENS.add(my_id)

                # Add organisms
                response = requests.get("http://data.faang.org/api/specimen/{}".format(my_id)).json()
                if 'biosampleId' in response['hits']['hits'][0]['_source']['organism']:
                    organism_id = response['hits']['hits'][0]['_source']['organism']['biosampleId']
                    ORGANISMS.add(organism_id)

                # Add datasets
                response = requests.get(
                    "http://data.faang.org/api/dataset/_search/?q=specimen.biosampleId:{}&size=100000".format(my_id)
                ).json()
                for item in response['hits']['hits']:
                    DATASETS.add(item['_id'])

                # Add files
                response = requests.get(
                    "http://data.faang.org/api/file/_search/?q=specimen:{}&size=100000".format(my_id)).json()
                for item in response['hits']['hits']:
                    FILES.add(item['_id'])

            elif my_type == 'dataset':
                DATASETS.add(my_id)

                # Add specimens
                response = requests.get("http://data.faang.org/api/dataset/{}".format(my_id)).json()
                for item in response['hits']['hits'][0]['_source']['specimen']:
                    SPECIMENS.add(item['biosampleId'])

                # Add files
                for item in response['hits']['hits'][0]['_source']['file']:
                    FILES.add(item['fileId'])


async def check_specimen_for_organism(session, my_id):
    url = "http://data.faang.org/api/specimen/{}".format(my_id)
    async with session.get(url) as response:
        results = await response.json()
        if 'biosampleId' in results['hits']['hits'][0]['_source']['organism']:
            organism_id = results['hits']['hits'][0]['_source']['organism']['biosampleId']
            ORGANISMS.add(organism_id)


async def check_specimen_for_dataset(session, my_id):
    url = "http://data.faang.org/api/dataset/_search/?q=specimen.biosampleId:{}&size=100000".format(my_id)
    async with session.get(url) as response:
        results = await response.json()
        for item in results['hits']['hits']:
            DATASETS.add(item['_id'])


def update_records(records_array, array_type):
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    body = {"doc": {"paperPublished": "true"}}

    print("Starting to update {} records:".format(array_type))
    for index, item_id in enumerate(records_array):
        if index % 100 == 0:
            ratio = round(index / len(records_array) * 100)
            print(f"{ratio} % is ready...")
        try:
            es.update(index=array_type, doc_type="_doc", id=item_id, body=body)
        except ValueError:
            print("ValueError {}".format(item_id))
            continue


if __name__ == "__main__":
    start_time = time.time()

    main()
    update_records(SPECIMENS, 'specimen')
    update_records(ORGANISMS, 'organism')
    update_records(DATASETS, 'dataset')
    update_records(FILES, 'file')

    duration = time.time() - start_time
    print(f"Done in {round(duration / 60)} minutes")
