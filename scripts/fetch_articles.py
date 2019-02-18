import requests
from elasticsearch import Elasticsearch

# Addresses of servers
NODE1 = 'wp-np3-e2:9200'
NODE2 = 'wp-np3-e3:9200'
ORGANISMS = set()
SPECIMENS = set()
DATASETS = set()
FILES = set()


def main():
    es = Elasticsearch([NODE1, NODE2])
    organisms_id = retrieve_ids('organism', es)
    specimens_id = retrieve_ids('specimen', es)
    datasets_id = retrieve_ids('dataset', es)

    print("Starting to fetch articles for organisms...")
    fetch_articles(organisms_id, 'organism')

    print("Starting to fetch articles for specimens...")
    fetch_articles(specimens_id, 'specimen')

    print("Starting to fetch articles for datasets..")
    fetch_articles(datasets_id, 'dataset')

    print("Starting to check specimens for additional organisms and datasets...")
    check_specimens()


def retrieve_ids(index_name, es):
    print("Fetching ids from {}...".format(index_name))
    ids = []
    data = es.search(index=index_name, size=100000, _source="_id")
    for hit in data['hits']['hits']:
        ids.append(hit['_id'])
    print("Finishing fetch of ids from {}".format(index_name))
    return ids


def fetch_articles(ids, my_type):
    for index, my_id in enumerate(ids):
        if index % 100 == 0:
            ratio = round(index / len(ids) * 100)
            print("{} % is ready...".format(str(ratio)))
        response = requests.get("https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={}&format=json"
                                .format(my_id))
        results = response.json()['resultList']['result']
        if len(results) != 0:
            print("{}\t{}".format(my_type, my_id))
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


def check_specimens():
    for index, my_id in enumerate(SPECIMENS):
        if index % 100 == 0:
            ratio = round(index / len(SPECIMENS) * 100)
            print("{} % is ready...".format(str(ratio)))
        # Add organisms
        response = requests.get("http://data.faang.org/api/specimen/{}".format(my_id)).json()
        if 'biosampleId' in response['hits']['hits'][0]['_source']['organism']:
            organism_id = response['hits']['hits'][0]['_source']['organism']['biosampleId']
            ORGANISMS.add(organism_id)

        # Add datasets
        response = requests.get(
            "http://data.faang.org/api/dataset/_search/?q=specimen.biosampleId:{}&size=100000".format(my_id)).json()
        for item in response['hits']['hits']:
            DATASETS.add(item['_id'])


def update_records(records_array, array_type):
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    body = {"doc": {"paperPublished": "true"}}

    print("Starting to update {} records:".format(array_type))
    for index, item_id in enumerate(records_array):
        if index % 100 == 0:
            ratio = index / len(records_array) * 100
            print("{} % is ready...".format(str(ratio)))
        try:
            es.update(index=array_type, doc_type="_doc", id=item_id, body=body)
        except ValueError:
            print("ValueError {}".format(item_id))
            continue


if __name__ == "__main__":
    main()
    update_records(SPECIMENS, 'specimen')
    update_records(ORGANISMS, 'organism')
    update_records(DATASETS, 'dataset')
    update_records(FILES, 'file')
    print("Done!")
