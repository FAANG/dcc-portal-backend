import requests
from elasticsearch import Elasticsearch

# Addresses of servers
NODE1 = 'wp-np3-e2:9200'
NODE2 = 'wp-np3-e3:9200'


def main():
    es = Elasticsearch([NODE1, NODE2])
    organisms_id = retrieve_ids('organism', es)
    specimens_id = retrieve_ids('specimen', es)
    datasets_id = retrieve_ids('dataset', es)

    print("Starting to fetch articles for organisms")
    fetch_articles(organisms_id, es)

    print("Starting to fetch articles for specimens")
    fetch_articles(specimens_id, es)

    print("Starting to fetch articles for datasets")
    fetch_articles(datasets_id, es)


def retrieve_ids(index_name, es):
    print("Fetching ids from {}".format(index_name))
    ids = []
    data = es.search(index=index_name, size=100000, _source="_id")
    for hit in data['hits']['hits']:
        ids.append(hit['_id'])
    print("Finishing fetch of ids from {}".format(index_name))
    return ids


def fetch_articles(ids, es):
    for index, my_id in enumerate(ids):
        if index % 100 == 0:
            print(index)
        response = requests.get("https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={}&format=json"
                                .format(my_id))
        results = response.json()['resultList']['result']
        if len(results) != 0:
            print(results)


if __name__ == "__main__":
    main()
