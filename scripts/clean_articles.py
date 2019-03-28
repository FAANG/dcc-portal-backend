from elasticsearch import Elasticsearch

from fetch_articles import *


def main(index, es):
    ids = retrieve_ids(index, es)
    clean_records(index, ids, es)


def clean_records(index, ids, es):
    for id in ids:
        body = {"doc": {"paperPublished": "false", "publishedArticles": []}}
        es.update(index=index, doc_type="_doc", id=id, body=body)


if __name__ == "__main__":
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    index = 'dataset'
    main(index, es)
