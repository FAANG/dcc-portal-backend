from elasticsearch import Elasticsearch
import logging
import pprint

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s\t%(levelname)s:\t%(name)s line %(lineno)s\t%(message)s', level=logging.INFO)
# suppress logging information from elasticsearch
logging.getLogger('elasticsearch').setLevel(logging.WARNING)


def insert_into_es(es, es_index_prefix, doc_type, id, body):
    try:
        existing_flag = es.exists(index=f'{es_index_prefix}{doc_type}', doc_type="_doc", id=id)
        if existing_flag:
            es.delete(index=f'{es_index_prefix}{doc_type}', doc_type="_doc", id=id)
        es.create(index=f'{es_index_prefix}{doc_type}', doc_type="_doc", id=id, body=body)
    except Exception as e:
        # TODO logging error
        logger.error(f"Error when try to insert into index {es_index_prefix}{doc_type}: " + str(e.args))
        pprint.pprint(body)
