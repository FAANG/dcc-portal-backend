import os
from elasticsearch import Elasticsearch
from datetime import date, timedelta
import time

# Addresses of servers
STAGING_NODE1 = 'wp-np3-e2:9200'
STAGING_NODE2 = 'wp-np3-e3:9200'
FALLBACK_NODE1 = 'wp-p2m-e2:9200'
FALLBACK_NODE2 = 'wp-p2m-e3:9200'
PRODUCTION_NODE1 = 'wp-p1m-e2:9200'
PRODUCTION_NODE2 = 'wp-p1m-e3:9200'

# Paths for rsync command
FROM = '/nfs/public/rw/reseq-info/elastic_search_staging/snapshot_repo/es6_faang_repo/'
TO = '/nfs/public/rw/reseq-info/elastic_search/snapshot_repo/es6_faang_repo/'


def main():
    # Define connection to 3 nodes
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])
    es_fallback = Elasticsearch([FALLBACK_NODE1, FALLBACK_NODE2])
    es_production = Elasticsearch([PRODUCTION_NODE1, PRODUCTION_NODE2])

    # Get current date and snapshot name
    today = date.today().strftime('%Y-%m-%d')
    yesterday = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    snapshot_name = "snapshot_{}".format(today)

    # Do all the job
    create_snapshot(es_staging, snapshot_name)
    rsync_snapshot()
    time.sleep(5)
    restore_snapshot(es_fallback, es_production, today, snapshot_name)
    change_aliases(es_fallback, es_production, today, yesterday)
    delete_old_indices(es_fallback, es_production, yesterday)


def create_snapshot(es_staging, snapshot_name):
    print("Creating snapshot...")
    parameters = {"indices": "file3,organism3,specimen3,dataset3,experiment3",
                  "ignore_unavailable": True,
                  "include_global_state": False
                  }
    es_staging.snapshot.create(repository='es6_faang_repo', snapshot=snapshot_name, body=parameters)


def rsync_snapshot():
    print("Rsyncing snapshot...")
    os.system("rsync --archive --delete-during {} {}".format(FROM, TO))


def restore_snapshot(es_fallback, es_production, today, snapshot_name):
    print("Restoring snapshot...")
    parameters = {
        "indices": "file3,organism3,specimen3,dataset3,experiment3",
        "ignore_unavailable": True,
        "include_aliases": False,
        "rename_pattern": "([a-z]+)",
        "rename_replacement": "$1-{}".format(today)
    }
    es_fallback.snapshot.restore(repository='es6_faang_repo', snapshot=snapshot_name, body=parameters)
    es_production.snapshot.restore(repository='es6_faang_repo', snapshot=snapshot_name, body=parameters)


def change_aliases(es_fallback, es_production, today, yesterday):
    print("Changing aliases...")
    actions = {"actions": [
        {"remove": {"index": "file-{}3".format(yesterday), "alias": "file"}},
        {"add": {"index": "file-{}3".format(today), "alias": "file"}},
        {"remove": {"index": "organism-{}3".format(yesterday), "alias": "organism"}},
        {"add": {"index": "organism-{}3".format(today), "alias": "organism"}},
        {"remove": {"index": "specimen-{}3".format(yesterday), "alias": "specimen"}},
        {"add": {"index": "specimen-{}3".format(today), "alias": "specimen"}},
        {"remove": {"index": "dataset-{}3".format(yesterday), "alias": "dataset"}},
        {"add": {"index": "dataset-{}3".format(today), "alias": "dataset"}},
        {"remove": {"index": "experiment-{}3".format(yesterday), "alias": "experiment"}},
        {"add": {"index": "experiment-{}3".format(today), "alias": "experiment"}},
    ]
    }
    es_fallback.indices.update_aliases(body=actions)
    es_production.indices.update_aliases(body=actions)


def delete_old_indices(es_fallback, es_production, yesterday):
    print("Deleting old indices...")
    es_fallback.indices.delete(index="file-{}3,organism-{}3,specimen-{}3,dataset-{}3,experiment-{}3".format(yesterday,
                                                                                                            yesterday,
                                                                                                            yesterday,
                                                                                                            yesterday,
                                                                                                            yesterday))
    es_production.indices.delete(index="file-{}3,organism-{}3,specimen-{}3,dataset-{}3,experiment-{}3".format(yesterday,
                                                                                                              yesterday,
                                                                                                              yesterday,
                                                                                                              yesterday,
                                                                                                              yesterday))


if __name__ == "__main__":
    main()
