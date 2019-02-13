import os
from elasticsearch import Elasticsearch
from datetime import date, timedelta
import threading

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
    threads = []

    t1 = threading.Thread(target=create_snapshot, args=(es_staging, snapshot_name,))
    threads.append(t1)
    t2 = threading.Thread(target=rsync_snapshot)
    threads.append(t2)
    t3 = threading.Thread(target=restore_snapshot, args=(es_fallback, es_production, today, snapshot_name,))
    threads.append(t3)
    t4 = threading.Thread(target=change_aliases, args=(es_fallback, es_production, today, yesterday,))
    threads.append(t4)
    t5 = threading.Thread(target=delete_old_indices, args=(es_fallback, es_production, yesterday))
    threads.append(t5)

    for thread in threads:
        thread.start()
        thread.join()


def create_snapshot(es_staging, snapshot_name):
    print("Creating snapshot...")
    parameters = {"indices": "file3,organism3,specimen3,dataset3,experiment3,protocol_files3,protocol_samples3",
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
        "indices": "file3,organism3,specimen3,dataset3,experiment3,protocol_files3,protocol_samples3",
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
        {"remove": {"index": "protocol_files-{}3".format(yesterday), "alias": "protocol_files"}},
        {"add": {"index": "protocol_files-{}3".format(today), "alias": "protocol_files"}},
        {"remove": {"index": "protocol_samples-{}3".format(yesterday), "alias": "protocol_samples"}},
        {"add": {"index": "protocol_samples-{}3".format(today), "alias": "protocol_samples"}},
    ]
    }
    es_fallback.indices.update_aliases(body=actions)
    es_production.indices.update_aliases(body=actions)


def delete_old_indices(es_fallback, es_production, yesterday):
    print("Deleting old indices...")
    es_fallback.indices.delete(index="file-{}3,organism-{}3,specimen-{}3,dataset-{}3,experiment-{}3,protocol_files-{}3,protocol_samples-{}3".format(yesterday, yesterday, yesterday, yesterday, yesterday))
    es_production.indices.delete(index="file-{}3,organism-{}3,specimen-{}3,dataset-{}3,experiment-{}3,protocol_files-{}3,protocol_samples-{}3".format(yesterday, yesterday, yesterday, yesterday, yesterday))


if __name__ == "__main__":
    main()
