from elasticsearch import Elasticsearch
from datetime import date, timedelta
import os

from utils import *
from constants import *


class SyncHinxtonLondon:
    """
    This class will create backup for elasticsearch using following procedure:
    1. Create snapshot on test server
    2. Sync snapshot files to fallback and production servers
    3. Restore from snapshot on fallback and production servers
    4. Change aliases to point to restored snapshot on fallback and production
    5. Delete old indices on fallback and production servers
    It is essential to read https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-restore.html
    if you have no experience about snapshot
    """
    def __init__(self, es_staging, es_fallback, es_production, logger):
        """
        This function will assign es objects for each particular server
        :param es_staging: object for test server of elasticsearch
        :param es_fallback: object for fallback server of elasticsearch
        :param es_production: object for production server of elasticsearch
        :param logger: logger object to log into
        :param today: current date
        :param yesterday: yesterday date
        :param snapshot_name: name of snapshot (timestamp with current date)
        """
        self.es_staging = es_staging
        self.es_fallback = es_fallback
        self.es_production = es_production
        self.logger = logger
        self.today = date.today().strftime('%Y-%m-%d')
        self.yesterday = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
        self.snapshot_name = "snapshot_{}".format(self.today)
        self.from_path = FROM
        self.to_path = TO
        self.es_server_production = 'wp-p1m-e2'

    def run_sync(self):
        """
        Main function that will run syncing
        """
        self.create_snapshot('es6_faang_repo')
        self.rsync_snapshot()
        self.restore_snapshot()
        self.change_aliases()
        self.delete_old_indices()

    def create_snapshot(self, rep_name):
        """
        This function will create snapshot on test server
        :param rep_name name of the snapshot repository
        """
        self.logger.info('Creating snapshot')
        indices = ",".join(ALIASES_IN_USE.values())
        # indices defines specific indices to be backed up
        parameters = {
            "indices": indices,
            "ignore_unavailable": True,
            "include_global_state": False
        }

        self.es_staging.snapshot.create(
            repository=rep_name, snapshot=self.snapshot_name,
            body=parameters, wait_for_completion=True)

    def rsync_snapshot(self):
        os.system("rsync --archive --delete-during {} {}".format(
            self.from_path, self.to_path))
        os.system("rsync --archive --delete-during {} {}:{}".format(
            self.from_path, self.es_server_production, self.to_path))

    def restore_snapshot(self):
        """
        This function will restore snapshot on fallback and production servers
        https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshots-restore-snapshot.html
        which explains how to use renaming pattern etc. parameters
        """
        self.logger.info('Restoring snapshot')
        indices_in_use = "|".join(ALIASES_IN_USE.keys())
        parameters = {
            "ignore_unavailable": True,
            "include_aliases": False,
            "rename_pattern": "({})".format(indices_in_use),
            "rename_replacement": "{}_$1".format(self.today)
        }
        self.es_fallback.snapshot.restore(
            repository='es6_faang_repo_production',
            snapshot=self.snapshot_name, body=parameters)
        self.es_production.snapshot.restore(
            repository='es6_faang_repo_production',
            snapshot=self.snapshot_name, body=parameters)

    def change_aliases(self):
        """
        This function will change aliases to poing to new indices on production
        and fallback servers
        """
        self.logger.info('Changing aliases')
        actions = list()
        for k, v in ALIASES_IN_USE.items():
            actions.append({"remove": {"index": "{}_{}".format(
                self.yesterday, k), "alias": "{}".format(v)}})
            actions.append({"add": {"index": "{}_{}".format(self.today, k),
                                    "alias": "{}".format(v)}})
        body = {"actions": actions}
        self.es_fallback.indices.update_aliases(body=body)
        self.es_production.indices.update_aliases(body=body)

    def delete_old_indices(self):
        """
        This function will delete old indices from fallback and production
        directories
        """
        self.logger.info('Deleting old indices')
        indices_to_delete = ",".join(
            ["{}_{}".format(self.yesterday, k) for k in ALIASES_IN_USE.keys()])
        self.es_fallback.indices.delete(index=indices_to_delete)
        self.es_production.indices.delete(index=indices_to_delete)


if __name__ == "__main__":
    # Create elasticsearch objects for each server
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2], timeout=120)
    es_fallback = Elasticsearch([FALLBACK_NODE1, FALLBACK_NODE2], timeout=120)
    es_production = Elasticsearch([PRODUCTION_NODE1, PRODUCTION_NODE2],
                                  timeout=120)

    # Create logger to log info
    logger = create_logging_instance('sync_hx_hh')

    # Create object and run syncing
    sync_object = SyncHinxtonLondon(es_staging, es_fallback, es_production,
                                    logger)
    sync_object.run_sync()
