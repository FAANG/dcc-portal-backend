from elasticsearch import Elasticsearch
from datetime import date, timedelta
from dirsync import sync

from utils import *


class SyncHinxtonLondon:
    """
    This class will create backup for elasticsearch using following procedure:
    1. Create snapshot on test server
    2. Sync snapshot files to fallback and production servers
    3. Restore from snapshot on fallback and production servers
    4. Change aliases to point to restored snapshot on fallback and production servers
    5. Delete old indices on fallback and production servers
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

    def run_sync(self):
        """
        Main function that will run syncing
        """
        self.create_snapshot('staging')
        self.create_snapshot('production')
        self.restore_snapshot()
        self.change_aliases()
        self.delete_old_indices()

    def create_snapshot(self, rep_type):
        """
        This function will create snapshot on test server
        :param rep_type type of repo to use (staging or production)
        """
        self.logger.info('Creating snapshot')
        parameters = {"indices": "file,organism,specimen,dataset,experiment,protocol_files,protocol_samples",
                      "ignore_unavailable": True,
                      "include_global_state": False
                      }
        self.es_staging.snapshot.create(repository=rep_type, snapshot=self.snapshot_name, body=parameters)

    def restore_snapshot(self):
        """
        This function will restore snapshot on fallback and production servers
        """
        self.logger.info('Restoring snapshot')
        parameters = {
            "ignore_unavailable": True,
            "include_aliases": False,
            "rename_pattern": ".*((protocol_)?file(s)?|protocol_samples|organism|specimen|dataset|experiment).*",
            "rename_replacement": "{}_$1".format(self.today)
        }
        self.es_fallback.snapshot.restore(repository='es6_faang_repo', snapshot=self.snapshot_name, body=parameters)
        self.es_production.snapshot.restore(repository='es6_faang_repo', snapshot=self.snapshot_name, body=parameters)

    def change_aliases(self):
        """
        This function will change aliases to poing to new indices on production and fallback servers
        """
        self.logger.info('Changing aliases')
        actions = {"actions": [
            {"remove": {"index": "{}_file".format(self.yesterday), "alias": "file"}},
            {"add": {"index": "{}_file".format(self.today), "alias": "file"}},
            {"remove": {"index": "{}_organism".format(self.yesterday), "alias": "organism"}},
            {"add": {"index": "{}_organism".format(self.today), "alias": "organism"}},
            {"remove": {"index": "{}_specimen".format(self.yesterday), "alias": "specimen"}},
            {"add": {"index": "{}_specimen".format(self.today), "alias": "specimen"}},
            {"remove": {"index": "{}_dataset".format(self.yesterday), "alias": "dataset"}},
            {"add": {"index": "{}_dataset".format(self.today), "alias": "dataset"}},
            {"remove": {"index": "{}_experiment".format(self.yesterday), "alias": "experiment"}},
            {"add": {"index": "{}_experiment".format(self.today), "alias": "experiment"}},
            {"remove": {"index": "{}_files".format(self.yesterday), "alias": "protocol_files"}},
            {"add": {"index": "{}_files".format(self.today), "alias": "protocol_files"}},
            {"remove": {"index": "{}_protocol_samples".format(self.yesterday), "alias": "protocol_samples"}},
            {"add": {"index": "{}_protocol_samples".format(self.today), "alias": "protocol_samples"}}
        ]
        }
        self.es_fallback.indices.update_aliases(body=actions)
        self.es_production.indices.update_aliases(body=actions)

    def delete_old_indices(self):
        """
        This function will delete old indices from fallback and production directories
        """
        self.logger.info('Deleting old indices')
        self.es_fallback.indices.delete(index=("{}_file,{}_organism,{}_specimen,{}_dataset,{}_experiment," +
                                               "{}_files,{}_protocol_samples").format(self.yesterday, self.yesterday,
                                                                                      self.yesterday, self.yesterday,
                                                                                      self.yesterday, self.yesterday,
                                                                                      self.yesterday))
        self.es_production.indices.delete(index=("{}_file,{}_organism,{}_specimen,{}_dataset,{}_experiment," +
                                                 "{}_files,{}_protocol_samples").format(self.yesterday, self.yesterday,
                                                                                        self.yesterday, self.yesterday,
                                                                                        self.yesterday, self.yesterday,
                                                                                        self.yesterday))


if __name__ == "__main__":
    # Create elasticsearch objects for each server
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])
    es_fallback = Elasticsearch([FALLBACK_NODE1, FALLBACK_NODE2])
    es_production = Elasticsearch([PRODUCTION_NODE1, PRODUCTION_NODE2])

    # Create logger to log info
    logger = create_logging_instance('sync_hx_hh')

    # Create object and run syncing
    sync_object = SyncHinxtonLondon(es_staging, es_fallback, es_production, logger)
    sync_object.run_sync()
