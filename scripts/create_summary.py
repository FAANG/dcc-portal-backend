from elasticsearch import Elasticsearch

from utils import *


class CreateSummary:
    """
    This class will parse specimen, organism, dataset and file indexes data and create summary data for summary_specimen,
    summary_organism, summary_dataset and summary_file indexes; This data will be used by frontend to create charts in
    summary tab
    """
    def __init__(self, es_instance, logger_instance):
        """
        :param es_instance: staging es instance to write data to
        :param logger_instance: logger to write logs
        """
        self.es_instance = es_instance
        self.logger_instance = logger_instance

    def create_organism_summary(self):
        pass

    def create_specimen_summary(self):
        pass

    def create_dataset_summary(self):
        pass

    def create_file_summary(self):
        pass


if __name__ == "__main__":
    # Create elasticsearch objects for each server
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])

    # Create logger to log info
    logger = create_logging_instance('create_summary')

    # Create summary data for each of the indeces and write it to staging es
    summary_object = CreateSummary(es_instance=es_staging, logger_instance=logger)
    summary_object.create_organism_summary()
    summary_object.create_specimen_summary()
    summary_object.create_dataset_summary()
    summary_object.create_file_summary()
