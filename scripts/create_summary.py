from elasticsearch import Elasticsearch
import requests

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
        results = requests.get("http://test.faang.org/api/organism/_search/?size=100000").json()
        standard_data = dict()
        sex_data = dict()
        paper_published_data = {'yes': 0, 'no': 0}
        organism_data = dict()
        breeds_data = dict()
        for item in results['hits']['hits']:
            # Get data for standard_data
            standard_data.setdefault(item['_source']['standardMet'], 0)
            standard_data[item['_source']['standardMet']] += 1

            # get data for sex_data
            sex = item['_source']['sex']['text']
            if sex in MALES:
                sex = 'male'
            elif sex in FEMALES:
                sex = 'female'
            else:
                sex = 'not determined'
            sex_data.setdefault(sex, 0)
            sex_data[sex] += 1

            # get data for paper_published_data
            if 'paperPublished' in item['_source']:
                paper_published_data['yes'] += 1
            else:
                paper_published_data['no'] += 1

            # get data for organism_data
            organism = item['_source']['organism']['text']
            organism_data.setdefault(organism, 0)
            organism_data[organism] += 1

            # get data for breeds_data
            organism = item['_source']['organism']['text']
            breed = item['_source']['breed']['text']
            breeds_data.setdefault(organism, {})
            breeds_data[organism].setdefault(breed, 0)
            breeds_data[organism][breed] += 1
        # create document for es
        results = dict()
        for k, v in sex_data.items():
            results.setdefault('sexSummary', [])
            results['sexSummary'].append({
                'name': k,
                'value': v
            })
        for k, v in paper_published_data.items():
            results.setdefault('paperPublishedSummary', [])
            results['paperPublishedSummary'].append({
                'name': k,
                'value': v
            })
        for k, v in standard_data.items():
            results.setdefault('standardSummary', [])
            results['standardSummary'].append({
                'name': k,
                'value': v
            })
        for k, v in organism_data.items():
            results.setdefault('organismSummary', [])
            results['organismSummary'].append({
                'name': k,
                'value': v
            })
        for k, v in breeds_data.items():
            results.setdefault('breedSummary', [])
            tmp_list = list()
            for tmp_k, tmp_v in v.items():
                tmp_list.append({
                    'name': tmp_k,
                    'value': tmp_v
                })
            results['breedSummary'].append({
                "name": k,
                "value": tmp_list
            })
        body = {"doc": results}
        self.es_instance.update(index="summary_organism", doc_type="_doc", id="summary_organism", body=body)

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
