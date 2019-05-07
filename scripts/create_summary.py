from elasticsearch import Elasticsearch
import requests
import json

from utils import *
from constants import STAGING_NODE1, STAGING_NODE2, MALES, FEMALES


class CreateSummary:
    """
    This class will parse specimen, organism, dataset and file indexes data and create summary data for
    summary_specimen, summary_organism, summary_dataset and summary_file indexes; This data will be used by frontend to
    create charts in summary tab
    """
    def __init__(self, es_instance, logger_instance):
        """
        :param es_instance: staging es instance to write data to
        :param logger_instance: logger to write logs
        """
        self.es_instance = es_instance
        self.logger_instance = logger_instance

    def create_organism_summary(self):
        """
        This function will parse organism data and create summary document for es
        """
        results = requests.get("http://test.faang.org/api/organism/_search/?size=100000").json()
        standard_data = get_standard(results['hits']['hits'])
        sex_data = dict()
        paper_published_data = get_number_of_published_papers(results['hits']['hits'])
        organism_data = dict()
        breed_data = dict()
        for item in results['hits']['hits']:
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

            # get data for organism_data
            organism = item['_source']['organism']['text']
            organism_data.setdefault(organism, 0)
            organism_data[organism] += 1

            # get data for breed_data
            breed = item['_source']['breed']['text']
            breed_data.setdefault(organism, {})
            breed_data[organism].setdefault(breed, 0)
            breed_data[organism][breed] += 1

        # create document for es
        results = dict()
        results['sexSummary'] = create_summary_document_for_es(sex_data)
        results['paperPublishedSummary'] = create_summary_document_for_es(paper_published_data)
        results['standardSummary'] = create_summary_document_for_es(standard_data)
        results['organismSummary'] = create_summary_document_for_es(organism_data)
        results['breedSummary'] = create_summary_document_for_breeds(breed_data)
        body = json.dumps(results)
        self.es_instance.index(index="summary_organism", doc_type="_doc", id="summary_organism", body=body)

    def create_specimen_summary(self):
        """
        This function will parse specimen data and create summary document for es
        """
        results = requests.get("http://test.faang.org/api/specimen/_search/?size=100000").json()
        sex_data = dict()
        paper_published_data = get_number_of_published_papers(results['hits']['hits'])
        standard_data = get_standard(results['hits']['hits'])
        cell_type_data = dict()
        organism_data = dict()
        material_data = dict()
        breed_data = dict()

        # get data for sex_data
        for item in results['hits']['hits']:
            # get data for sex_data
            if 'sex' in item['_source']['organism']:
                sex = item['_source']['organism']['sex']['text']
                if sex in MALES:
                    sex = 'male'
                elif sex in FEMALES:
                    sex = 'female'
                else:
                    sex = 'not determined'
            else:
                sex = 'not determined'
            sex_data.setdefault(sex, 0)
            sex_data[sex] += 1

            # get data for cell_type_data
            if 'cellType' in item['_source']:
                cell_type_data.setdefault(item['_source']['cellType']['text'], 0)
                cell_type_data[item['_source']['cellType']['text']] += 1

            # get data for organism_data
            if 'organism' in item['_source']:
                organism = item['_source']['organism']['organism']['text']
                organism_data.setdefault(organism, 0)
                organism_data[organism] += 1

            # get data for material_data
            if 'material' in item['_source']:
                material = item['_source']['material']['text']
                material_data.setdefault(material, 0)
                material_data[material] += 1

            # get data for breed_data
            if 'organism' in item['_source'] and 'breed' in item['_source']['organism']:
                organism = item['_source']['organism']['organism']['text']
                breed = item['_source']['organism']['breed']['text']
                breed_data.setdefault(organism, {})
                breed_data[organism].setdefault(breed, 0)
                breed_data[organism][breed] += 1

        # create document for es
        results = dict()
        results['sexSummary'] = create_summary_document_for_es(sex_data)
        results['paperPublishedSummary'] = create_summary_document_for_es(paper_published_data)
        results['standardSummary'] = create_summary_document_for_es(standard_data)
        results['cellTypeSummary'] = create_summary_document_for_es(cell_type_data)
        results['organismSummary'] = create_summary_document_for_es(organism_data)
        results['materialSummary'] = create_summary_document_for_es(material_data)
        results['breedSummary'] = create_summary_document_for_breeds(breed_data)
        body = json.dumps(results)
        self.es_instance.index(index="summary_specimen", doc_type="_doc", id="summary_specimen", body=body)

    def create_dataset_summary(self):
        """
        This function will parse dataset data and create summary document for es
        """
        results = requests.get("http://test.faang.org/api/dataset/_search/?size=100000").json()
        standard_data = get_standard(results['hits']['hits'])
        paper_published_data = get_number_of_published_papers(results['hits']['hits'])
        species_data = dict()
        assay_type_data = dict()
        for item in results['hits']['hits']:
            # get data for species_data
            for specie in item['_source']['species']:
                species_data.setdefault(specie['text'], 0)
                species_data[specie['text']] += 1

            # get data for assay_type_data
            for assay_type in item['_source']['assayType']:
                assay_type_data.setdefault(assay_type, 0)
                assay_type_data[assay_type] += 1

        # create document for es
        results = dict()
        results['standardSummary'] = create_summary_document_for_es(standard_data)
        results['paperPublishedSummary'] = create_summary_document_for_es(paper_published_data)
        results['specieSummary'] = create_summary_document_for_es(species_data)
        results['assayTypeSummary'] = create_summary_document_for_es(assay_type_data)
        body = json.dumps(results)
        self.es_instance.index(index="summary_dataset", doc_type="_doc", id="summary_dataset", body=body)

    def create_file_summary(self):
        """
        This function will parse file data and create summary document for es
        """
        results = requests.get("http://test.faang.org/api/file/_search/?size=100000").json()
        standard_data = dict()
        paper_published_data = get_number_of_published_papers(results['hits']['hits'])
        species_data = dict()
        assay_type_data = dict()
        for item in results['hits']['hits']:
            # get data for standard_data
            standard = item['_source']['experiment']['standardMet']
            standard_data.setdefault(standard, 0)
            standard_data[standard] += 1

            # get data for species_data
            specie = item['_source']['species']['text']
            species_data.setdefault(specie, 0)
            species_data[specie] += 1

            # get data for assay_type_data
            assay_type = item['_source']['experiment']['assayType']
            assay_type_data.setdefault(assay_type, 0)
            assay_type_data[assay_type] += 1

        # create document for es
        results = dict()
        results['standardSummary'] = create_summary_document_for_es(standard_data)
        results['paperPublishedSummary'] = create_summary_document_for_es(paper_published_data)
        results['specieSummary'] = create_summary_document_for_es(species_data)
        results['assayTypeSummary'] = create_summary_document_for_es(assay_type_data)
        body = json.dumps(results)
        self.es_instance.index(index="summary_file", doc_type="_doc", id="summary_file", body=body)


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
