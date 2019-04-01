from elasticsearch import Elasticsearch

from constants import *
from utils import *


class CreateProtocols:
    """
    This class will create indexes for http://data.faang.org/protocol/samples (create_sample_protocol)
    and http://data.faang.org/protocol/experiments (create_experiment_protocol)
    """
    def __init__(self, es_staging, logger):
        """
        Initialize es_staging with elasticsearch object and logger with logger object
        :param es_staging: es staging object
        :param logger: logger object
        """
        self.es_staging = es_staging
        self.logger = logger

    def create_protocols(self):
        """
        Main function that will run function to create protocols
        """
        self.create_sample_protocol()
        self.create_experiment_protocol()

    def create_sample_protocol(self):
        """
        This function will create protocols data for samples
        """
        self.logger.info("Creating sample protocols")
        results = self.es_staging.search(index="specimen", size=100000)
        entries = {}
        for result in results["hits"]["hits"]:
            if "specimenFromOrganism" in result["_source"] and 'specimenCollectionProtocol' in \
                    result['_source']['specimenFromOrganism']:
                key = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['filename']
                url = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['url']
                try:
                    protocol_type = \
                        result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['url'].split("/")[5]
                except Exception as e:
                    self.logger.warning("Error was: {}, URL was: {}".format(
                        e.args[0],
                        result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['url']
                    ))
                    protocol_type = ""
                parsed = key.split("_")
                if parsed[0] in UNIVERSITIES:
                    name = UNIVERSITIES[parsed[0]]
                    protocol_name = " ".join(parsed[2:-1])
                    date = parsed[-1].split(".")[0]
                    entries.setdefault(key, {"specimen": [], "universityName": "", "protocolDate": "",
                                             "protocolName": "", "key": "", "url": "", "protocolType": ""})
                    specimen = dict()
                    specimen["id"] = result["_id"]
                    specimen["organismPartCellType"] = result["_source"]["cellType"]["text"]
                    specimen["organism"] = result["_source"]["organism"]["organism"]["text"]
                    specimen["breed"] = result["_source"]["organism"]["breed"]["text"]
                    specimen["derivedFrom"] = result["_source"]["derivedFrom"]

                    entries[key]["specimen"].append(specimen)
                    entries[key]['universityName'] = name
                    entries[key]['protocolDate'] = date[0:4]
                    entries[key]["protocolName"] = protocol_name
                    entries[key]["key"] = key
                    if protocol_type in ["analysis", "assays", "samples"]:
                        entries[key]["protocolType"] = protocol_type
                    entries[key]["url"] = url
        for item in entries:
            self.es_staging.index(index='protocol_samples', doc_type="_doc", id=item, body=entries[item])

    def create_experiment_protocol(self):
        """
        This function will create protocols data for experiments
        """
        return_results = {}
        results = self.es_staging.search(index="experiment", size=100000)

        def expand_object(data, assay='', target='', accession='', storage='', processing=''):
            for key in data:
                if isinstance(data[key], dict):
                    if 'filename' in data[key]:
                        if data[key]['filename'] != '' and data[key]['filename'] is not None:
                            if assay == '' and target == '' and accession == '' and storage == '' and processing == '':
                                data_key = "{}-{}-{}".format(key, data['assayType'], data['experimentTarget'])
                                # remove all spaces to form a key
                                data_key = "".join(data_key.split())
                                data_experiment = dict()
                                data_experiment['accession'] = data['accession']
                                data_experiment['sampleStorage'] = data['sampleStorage']
                                data_experiment['sampleStorageProcessing'] = data['sampleStorageProcessing']
                                return_results.setdefault(data_key, {'name': key,
                                                                     'experimentTarget': data['experimentTarget'],
                                                                     'assayType': data['assayType'],
                                                                     'key': data_key,
                                                                     'url': data[key]['url'],
                                                                     'filename': data[key]['filename'],
                                                                     'experiments': []})
                                return_results[data_key]['experiments'].append(data_experiment)
                            else:
                                data_key = "{}-{}-{}".format(key, assay, target)
                                data_key = "".join(data_key.split())
                                data_experiment = dict()
                                data_experiment['accession'] = accession
                                data_experiment['sampleStorage'] = storage
                                data_experiment['sampleStorageProcessing'] = processing
                                return_results.setdefault(data_key, {'name': key,
                                                                     'experimentTarget': target,
                                                                     'assayType': assay,
                                                                     'key': data_key,
                                                                     'url': data[key]['url'],
                                                                     'filename': data[key]['filename'],
                                                                     'experiments': []})
                                return_results[data_key]['experiments'].append(data_experiment)
                    else:
                        expand_object(data[key], data['assayType'], data['experimentTarget'], data['accession'],
                                      data['sampleStorage'], data['sampleStorageProcessing'])

        for item in results['hits']['hits']:
            expand_object(item['_source'])
        for item in return_results:
            self.es_staging.index(index='protocol_files', doc_type="_doc", id=item, body=return_results[item])


if __name__ == "__main__":
    # Create elasticsearch object
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])

    # Create logger to log info
    logger = create_logging_instance('create_protocols')

    # Create object and run syncing
    protocols_object = CreateProtocols(es_staging, logger)
    protocols_object.create_protocols()
