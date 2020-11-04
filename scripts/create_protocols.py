import os
import datetime
from elasticsearch import Elasticsearch

from constants import *
from utils import *


class CreateProtocols:
    """
    This class will create indexes for http://data.faang.org/protocol/samples
    (create_sample_protocol) and http://data.faang.org/protocol/experiments
    (create_experiment_protocol)
    """
    def __init__(self, es_staging, logger):
        """
        Initialize es_staging with elasticsearch object and logger with logger
        object
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
        # self.create_experiment_protocol()
        # self.create_analysis_protocol()

    def create_sample_protocol(self):
        """
        This function will create protocols data for samples
        """
        self.logger.info("Creating sample protocols")
        results = self.es_staging.search(index="specimen", size=1000000)
        entries = {}
        for result in results["hits"]["hits"]:
            # Choose field name for specimen type and protocol
            if "specimenFromOrganism" in result["_source"] and \
                    'specimenCollectionProtocol' in \
                    result['_source']['specimenFromOrganism']:
                specimen = 'specimenFromOrganism'
                protocol = 'specimenCollectionProtocol'
            elif 'poolOfSpecimens' in result['_source'] and \
                    'poolCreationProtocol' in \
                    result['_source']['poolOfSpecimens']:
                specimen = 'poolOfSpecimens'
                protocol = 'poolCreationProtocol'
            elif 'cellSpecimen' in result['_source'] and \
                    'purificationProtocol' in result['_source']['cellSpecimen']:
                specimen = 'cellSpecimen'
                protocol = 'purificationProtocol'
            elif 'cellCulture' in result['_source'] and \
                    'cellCultureProtocol' in result['_source']['cellCulture']:
                specimen = 'cellCulture'
                protocol = 'cellCultureProtocol'
            elif 'cellLine' in result['_source'] and \
                    'cultureProtocol' in result['_source']['cellLine']:
                specimen = 'cellLine'
                protocol = 'cultureProtocol'
            else:
                continue
            if result['_source'][specimen][protocol]['filename']:
                key = result['_source'][specimen][protocol]['filename']
                url = result['_source'][specimen][protocol]['url']
                # TODO: special case, will need to update all specimens
                if 'NMBU_SOP_Isolation_of_Monocyte-derived_Macrophages_from_' \
                   'Blood_of_Norwegian_Red_Cattle_20171219.pdf' in key:
                    key = os.path.basename(key)
                parsed = key.strip().split("_")
                # Custom protocols, only protocol_name is known
                if parsed[0] not in UNIVERSITIES and parsed[0] != 'WUR':
                    protocol_name = key
                    university_name = None
                    date = None
                else:
                    # Parsing university name
                    if parsed[0] == 'WUR':
                        university_name = 'WUR'
                    else:
                        university_name = UNIVERSITIES[parsed[0]]
                    # Parsing protocol name
                    if 'SOP' in parsed:
                        protocol_name = " ".join(parsed[2:-1])
                    else:
                        protocol_name = " ".join(parsed[1:-1])
                    # Parsing date
                    for fmt in ['%Y%m%d', '%d%m%Y']:
                        try:
                            date = datetime.strptime(
                                parsed[-1].split(".pdf")[0], fmt)
                        except ValueError:
                            date = None

                # Adding information about specimens
                entries.setdefault(key, {"specimens": [], "universityName": "",
                                         "protocolDate": "",
                                         "protocolName": "", "key": "",
                                         "url": ""})
                specimen = dict()
                specimen["id"] = result["_id"]
                if 'cellType' in result['_source']:
                    specimen["organismPartCellType"] = result["_source"][
                        "cellType"]["text"]
                else:
                    specimen['organismPartCellType'] = None
                specimen["organism"] = result["_source"]["organism"][
                    "organism"]["text"]
                specimen["breed"] = result["_source"]["organism"]["breed"][
                    "text"]
                specimen["derivedFrom"] = result["_source"]["derivedFrom"]

                entries[key]["specimens"].append(specimen)
                entries[key]['universityName'] = university_name
                entries[key]['protocolDate'] = date
                entries[key]["protocolName"] = protocol_name
                entries[key]["key"] = key
                entries[key]["url"] = url

        for protocol_name, protocol_data in entries.items():
            if es_staging.exists('protocols_samples', id=protocol_name):
                es_staging.update(
                    'protocols_samples', id=protocol_name,
                    body={
                        'doc': protocol_data
                    }
                )
            else:
                es_staging.create(
                    'protocols_samples', id=protocol_name,
                    body=protocol_data
                )


    def create_experiment_protocol(self):
        """
        This function will create protocols data for experiments
        """
        pass

    def create_analysis_protocol(self):
        """
        This function will create protocols data for analyses
        """
        pass


if __name__ == "__main__":
    # Create elasticsearch object
    es_staging = Elasticsearch([STAGING_NODE1, STAGING_NODE2])

    # Create logger to log info
    logger = create_logging_instance('create_protocols')

    # Create object and run syncing
    protocols_object = CreateProtocols(es_staging, logger)
    protocols_object.create_protocols()
