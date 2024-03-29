import os
import datetime
from elasticsearch import Elasticsearch, RequestsHttpConnection

from constants import *
from utils import *

ES_USER = os.getenv('ES_USER')
ES_PASSWORD = os.getenv('ES_PASSWORD')

class CreateProtocols:
    """
    This class will create indexes for 
    http://data.faang.org/protocol/samples (create_sample_protocol), 
    http://data.faang.org/protocol/experiments (create_experiment_protocol) 
    and http://data.faang.org/protocol/analysis (create_analysis_protocol)
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
        self.create_experiment_protocol()
        self.create_analysis_protocol()

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
                    for fmt in ['%Y%m%d']:
                        try:
                            date = datetime.strptime(
                                parsed[-1].split(".pdf")[0], fmt)
                            date = date.year
                        except ValueError:
                            date = None

                # Adding information about specimens
                key = requests.utils.unquote(key)
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
                entries[key]["protocolName"] = requests.utils.unquote(protocol_name)
                entries[key]["key"] = key
                entries[key]["url"] = url

        for key, protocol_data in entries.items():
            if key == 'restricted access':
                continue
            # handle special cases for external protocols
            if key == protocol_data['url']:
                parsed_name = key.split('/')
                if len(parsed_name) > 1:
                    id = ' '.join(parsed_name[-2:])
                else:
                    id = parsed_name[-1]
                protocol_data['key'] = id
                protocol_data['protocolName'] = id
            else:
                id = key
            if es_staging.exists('protocol_samples', id=id):
                es_staging.update(
                    'protocol_samples', id=id,
                    body={
                        'doc': {
                            'specimens': protocol_data["specimens"]
                        }
                    }
                )
            else:
                es_staging.create(
                    'protocol_samples', id=id,
                    body=protocol_data
                )


    def create_experiment_protocol(self):
        """
        This function will create protocols data for experiments
        """
        self.logger.info("Creating experiments protocols")
        results = self.es_staging.search(index="experiment", size=1000000)
        entries = {}
        assay_types = {
            "ATAC-seq": ["transposaseProtocol"],
            "BS-seq": [
                "bisulfiteConversionProtocol", 
                "pcrProductIsolationProtocol"
            ],
            "ChIP-seq DNA-binding": ["chipProtocol"],
            "ChIP-seq input DNA": ["chipProtocol"],
            "DNase-seq": ["dnaseProtocol"],
            "Hi-C": ["hi-cProtocol"],
            "RNA-seq": [
                "rnaPreparation3AdapterLigationProtocol", 
                "rnaPreparation5AdapterLigationProtocol",
                "libraryGenerationPcrProductIsolationProtocol",
                "preparationReverseTranscriptionProtocol",
                "libraryGenerationProtocol"
            ],
            "WGS": [
                "libraryGenerationPcrProductIsolationProtocol",
                "libraryGenerationProtocol"
            ],
            "CAGE-seq": ["cageProtocol"],

        }
        for result in results["hits"]["hits"]:
            # Choose field name for specimen type and protocol
            if "experimentalProtocol" in result["_source"]:
                protocol = "experimentalProtocol"
                filename = result['_source'][protocol]['filename']
                url = result['_source'][protocol]['url']
            elif "extractionProtocol" in result["_source"]:
                protocol = "extractionProtocol"
                filename = result['_source'][protocol]['filename']
                url = result['_source'][protocol]['url']
            else:
                protocol = None
                for assay in assay_types:
                    if assay in result["_source"]:
                        for prot in assay_types[assay]:
                            if prot in result["_source"][assay]:
                                protocol = prot
                                filename = result['_source'][assay][prot]['filename']
                                url = result['_source'][assay][prot]['url']
                            if protocol:
                                break
                    if protocol:
                        break 

            exp_target = result["_source"]["experimentTarget"]
            assay_type = result["_source"]["assayType"]

            # Adding information about experiments
            if protocol and exp_target and assay_type:
                key = f"{protocol}-{assay_type}-{exp_target}"
                entries.setdefault(key, {"experiments": [], "experimentTarget": "",
                                         "assayType": "", "name": "",
                                         "filename": "", "key": "",
                                         "url": ""})
                experiments = dict()
                experiments["accession"] = result["_source"]["accession"]
                experiments["sampleStorage"] = result["_source"]["sampleStorage"] \
                    if result["_source"]["sampleStorage"] else None
                experiments["sampleStorageProcessing"] = result["_source"]["sampleStorageProcessing"] \
                    if result["_source"]["sampleStorageProcessing"] else None

                entries[key]["experiments"].append(experiments)
                entries[key]['experimentTarget'] = exp_target
                entries[key]['assayType'] = assay_type
                entries[key]["name"] = protocol
                entries[key]["filename"] = filename
                entries[key]["key"] = key
                entries[key]["url"] = url

        for key, protocol_data in entries.items():
            if es_staging.exists('protocol_files', id=key):
                es_staging.update(
                    'protocol_files', id=key,
                    body={
                        'doc': {
                            'experiments': protocol_data["experiments"]
                        }
                    }
                )
            else:
                es_staging.create(
                    'protocol_files', id=key,
                    body=protocol_data
                )

    def create_analysis_protocol(self):
        """
        This function will create protocols data for analyses
        """
        self.logger.info("Creating analysis protocols")
        results = self.es_staging.search(index="analysis", size=1000000)
        entries = {}
        for result in results["hits"]["hits"]:
            if "analysisProtocol" in result["_source"] and \
                    result["_source"]["analysisProtocol"]:
                key = result['_source']['analysisProtocol']['filename']
                url = result['_source']['analysisProtocol']['url']
                if not url:
                    continue
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
                    for fmt in ['%Y%m%d']:
                        try:
                            date = datetime.strptime(
                                parsed[-1].split(".pdf")[0], fmt)
                            date = date.year
                        except ValueError:
                            date = None

                # Adding information about analyses
                key = requests.utils.unquote(key)
                entries.setdefault(key, {"analyses": [], "universityName": "",
                                         "protocolDate": "",
                                         "protocolName": "", "key": "",
                                         "url": ""})
                analyses = dict()
                analyses["accession"] = result["_source"]["accession"]
                if result["_source"]['organism'] and result["_source"]['organism']['text']:
                    analyses['organism'] = result["_source"]['organism']['text']
                if result["_source"]['datasetAccession']:
                    analyses['datasetAccession'] = result["_source"]['datasetAccession']
                if result["_source"]['analysisType']:
                    analyses['analysisType'] = result["_source"]['analysisType']

                entries[key]["analyses"].append(analyses)
                entries[key]["universityName"] = university_name
                entries[key]["protocolDate"] = date
                entries[key]["protocolName"] = requests.utils.unquote(protocol_name)
                entries[key]["key"] = key
                entries[key]["url"] = url

        for key, protocol_data in entries.items():
            if key == 'restricted access':
                continue
            # handle special cases for external protocols
            if key == protocol_data['url']:
                parsed_name = key.split('/')
                if len(parsed_name) > 1:
                    id = ' '.join(parsed_name[-2:])
                else:
                    id = parsed_name[-1]
                protocol_data['key'] = id
                protocol_data['protocolName'] = id
            else:
                id = key
            if es_staging.exists('protocol_analysis', id=id):
                es_staging.update(
                    'protocol_analysis', id=id,
                    body={
                        'doc': {
                            'analyses': protocol_data["analyses"]
                        }
                    }
                )
            else:
                es_staging.create(
                    'protocol_analysis', id=id,
                    body=protocol_data
                )


if __name__ == "__main__":
    # Create elasticsearch object
    es_staging = Elasticsearch([PRODUCTION__NODE_ELASTIC_CLOUD], connection_class=RequestsHttpConnection, http_auth=(ES_USER, ES_PASSWORD), use_ssl=True, verify_certs=False)

    # Create logger to log info
    logger = create_logging_instance('create_protocols')

    # Create object and run syncing
    protocols_object = CreateProtocols(es_staging, logger)
    protocols_object.create_protocols()
