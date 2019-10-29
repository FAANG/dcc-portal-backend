import validate_record
import utils
from typing import List, Dict

EXPERIMENT_FIELDS_CONVERSION_MAPPING = {
    'rnaPreparation3AdapterLigationProtocol': "rna preparation 3' adapter ligation protocol",
    'rnaPreparation5AdapterLigationProtocol': "rna preparation 5' adapter ligation protocol"
}

FIELDS_TO_BE_REMOVED = [
    'accession',  # validation tool use fixed id field
    # not in the ruleset
    'standardMet',
    'versionLastStandardMet'
]

logger = utils.create_logging_instance('validate_experiment')


class ValidateExperimentRecord(validate_record.ValidateRecord):
    def __init__(self, records, rulesets, batch_size=600):
        """
        inherited constructor, call the parental constructor directly with type set as experiment
        """
        super().__init__('experiment', records, rulesets, batch_size)

    def convert_data(self, item):
        """
        Overwrite the abstract method
        Create an experiment data structure to be validated
        """
        data = dict(item)
        attr: List = list()
        result = dict()
        result['entity_type'] = 'experiment'
        result['id'] = data['accession']
        type_specific_dict: Dict = dict()
        for removal_field in FIELDS_TO_BE_REMOVED:
            if removal_field in data:
                del data[removal_field]
        if data['assayType'] == 'methylation profiling by high throughput sequencing':
            type_specific = 'BS-seq'
        elif data['assayType'] == 'DNase-Hypersensitivity seq':
            type_specific = 'DNase-seq'
        elif data['assayType'] == 'ATAC-seq':
            type_specific = 'ATAC-seq'
        elif data['assayType'] == 'ChIP-seq':
            if data['experimentTarget'].lower() == 'input dna':
                type_specific = 'ChiP-seq input DNA'
            else:
                type_specific = 'ChiP-seq histone'
        elif data['assayType'] == 'Hi-C':
            type_specific = 'Hi-C'
        elif data['assayType'] == 'whole genome sequencing assay':
            type_specific = 'WGS'
        else:
            type_specific = 'RNA-seq'
        if type_specific in data:
            type_specific_dict = data[type_specific]
            del data[type_specific]
        attr = self.parse(data, attr, EXPERIMENT_FIELDS_CONVERSION_MAPPING)
        attr = self.parse(type_specific_dict, attr, EXPERIMENT_FIELDS_CONVERSION_MAPPING)
        result['attributes'] = attr
        return result
