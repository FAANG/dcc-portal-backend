import validate_record
from utils import create_logging_instance
from typing import List

ORGANISM_FIELDS_CONVERSION_MAPPING = {
    'material': 'Material',
    'childOf': 'Child of',
    'sex': 'Sex',
    'description': 'Sample Description'
}

FIELDS_TO_BE_REMOVED = [
    'releaseDate',
    'updateDate',
    'organization',
    'biosampleId',
    'name',
    'standardMet',
    'versionLastStandardMet',
    'etag',
    'id_number',
    'custom field'
]

logger = create_logging_instance('validate_organism')


class ValidateOrganismRecord(validate_record.ValidateRecord):
    def __init__(self, records, rulesets, batch_size=600):
        """
        inherited constructor, call the parental constructor directly with type set as experiment
        """
        super().__init__('organism', records, rulesets, batch_size)

    def convert_data(self, item):
        """
        Overwrite the abstract method
        Create an experiment data structure to be validated
        """
        data = dict(item)
        attr: List = list()
        result = dict()
        result['entity_type'] = 'sample'
        result['id'] = data['biosampleId']
        for removal_field in FIELDS_TO_BE_REMOVED:
            if removal_field in data:
                del data[removal_field]
        attr = self.parse(data, attr, ORGANISM_FIELDS_CONVERSION_MAPPING)
        result['attributes'] = attr
        return result

