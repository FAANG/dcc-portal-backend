import validate_record
from utils import create_logging_instance
from misc import to_lower_camel_case
from typing import List

SPECIMEN_FIELDS_CONVERSION_MAPPING = {
    'material': 'Material',
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
    'custom field',
    'allDeriveFromSpecimens',
    'cellType',
    'organism'
]

logger = create_logging_instance('validate_specimen')


class ValidateSpecimenRecord(validate_record.ValidateRecord):
    def __init__(self, records, rulesets, batch_size=600):
        """
        inherited constructor, call the parental constructor directly with type set as experiment
        """
        super().__init__('specimen', records, rulesets, batch_size)

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
        attr = self.parse(data, attr, SPECIMEN_FIELDS_CONVERSION_MAPPING)

        material = item['material']['text']
        if not material and 'Material' in item:
            material = item['Material']['text']
        type_specific = to_lower_camel_case(material)
        if type_specific in data:
            type_specific_dict = data[type_specific]
            del data[type_specific]
            attr = self.parse(type_specific_dict, attr, SPECIMEN_FIELDS_CONVERSION_MAPPING)
        else:
            logger.error(f"Error: type specific data not found for {result['id']} (type {material})")

        result['attributes'] = attr
        return result
