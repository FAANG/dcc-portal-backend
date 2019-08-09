import validate_record
import utils

ANALYSES_FIELDS_CONVERSION_MAPPING = {
    'datasetAccession': 'study',
    'sampleAccessions': 'samples'
}
FIELDS_TO_BE_REMOVED = [
    'accession',  # validation tool use fixed id field
    # not in the ruleset
    'standardMet',
    'versionLastStandardMet',
    'releaseDate',
    'updateDate',
    'fileSizes'
]
logger = utils.create_logging_instance("validate_analysis")

class validate_analysis_record(validate_record.validate_record):
    def __init__(self, records, rulesets, batch_size=600):
        super().__init__('analysis', records, rulesets, batch_size)

    def convert_data(self, item):
        """
        Overwrite the abstract method
        Create an analysis data structure to be validated
        :return:
        """
        data = dict(item)
        attr = list()
        result = dict()
        result['entity_type'] = 'analysis'
        result['id'] = data['accession']
        # remove fields known not in the ruleset
        for removal_field in FIELDS_TO_BE_REMOVED:
            if removal_field in data:
                del data[removal_field]
        attr = self.parse(data, attr, ANALYSES_FIELDS_CONVERSION_MAPPING)
        result['attributes'] = attr
        return result
