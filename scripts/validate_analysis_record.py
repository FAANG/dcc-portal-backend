import validate_record
import utils

ANALYSES_FIELDS_CONVERSION_MAPPING = {
    'datasetAccession': 'study',
    'sampleAccessions': 'samples',
    'experimentAccessions': 'experiments',
    'runAccessions': 'runs',
    'analysisAccessions': 'related analyses'
}
FIELDS_TO_BE_REMOVED = [
    'accession',  # validation tool use fixed id field
    # not in the ruleset
    'standardMet',
    'versionLastStandardMet',
    'releaseDate',
    'updateDate',
    'fileSizes',
    'organism',
    'urls',
    'datasetInPortal'
]
logger = utils.create_logging_instance("validate_analysis")


class ValidateAnalysisRecord(validate_record.ValidateRecord):
    def __init__(self, records, rulesets, batch_size=600):
        """
        inherited constructor, call the parental constructor directly with type set as analysis
        """
        super().__init__('analysis', records, rulesets, batch_size)

    def convert_data(self, item):
        """
        Overwrite the abstract method
        Create an analysis data structure to be validated
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
