"""
The base class which deals with validation records against the ruleset
"""

import json
import os
import sys
from typing import Dict, List
import utils
from misc import from_lower_camel_case


logger = utils.create_logging_instance("validate_record")


def parse_ontology_term(ontology_term):
    """
    extract ontology short term from iri
    a special case that UBERON_0000468 is wrongly used for organism, corrected to the correct one OBI_0100026
    :param ontology_term:
    :return:
    """
    ontology_id = ontology_term.split("/")[-1].replace(":", "_")
    if ontology_id == 'UBERON_0000468':
        ontology_id = 'OBI_0100026'
    result = {
        'id': ontology_id,
        'source_ref': ontology_id.split("_")[0]
    }
    return result


class ValidateRecord:
    def __init__(self, record_type: str, records: Dict, rulesets: List, batch_size: int = 600):
        """
        constructor method
        :param record_type: indicates the type of records, could be one of experiment, analysis
        :param records: the records to be validated, stored as a Dict, keys are record accession and values are the data
        :param rulesets: the name of ruleset(s) to be validated against
        :param batch_size: the list of records to be validated could be very long and to make it possible to transfer to
        the validation server without timeout, it needs to split into small batches. The batch size determines how many
        records are contained in a batch
        """
        self.record_type = record_type
        self.records = records
        self.rulesets = rulesets
        self.batch_size = batch_size

    def get_record_type(self):
        """
        :return: the type of records
        """
        return self.record_type

    def convert_data(self, item):
        """
        abstract method, convert single record into data structure which is expected by the validation server
        it may involve with removing some known fields not in the ruleset, e.g. release date
        :param item: single record data
        :return: converted record
        """
        raise NotImplemented

    def parse(self, data: Dict, attrs: List, mapping_field_names: Dict) -> List:
        """
        parse the record data into list of attributes (required by the validation service) and change the field name
        if mapping_filed_names provided
        :param data: the single record data
        :param attrs: the list of existing converted attributes
        :param mapping_field_names: the list of fields the name of which needs to be replaced
        (ES and ruleset use different names)
        :return: the updated list of attributes
        """
        for key, value in data.items():
            if isinstance(value, list):
                matched = from_lower_camel_case(key)
                if key == 'childOf':
                    matched = 'Child of'
                for elmt in value:
                    if isinstance(elmt, list):
                        attrs.append(self.parse_hash(elmt, matched))
                    else:
                        attrs.append(
                            {
                                'name': matched,
                                'value': elmt
                            }
                        )
            elif isinstance(value, dict):
                attrs.append(self.parse_hash(value, key))
            else:
                tmp = dict()
                if key in mapping_field_names:
                    tmp['name'] = mapping_field_names[key]
                else:
                    tmp['name'] = from_lower_camel_case(key)
                tmp['value'] = value
                attrs.append(tmp)
        return attrs

    def parse_hash(self, hash_value, field_name):
        """
        convert data in hash (Dict) into accepted format
        :param hash_value: the original data in the form of hash
        :param field_name: the field name
        :return: the converted attribute
        """
        if field_name == 'rnaPreparation3AdapterLigationProtocol':
            field_name = "rna preparation 3' adapter ligation protocol"
        if field_name == 'rnaPreparation5AdapterLigationProtocol':
            field_name = "rna preparation 5' adapter ligation protocol"
        tmp = dict()
        if 'ontologyTerms' in hash_value:
            if len(hash_value['ontologyTerms']) > 0:
                tmp = parse_ontology_term(hash_value['ontologyTerms'])
        if 'unit' in hash_value:
            tmp['units'] = hash_value['unit']
        if 'url' in hash_value:
            tmp['value'] = hash_value['url']
            tmp['uri'] = hash_value['url']
            field_name = from_lower_camel_case(field_name)
        else:
            field_name = from_lower_camel_case(field_name)
            if 'text' in hash_value:
                tmp['value'] = hash_value['text']
            else:
                tmp['value'] = None
        tmp['name'] = field_name
        return tmp

    @staticmethod
    def get_ruleset_version():
        """
        This function will get ruleset version
        :return: version of ruleset
        """
        # url = 'https://api.github.com/repos/FAANG/faang-metadata/releases'
        # response = requests.get(url).json()
        # print(response)
        # return response[0]['tag_name']
        # GitHub API introduced rate limit, temporarily use hard-coded version
        # https://github.com/FAANG/dcc-metadata/releases
        return "3.6.3"

    def validate(self) -> Dict:
        """
        Validate all records
        This function mainly splits all records into small batches, validate each batch and put the results together
        :return: the total validation result
        """
        total_results = dict()
        ids = sorted(list(self.records.keys()))
        total_size = len(ids)
        num_portions = (total_size - total_size % self.batch_size) / self.batch_size
        for i in range(int(num_portions)):
            part = list()
            for j in range(self.batch_size):
                part.append(self.records[ids.pop()])
            total_results = self.get_validation_results(total_results, part)

        # Rest of the samples
        part = list()
        for record_id in ids:
            part.append(self.records[record_id])
        total_results = self.get_validation_results(total_results, part)
        return total_results

    def get_validation_results(self, total_results, part_records) -> Dict:
        """
        For the given batch, do the validation and merge the batch result into total result
        :param total_results: the total validation result
        :param part_records: the batch of records to be validated
        :return: the updated total validation result
        """
        for ruleset in self.rulesets:
            validation_results = self.validate_record_ruleset(part_records, ruleset)
            total_results = self.merge_results(total_results, validation_results, ruleset)
        return total_results

    def merge_results(self, total_results, validation_results, ruleset):
        """
        Merge single batch validation result into total validation result
        :param total_results: the total validation result
        :param validation_results: single batch validation result
        :param ruleset: ruleset name
        :return: the updated total validation result
        """
        sub_results = dict()
        if ruleset in total_results:
            sub_results = total_results[ruleset]
        if 'summary' in sub_results:
            for status in ['pass', 'warning', 'error']:
                sub_results['summary'].setdefault(status, 0)
                if status in validation_results['summary']:
                    sub_results['summary'][status] += validation_results['summary'][status]
            for tmp in list(validation_results['detail'].keys()):
                sub_results['detail'][tmp] = validation_results['detail'][tmp]
            new_error_messages = validation_results['errors']
            for msg in list(new_error_messages.keys()):
                sub_results['errors'].setdefault(msg, 0)
                sub_results['errors'][msg] += new_error_messages[msg]
        else:  # summary not existing, i.e. first batch (empty total validation result)
            sub_results = validation_results
        total_results[ruleset] = sub_results
        return total_results

    def validate_record_ruleset(self, part_records, ruleset):
        """
        Convert the batch records to the format recognized by the validation server, save into file
        and do the validation
        :param part_records: the batch records
        :param ruleset: the ruleset name
        :return: the converted validation result of the batch record
        """
        tmp_out_file = f'tmp_{self.record_type}_records.json'
        with open(tmp_out_file, 'w') as w:
            w.write("[\n")
            for index, item in enumerate(part_records):
                converted_data = self.convert_data(item)
                converted_data = json.dumps(converted_data)
                if index != 0:
                    w.write(",\n")
                w.write(f"{converted_data}\n")
            w.write("]\n")
        tmp_validation_result_file = f'{self.record_type}_{ruleset}_validation_result.json'
        tmp_validation_result_file = tmp_validation_result_file.replace(' ', '_')
        try:
            command = f'curl -s -F "format=json" -F "rule_set_name={ruleset}" -F "file_format=JSON"' + \
                      f' -F "metadata_file=@{tmp_out_file}" "https://www.ebi.ac.uk/vg/faang/validate" > ' \
                          f'{tmp_validation_result_file}'
            os.system(command)
        except Exception:
            logger.error("Validation Error!!!")
            sys.exit(0)
        with open(tmp_validation_result_file, 'r') as f:
            data = json.load(f)
        return self.parse_validation_results(data['entities'])

    def parse_validation_results(self, entities):
        """
        Convert the validation result directly from the validation server for the batch into intermediate structure
        which could be merged into total result
        :param entities: validation result from the server
        :return: the converted result which is a dict having three fixed keys: summary, detail and errors
        The value of "summary" is a hash with fixed keys: pass, warning and error with the count as their values
        The value of "detail" is the dict with id (as input) as its keys and error/warning messages as the values
        The value of "errors" is the dict of error message and its occurrence
        """
        """
        entities has the data structure: list of single record validation result
        each result has the keys: 
            _outcome:
                status
                errors
                warnings
            id:
            attributes: list of attribute
                "value": "microRNA profiling by high throughput sequencing",
                "name": "assay type",
                "allow_further_validation": 1,
                "uri": null,
                "_outcome": {
                    "warnings": [],
                    "errors": [],
                    "status": "pass"
                },
                "source_ref": null,
                "id": null,
                "units": null
        """
        summary = dict()
        errors = dict()
        result = dict()
        for entity in entities:
            status = entity['_outcome']['status']
            summary.setdefault(status, 0)
            summary[status] += 1
            entity_id = entity['id']
            result.setdefault('detail', dict())
            result['detail'].setdefault(entity_id, dict())
            result['detail'][entity_id]['status'] = status

            backup_msg = ''
            tag = status + 's'
            status = status.upper()
            outcome_msgs = list()
            # if the warning/error related to columns is "not existing in the data" (e.g. no project column found),
            # the attribute iteration will not go through that column as the attribute not there
            if tag in entity['_outcome']:
                for message in entity['_outcome'][tag]:
                    outcome_msgs.append(f"({status}){message}")
                backup_msg = ";".join(outcome_msgs)

            msgs = list()
            attributes = entity['attributes']
            both_type_flag = 0
            contain_error_flag = 0
            for attr in attributes:
                field_status = attr['_outcome']['status']
                if field_status.upper() == 'PASS':
                    continue
                if field_status != status:
                    both_type_flag = 1
                if field_status.upper() == 'ERROR':
                    contain_error_flag = 1
                tag = field_status.lower() + 's'
                msg = f"{attr['name']}:{attr['_outcome'][tag][0]}"
                if field_status.upper() == 'ERROR':
                    errors.setdefault(msg, 0)
                    errors[msg] += 1
                msg = f"({field_status}){msg}"
                msgs.append(msg)
            msgs = sorted(msgs)
            total_msg = ";".join(msgs)
            if len(msgs) == 0:
                total_msg = backup_msg
                if status == 'error':
                    errors.setdefault(backup_msg, 0)
                    errors[backup_msg] += 1
            # existing both errors and warnings, but attributes iteration does not contain error
            # means that error contained in the backup_msg, e.g. missing mandatory fields
            elif both_type_flag == 1 and contain_error_flag == 0:
                total_msg += f";{backup_msg}"
            result['detail'][entity_id]['message'] = total_msg

        result['summary'] = summary
        result['errors'] = errors
        return result
