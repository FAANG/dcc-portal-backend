import requests
import sys
import json
import os
from misc import *
import logging
import utils


logger = utils.create_logging_instance('validate_sample', logging.INFO)


def get_ruleset_version():
    """
    This function will get ruleset version
    :return: version of ruleset
    """
    url = 'https://api.github.com/repos/FAANG/faang-metadata/releases'
    response = requests.get(url).json()
    return response[0]['tag_name']


def validate_total_sample_records(target_dict, material_type, rulesets):
    """
    This function will validate all records inside target_dict
    :param target_dict: dictionary with data
    :param material_type: type of index to use
    :param rulesets: list of possible rulesets
    :return: ...
    """
    total_results = dict()
    portion_size = 600
    data = sorted(list(target_dict.keys()))
    total_size = len(target_dict)
    num_portions = (total_size - total_size % portion_size) / portion_size
    for i in range(int(num_portions)):
        part = list()
        for j in range(portion_size):
            part.append(target_dict[data.pop()])
        total_results = get_validation_results(total_results, part, material_type, rulesets)

    # Rest of the samples
    part = list()
    for biosample_id in data:
        part.append(target_dict[biosample_id])
    total_results = get_validation_results(total_results, part, material_type, rulesets)
    return total_results


def get_validation_results(total_results, part, my_type, rulesets):
    for ruleset in rulesets:
        validation_results = validate_record(part, my_type, ruleset)
        total_results = merge_results(total_results, validation_results, ruleset)
    return total_results


def validate_record(data, my_type, ruleset):
    tmp_out_file = f"{my_type}_records_python.json"
    with open(tmp_out_file, 'w') as w:
        w.write("[\n")
        for index, item in enumerate(data):
            converted_data = convert(item, my_type)
            try:
                converted_data = json.dumps(converted_data)
            except TypeError:
                logger.info(str(item))
                logger.info(str(converted_data))
                exit()

            if index != 0:
                w.write(",\n")
            w.write(f"{converted_data}\n")
        w.write("]\n")
    try:
        command = f'curl -s -F "format=json" -F "rule_set_name={ruleset}" -F "file_format=JSON"' + \
                  f' -F "metadata_file=@{tmp_out_file}" "https://www.ebi.ac.uk/vg/faang/validate" > validation.json'
        os.system(command)
    except Exception as e:
        # TODO log to error
        logger.error("Validation Error!!!" + str(e.args))
        sys.exit(0)
    with open('validation.json', 'r') as f:
        data = json.load(f)
    return parse_validation_results(data['entities'], my_type)


def convert(item, my_type):
    attr = list()
    item_to_test = dict(item)
    # remove the fields not in ruleset, i.e. could not be validated
    for field_name in ['releaseDate', 'updateDate', 'organization', 'biosampleId', 'name', 'standardMet',
                       'versionLastStandardMet', 'etag', 'id_number', "custom field", "allDeriveFromSpecimens"]:
        if field_name in item_to_test:
            del item_to_test[field_name]
    result = dict()
    result['entity_type'] = 'sample'
    result['id'] = item['biosampleId']
    material = item['material']['text']
    if my_type == 'organism':
        attr = parse(attr, item_to_test)
    else:
        del item_to_test['cellType']
        del item_to_test['organism']
        type_specific = to_lower_camel_case(material)
        if type_specific in item_to_test:
            type_specific_dict = item_to_test[type_specific]
            del item_to_test[type_specific]
        else:
            # TODO logging to error
            print(f"Error: type specific data not found for {result['id']} (type {material})")
            return
        attr = parse(attr, item_to_test)
        attr = parse(attr, type_specific_dict)
    result['attributes'] = attr
    return result


def parse(attr, item):
    for key, value in item.items():
        if isinstance(value, list):
            matched = from_lower_camel_case(key)
            if key == 'childOf':
                matched = 'Child of'
            for element in value:
                if isinstance(element, dict):
                    attr.append(parse_hash(element, matched))
                else:
                    attr.append(
                        {
                            'name': matched,
                            'value': element
                        }
                    )
        elif isinstance(value, dict):
            attr.append(parse_hash(value, key))
        else:
            tmp = dict()
            tmp['name'] = from_lower_camel_case(key)
            if key == 'description':
                tmp['name'] = 'Sample Description'
            if key == 'derivedFrom':
                tmp['name'] = 'Derived from'
            tmp['value'] = value
            attr.append(tmp)
    return attr


def parse_hash(element, matched):
    """
    This function will parse list field of data
    :param element: element to parse
    :param matched:
    :return:
    """
    tmp = dict()
    if 'ontologyTerms' in element and element['ontologyTerms'] and len(element['ontologyTerms']) > 0:
        ontology_term = element['ontologyTerms']
        if isinstance(ontology_term, list):
            ontology_term = ontology_term[0]
        tmp = parse_ontology_term(ontology_term)
    if 'unit' in element:
        tmp['units'] = element['unit']
    if 'url' in element:
        tmp['value'] = element['url']
        tmp['uri'] = element['url']
        matched = from_lower_camel_case(matched)
    elif 'URL' in element:
        tmp['value'] = element['URL']
        tmp['uri'] = element['URL']
        matched = from_lower_camel_case(matched)
    else:
        if matched == 'material':
            matched = 'Material'
        else:
            matched = from_lower_camel_case(matched)
        if 'text' in element:
            tmp['value'] = element['text']
        else:
            tmp['value'] = None
    tmp['name'] = matched
    return tmp


def parse_ontology_term(ontology_term):
    """
    This function will parse ontology term
    :param ontology_term: ontology term to parse
    :return: new dict with two values: short_term and source_ref
    """
    # TODO: in general this method is correct, safest is to use OLS API which is used in validation tool.
    #  For ontology libraries used in FAANG, it is fine
    short_term = ontology_term.split("/")[-1].replace(":", "_")
    # some user wrongly provided multicellular organism (UBERON_0000468) for organism
    if short_term == 'UBERON_0000468':
        short_term = 'OBI_0100026'
    result = {
        'id': short_term,
        'source_ref': short_term.split("_")[0]
    }
    return result


def parse_validation_results(data, my_type):
    """
    This function will parse results that were returned by validation tool
    :param data: results to parse in json format
    :param my_type: type of index
    :return: parsing results
    """
    summary = dict()
    errors = dict()
    result = dict()
    for entity in data:
        status = entity['_outcome']['status']
        summary.setdefault(status, 0)
        summary[status] += 1
        biosample_id = entity['id']
        result.setdefault('detail', {})
        result['detail'].setdefault(biosample_id, {})
        result['detail'][biosample_id]['status'] = status
        result['detail'][biosample_id]['type'] = my_type
        tag = status + 's'
        outcome_msgs = list()
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
            tag = field_status + 's'
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
        elif both_type_flag == 1 and contain_error_flag == 0:
            total_msg += f";{backup_msg}"
        result['detail'].setdefault(entity['id'], {})
        result['detail'][entity['id']]['message'] = total_msg

    result['summary'] = summary
    result['errors'] = errors
    return result


def merge_results(total_results, validation_results, ruleset):
    subresults = dict()
    if ruleset in total_results:
        subresults = total_results[ruleset]
    if 'summary' in subresults:
        for status in ['pass', 'warning', 'error']:
            if status in subresults['summary'] and status in validation_results['summary']:
                # TODO check this code
                # subresults['summary'].setdefault(status, [])
                subresults['summary'][status] += validation_results['summary'][status]
            elif status in validation_results['summary']:
                subresults['summary'][status] = validation_results['summary'][status]
            else:
                subresults['summary'][status] = 0
        for tmp in list(validation_results['detail'].keys()):
            subresults['detail'][tmp] = validation_results['detail'][tmp]
        new_error_messages = validation_results['errors']
        for msg in list(new_error_messages.keys()):
            if msg in subresults['errors']:
                subresults['errors'][msg] += new_error_messages[msg]
            else:
                subresults['errors'][msg] = new_error_messages[msg]
    else:
        subresults = validation_results
    total_results[ruleset] = subresults
    return total_results
