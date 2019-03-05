import requests
import sys
import json
from misc import *


def get_ruleset_version():
    url = 'https://api.github.com/repos/FAANG/faang-metadata/releases'
    response = requests.get(url).json()
    return response[0]['tag_name']


def validate_total_sample_records(target_dict, my_type, rulesets):
    """
    This function will validate all records inside target_dict
    :param target_dict: dictionary with data
    :param my_type: type of index to use
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
        total_results = get_validation_results(part, my_type, rulesets)

    # Rest of the samples
    part = list()
    for biosample_id in data:
        part.append(target_dict[biosample_id])
    total_results = get_validation_results(part, my_type, rulesets)
    return total_results


def get_validation_results(part, my_type, rulesets):
    for ruleset in rulesets:
        validation_results = validate_record(part, my_type, ruleset)
        total_results = ''
    return total_results


def validate_record(data, my_type, ruleset):
    tmp_out_file = f"{my_type}_records_python.json"
    with open(tmp_out_file, 'w') as w:
        w.write("[\n")
        for index, item in enumerate(data):
            converted_data = convert(item, my_type)
            converted_data = json.dumps(converted_data)
            if index != 0:
                w.write(",\n")
            w.write(f"{converted_data}\n")
        w.write("]\n")
    return ''


def convert(item, my_type):
    attr = list()
    item_to_test = dict(item)
    for field_name in ['releaseDate', 'updateDate', 'organization', 'biosampleId', 'name', 'standardMet',
                       'versionLastStandardMet']:
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
    if 'ontologyTerms' in element and len(element['ontologyTerms']) > 0:
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
    :return: new dict with two values: id and source_ref
    """
    id = ontology_term.split("/")[-1].replace(":", "_")
    if id == 'UBERON_0000468':
        id = 'OBI_0100026'
    result = {
        'id': id,
        'source_ref': id.split("_")[0]
    }
    return result
