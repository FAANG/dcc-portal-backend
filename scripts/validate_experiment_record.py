import os
import sys

from misc import *


def validate_total_experiment_records(target_dict, rulesets):
    total_results = dict()
    portion_size = 600
    data = sorted(list(target_dict.keys()))
    total_size = len(data)
    num_portions = (total_size - total_size % portion_size) / portion_size
    for i in range(int(num_portions)):
        part = list()
        for j in range(portion_size):
            part.append(target_dict[data.pop()])
        total_results = get_validation_results(part, rulesets)

    # Rest of the samples
    part = list()
    for biosample_id in data:
        part.append(target_dict[biosample_id])
    total_results = get_validation_results(part, rulesets)
    return total_results


def get_validation_results(part, rulesets):
    total_results = dict()
    for ruleset in rulesets:
        validation_results = validate_record(part, ruleset)
        total_results = merge_results(total_results, validation_results, ruleset)
    return total_results


def validate_record(data, ruleset):
    tmp_out_file = 'tmp_experiment_records.json'
    with open(tmp_out_file, 'w') as w:
        w.write("[\n")
        for index, item in enumerate(data):
            converted_data = convert_data(item)
            converted_data = json.dumps(converted_data)
            if index != 0:
                w.write(",\n")
            w.write(f"{converted_data}\n")
        w.write("]\n")
    try:
        command = f'curl -F "format=json" -F "rule_set_name={ruleset}" -F "file_format=JSON"' + \
                  f' -F "metadata_file=@{tmp_out_file}" "https://www.ebi.ac.uk/vg/faang/validate" > validation.json'
        os.system(command)
    except:
        # TODO log to error
        print("Validation Error!!!")
        sys.exit(0)
    with open('validation.json', 'r') as f:
        data = json.load(f)
    return parse_validation_results(data['entities'])


def merge_results(total_results, validation_results, ruleset):
    pass


def convert_data(item):
    data = dict(item)
    attr = list()
    result = dict()
    result['entity_type'] = 'experiment'
    result['id'] = data['accession']
    del data['accession']
    del data['standardMet']
    del data['versionLastStandardMet']
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
    attr = parse(attr, data)
    attr = parse(attr, type_specific_dict)
    result['attributes'] = attr
    return result


def parse(attr, data):
    for key, value in data.items():
        if isinstance(value, list):
            matched = from_lower_camel_case(key)
            if key == 'childOf':
                matched = 'Child of'
            for elmt in value:
                if isinstance(elmt, list):
                    attr.append(parse_hash(elmt, matched))
                else:
                    attr.append(
                        {
                            'name': matched,
                            'value': elmt
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
            if key == 'rnaPurity260280ratio':
                tmp['name'] =  'rna purity - 260:280 ratio'
            if key == 'rnaPurity260230ratio':
                tmp['name'] = 'rna purity - 260:230 ratio'
            tmp['value'] = value
            attr.append(tmp)
    return attr


def parse_hash(hash, key):
    if key == 'rnaPreparation3AdapterLigationProtocol':
        key = "rna preparation 3' adapter ligation protocol"
    if key == 'rnaPreparation5AdapterLigationProtocol':
        key = "rna preparation 5' adapter ligation protocol"
    tmp = dict()
    if 'ontologyTerms' in hash:
        if len(hash['ontologyTerms']) > 0:
            tmp = parse_ontology_term(hash['ontologyTerms'])
    if 'unit' in hash:
        tmp['units'] = hash['unit']
    if 'url' in hash:
        tmp['value'] = hash['url']
        tmp['uri'] = hash['url']
        key = from_lower_camel_case(key)
    else:
        key = from_lower_camel_case(key)
        tmp['value'] = hash['text']
    tmp['name'] = key
    return tmp


def parse_ontology_term(ontology_term):
    id = ontology_term.split("/")[-1].replace(":", "_")
    if id == 'UBERON_0000468':
        id = 'OBI_0100026'
    result = {
        'id': id,
        'source_ref': id.split("_")[0]
    }
    return result


def parse_validation_results(entities):
    pass
