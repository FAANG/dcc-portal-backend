import os


def validate_total_experiment_records(target_dict, rulesets):
    total_results = dict()
    portion_size = 600
    data = sorted(list(target_dict.keys))
    total_size = len(data)
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
    total_results = dict()
    for ruleset in rulesets:
        validation_results = validate_record(part, my_type, ruleset)
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
    pass


def parse_validation_results(data):
    pass

