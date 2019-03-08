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

def validate_record(part, my_type, ruleset):
    pass