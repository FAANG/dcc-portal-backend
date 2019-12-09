"""
This script reads from ENA FAANG data portal, parses and validates the data and stores into Elastic Search
It is highly recommended to check out the corresponding rule set
https://github.com/FAANG/faang-metadata/blob/master/rulesets/faang_experiments.metadata_rules.json
to help understanding the code
"""
import click
from constants import TECHNOLOGIES, STANDARDS, STAGING_NODE1, STANDARD_LEGACY, STANDARD_FAANG
from elasticsearch import Elasticsearch
from utils import determine_file_and_source, check_existsence, remove_underscore_from_end_prefix, \
    create_logging_instance, insert_into_es, generate_ena_api_endpoint
import validate_experiment_record
import validate_record
import sys
import json
import requests
from misc import convert_readable, get_filename_from_url

RULESETS = ["FAANG Experiments", "FAANG Legacy Experiments"]

logger = create_logging_instance('import_ena')


@click.command()
@click.option(
    '--es_hosts',
    default=STAGING_NODE1,
    help='Specify the Elastic Search server(s) (port could be included), e.g. wp-np3-e2:9200. '
         'If multiple servers are provided, please use ";" to separate them, e.g. "wp-np3-e2;wp-np3-e3"'
)
@click.option(
    '--es_index_prefix',
    default="",
    help='Specify the Elastic Search index prefix, e.g. '
         'faang_build_1_ then the indices will be faang_build_1_experiment etc.'
         'If not provided, then work on the aliases, e.g. experiment'
)
# TODO check single or double quotes
def main(es_hosts, es_index_prefix):
    """
    Main function that will import data from ena
    :param es_hosts: elasticsearch hosts where the data import into
    :param es_index_prefix: the index prefix points to a particular version of data
    :return:
    """

    hosts = es_hosts.split(";")
    logger.info("Command line parameters")
    logger.info("Hosts: "+str(hosts))
    es_index_prefix = remove_underscore_from_end_prefix(es_index_prefix)
    if es_index_prefix:
        logger.info("Index_prefix:"+es_index_prefix)

    es = Elasticsearch(hosts)

    logger.info(f"Get current specimens stored in the corresponding ES index {es_index_prefix}_specimen")
    biosample_ids = get_all_specimen_ids(hosts[0], es_index_prefix)

    if not biosample_ids:
        logger.error("No specimen data found in the given index, please run import_from_biosamles.py first")
        sys.exit(1)
    known_errors = get_known_errors()
    new_errors = dict()

    ruleset_version = validate_record.ValidateRecord.get_ruleset_version()
    logger.info(f"Current experiment ruleset version: {ruleset_version}")

    logger.info("Retrieving data from ENA")
    data = get_ena_data()

    indexed_files = dict()
    datasets = dict()
    experiments = dict()
    files_dict = dict()
    studies_from_api = dict()
    exps_in_dataset = dict()
    for record in data:
        studies_from_api.setdefault(record['study_accession'], 0)
        studies_from_api[record['study_accession']] += 1
        library_strategy = record['library_strategy']
        assay_type = record['assay_type']
        experiment_target = record['experiment_target']
        if experiment_target == 'EFO_0005031':
            experiment_target = 'input DNA'
        if experiment_target == 'CHEBI_33697':
            experiment_target = 'RNA'
        # some studies use non-standard values or miss value for assay type, library strategy and experiment target
        # standardize them below based on the 1-to-1 relationship in the ruleset
        # assign assay type according to library strategy
        if assay_type == '':
            if library_strategy == 'Bisulfite-Seq':
                assay_type = 'methylation profiling by high throughput sequencing'
            elif library_strategy == 'DNase-Hypersensitivity':
                assay_type = 'DNase-Hypersensitivity seq'
        if assay_type == 'whole genome sequencing':
            assay_type = 'whole genome sequencing assay'
        # assign value to experiment target if empty according to assay type
        if assay_type == 'ATAC-seq':
            if len(experiment_target) == 0:
                experiment_target = 'open_chromatin_region'
        elif assay_type == 'methylation profiling by high throughput sequencing':
            if len(experiment_target) == 0:
                experiment_target = 'DNA methylation'
        elif assay_type == 'DNase-Hypersensitivity seq':
            if len(experiment_target) == 0:
                experiment_target = 'open_chromatin_region'
        elif assay_type == 'Hi-C':
            if len(experiment_target) == 0:
                experiment_target = 'chromatin'
        elif assay_type == 'whole genome sequencing assay':
            if len(experiment_target) == 0:
                experiment_target = 'input DNA'
        elif assay_type == 'CAGE-seq':
            if len(experiment_target) == 0:
                experiment_target = 'TSS'

        if assay_type == 'ChIP-seq' and experiment_target.lower() != 'input dna':
            target_used_in_file = record['chip_target']
        else:
            target_used_in_file = experiment_target

        file_type, source_type = determine_file_and_source(record)

        if file_type == '':
            continue
        if file_type == 'fastq':
            archive = 'ENA'
        elif file_type == 'cram_index':
            archive = 'ENA'
            file_type = 'submitted'
            source_type = 'ftp'
        else:
            archive = 'SRA'

        files = record[f"{file_type}_{source_type}"].split(";")
        sizes = record[f"{file_type}_bytes"].split(";")
        if file_type == 'fastq':
            types = ['fastq'] * len(files)
        else:
            types = record['submitted_format'].split(";")
        if len(files) != len(types) or len(files) != len(sizes) or len(types) == 0:
            continue
        # for ENA, it is fixed to MD5 as the checksum method
        checksums = record[f"{file_type}_md5"].split(";")
        for index, file in enumerate(files):
            specimen_biosample_id = record['sample_accession']
            # if the ena records contains biosample records which have not been in FAANG data portal (biosample_ids)
            # and not been reported before (known_errors) then these records need to be reported
            if specimen_biosample_id not in biosample_ids:
                if (record['study_accession'] not in known_errors) \
                        or (record['study_accession'] in known_errors
                            and specimen_biosample_id not in known_errors[record['study_accession']]):
                    new_errors.setdefault(record['study_accession'], {})
                    new_errors[record['study_accession']][specimen_biosample_id] = 1
                continue
            fullname = file.split("/")[-1]
            filename = fullname.split(".")[0]
            es_file_doc = {
                'specimen': specimen_biosample_id,
                'organism': check_existsence(biosample_ids[specimen_biosample_id]['organism'], 'biosampleId'),
                'species': biosample_ids[specimen_biosample_id]['organism']['organism'],
                'url': file,
                'name': fullname,
                'type': types[index],
                'size': sizes[index],
                'readableSize': convert_readable(sizes[index]),
                'checksumMethod': 'md5',
                'checksum': checksums[index],
                'archive': archive,
                'baseCount': record['base_count'],
                'readCount': record['read_count'],
                'releaseDate': record['first_public'],
                'updateDate': record['last_updated'],
                'submission': record['submission_accession'],
                'experiment': {
                    'accession': record['experiment_accession'],
                    'assayType': assay_type,
                    'target': target_used_in_file
                },
                'run': {
                    'accession': record['run_accession'],
                    'alias': record['run_alias'],
                    'platform': record['instrument_platform'],
                    'instrument': record['instrument_model'],
                    'centerName': record['center_name'],
                    'sequencingDate': record['sequencing_date'],
                    'sequencingLocation': record['sequencing_location'],
                    'sequencingLatitude': record['sequencing_latitude'],
                    'sequencingLongitude': record['sequencing_longitude']
                },
                'study': {
                    'accession': record['study_accession'],
                    'alias': record['study_alias'],
                    'title': record['study_title'],
                    # TODO it is mandatory field
                    'type': check_existsence(record, 'study_type'),
                    'secondaryAccession': record['secondary_study_accession']
                }
            }
            files_dict[filename] = es_file_doc
            exp_id = record['experiment_accession']
            # one experiment could have multiple runs/files, therefore experiment info needs to be collected once
            if exp_id not in experiments:
                experiment_protocol = None
                experiment_protocol_filename = None
                if 'experimental_protocol' in record and record['experimental_protocol']:
                    experiment_protocol = record['experimental_protocol']
                    experiment_protocol_filename = get_filename_from_url(experiment_protocol,
                                                                         f"{exp_id} experiment protocol")

                extraction_protocol = None
                extraction_protocol_filename = None
                if 'extraction_protocol' in record and record['extraction_protocol']:
                    extraction_protocol = record['extraction_protocol']
                    extraction_protocol_filename = get_filename_from_url(extraction_protocol,
                                                                         f"{exp_id} extraction protocol")

                exp_es = {
                    'accession': exp_id,
                    'project': record['project'],
                    'assayType': assay_type,
                    'experimentTarget': experiment_target,
                    'sampleStorage': check_existsence(record, 'sample_storage'),
                    'sampleStorageProcessing': record['sample_storage_processing'],
                    'samplingToPreparationInterval': {
                        'text': check_existsence(record, 'sample_prep_interval'),
                        'unit': check_existsence(record, 'sample_prep_interval_units')
                    },
                    'experimentalProtocol': {
                        'url': experiment_protocol,
                        'filename': experiment_protocol_filename
                    },
                    'extractionProtocol': {
                        'url': extraction_protocol,
                        'filename': extraction_protocol_filename
                    },
                    'libraryPreparationLocation': check_existsence(record, 'library_prep_location'),
                    'libraryPreparationDate': {
                        'text': check_existsence(record, 'library_prep_date'),
                        'unit': check_existsence(record, 'library_prep_date_format')
                    },
                    'sequencingLocation': check_existsence(record, 'sequencing_location'),
                    'sequencingDate': {
                        'text': check_existsence(record, 'sequencing_date'),
                        'unit': check_existsence(record, 'sequencing_date_format')
                    }
                }
                if 'library_prep_longitude' in record and len(record['library_prep_longitude']) > 0:
                    exp_es['libraryPreparationLocationLongitude'] = {
                        'text': record['library_prep_longitude'],
                        'unit': 'decimal degrees'
                    }
                if 'library_prep_latitude' in record and len(record['library_prep_latitude']) > 0:
                    exp_es['libraryPreparationLocationLatitude'] = {
                        'text': record['library_prep_latitude'],
                        'unit': 'decimal degrees'
                    }
                if 'sequencing_longitude' in record and len(record['sequencing_longitude']) > 0:
                    exp_es['sequencingLocationLongitude'] = {
                        'text': record['sequencing_longitude'],
                        'unit': 'decimal degrees'
                    }
                if 'sequencing_latitude' in record and len(record['sequencing_latitude']) > 0:
                    exp_es['sequencingLocationLatitude'] = {
                        'text': record['sequencing_latitude'],
                        'unit': 'decimal degrees'
                    }
                # deal with technology specific data
                if assay_type == 'ATAC-seq':  # ATAC-seq
                    transposase_protocol = record['transposase_protocol']
                    transposase_protocol_filename = get_filename_from_url(transposase_protocol,
                                                                          f"{exp_id} ATAC transposase protocol")
                    exp_es['ATAC-seq'] = {
                        'transposaseProtocol': {
                            'url': transposase_protocol,
                            'filename': transposase_protocol_filename
                        }
                    }
                elif assay_type == 'methylation profiling by high throughput sequencing':  # BS-Seq
                    conversion_protocol = None
                    conversion_protocol_filename = None
                    pcr_isolation_protocol = None
                    pcr_isolation_protocol_filename = None
                    if 'bisulfite_protocol' in record:
                        conversion_protocol = record['bisulfite_protocol']
                        conversion_protocol_filename = get_filename_from_url(conversion_protocol,
                                                                             f"{exp_id} BS conversion protocol")
                    if 'pcr_isolation_protocol' in record:
                        pcr_isolation_protocol = record['pcr_isolation_protocol']
                        pcr_isolation_protocol_filename = get_filename_from_url(pcr_isolation_protocol,
                                                                                f"{exp_id} BS pcr protocol")
                    exp_es['BS-seq'] = {
                        'librarySelection': record['faang_library_selection'],
                        'bisulfiteConversionProtocol': {
                            'url': conversion_protocol,
                            'filename': conversion_protocol_filename
                        },
                        'pcrProductIsolationProtocol': {
                            'url': pcr_isolation_protocol,
                            'filename': pcr_isolation_protocol_filename
                        },
                        'bisulfiteConversionPercent': record['bisulfite_percent'],
                        'restrictionEnzyme': record['restriction_enzyme']
                    }
                    # in the old ruleset, mistake made to allow RBBS instead of RRBS,
                    # so the statement below must be there until old data gets curated
                    if exp_es['BS-seq']['librarySelection'] == 'RBBS':
                        exp_es['BS-seq']['librarySelection'] = 'RRBS'
                elif assay_type == 'ChIP-seq':  # ChIP-seq
                    chip_protocol = record['chip_protocol']
                    chip_protocol_filename = get_filename_from_url(chip_protocol, f"{exp_id} chip protocol")
                    section_info = {
                        'chipProtocol': {
                            'url': chip_protocol,
                            'filename': chip_protocol_filename
                        },
                        'libraryGenerationMaxFragmentSizeRange': record['library_max_fragment_size'],
                        'libraryGenerationMinFragmentSizeRange': record['library_min_fragment_size']
                    }
                    if experiment_target.lower() == 'input dna':
                        exp_es['ChIP-seq input DNA'] = section_info
                    else:
                        section_info['chipAntibodyProvider'] = record['chip_ab_provider']
                        section_info['chipAntibodyCatalog'] = record['chip_ab_catalog']
                        section_info['chipAntibodyLot'] = record['chip_ab_lot']

                        section_info['chipTarget'] = record['chip_target']
                        section_info['controlExperiment'] = record['control_experiment']
                        exp_es['ChIP-seq DNA-binding'] = section_info
                elif assay_type == 'DNase-Hypersensitivity seq"':  # DNase seq
                    dnase_protocol = None
                    dnase_protocol_filename = None
                    if 'dnase_protocol' in record:
                        dnase_protocol = record['dnase_protocol']
                        dnase_protocol_filename = get_filename_from_url(dnase_protocol, f"{exp_id} dnase protocol")
                    exp_es['DNase-seq'] = {
                        'dnaseProtocol': {
                            'url': dnase_protocol,
                            'filename': dnase_protocol_filename
                        }
                    }
                elif assay_type == 'Hi-C':  # Hi-C
                    hi_c_protocol = None
                    hi_c_protocol_filename = None
                    if 'hi_c_protocol' in record:
                        hi_c_protocol = record['hi_c_protocol']
                        hi_c_protocol_filename = get_filename_from_url(hi_c_protocol, f"{exp_id} hi-c protocol")
                    exp_es['Hi-C'] = {
                        'restrictionEnzyme': record['restriction_enzyme'],
                        'restrictionSite': record['restriction_site'],
                        'hi-cProtocol': {
                            'url': hi_c_protocol,
                            'filename': hi_c_protocol_filename
                        }
                    }
                elif assay_type == 'whole genome sequencing assay':  # WGS
                    library_pcr_protocol = record['library_pcr_isolation_protocol']
                    library_pcr_protocol_filename = get_filename_from_url(library_pcr_protocol,
                                                                          f"{exp_id} WGS pcr protocol")
                    library_generation_protocol = record['library_gen_protocol']
                    library_generation_protocol_filename = get_filename_from_url(library_generation_protocol,
                                                                                 f"{exp_id} WGS generation protocol")
                    exp_es['WGS'] = {
                        'libraryGenerationPcrProductIsolationProtocol': {
                            'url': library_pcr_protocol,
                            'filename': library_pcr_protocol_filename
                        },
                        'libraryGenerationProtocol': {
                            'url': library_generation_protocol,
                            'filename': library_generation_protocol_filename
                        },
                        'librarySelection': record['faang_library_selection']
                    }
                elif assay_type == 'CAGE-seq': # CAGE-seq
                    cage_protocol = record['cage_protocol']
                    cage_protocol_name = get_filename_from_url(cage_protocol, f"{exp_id} CAGE-seq protocol")
                    exp_es['CAGE-seq'] = {
                        'rnaPurity260280ratio': record['rna_purity_280_ratio'],
                        'rnaPurity260230ratio': record['rna_purity_230_ratio'],
                        'rnaIntegrityNumber': record['rna_integrity_num'],
                        'cageProtocol': {
                            'url': cage_protocol,
                            'filename': cage_protocol_name
                        },
                        'sequencingPrimerProvider': record['sequencing_primer_provider'],
                        'sequencingPrimerCatalog': record['sequencing_primer_catalog'],
                        'sequencingPrimerLot': record['sequencing_primer_lot'],
                        'restrictionEnzymeTargetSequence': record['restriction_enzyme_target_sequence']
                    }
                else:  # RNA-seq
                    rna_3_adapter_protocol = None
                    rna_3_adapter_protocol_filename = None
                    rna_5_adapter_protocol = None
                    rna_5_adapter_protocol_filename = None
                    library_pcr_protocol = None
                    library_pcr_protocol_filename = None
                    rt_protocol = None
                    rt_protocol_filename = None
                    library_generation_protocol = None
                    library_generation_protocol_filename = None
                    if 'rna_prep_3_protocol' in record:
                        rna_3_adapter_protocol = record['rna_prep_3_protocol']
                        rna_3_adapter_protocol_filename = get_filename_from_url(rna_3_adapter_protocol,
                                                                                f"{exp_id} RNA 3 protocol")
                    if 'rna_prep_5_protocol' in record:
                        rna_5_adapter_protocol = record['rna_prep_5_protocol']
                        rna_5_adapter_protocol_filename = get_filename_from_url(rna_5_adapter_protocol,
                                                                                f"{exp_id} RNA 5 protocol")
                    if 'library_pcr_isolation_protocol' in record:
                        library_pcr_protocol = record['library_pcr_isolation_protocol']
                        library_pcr_protocol_filename = get_filename_from_url(library_pcr_protocol,
                                                                              f"{exp_id} RNA pcr protocol")
                    if 'rt_prep_protocol' in record:
                        rt_protocol = record['rt_prep_protocol']
                        rt_protocol_filename = get_filename_from_url(rt_protocol, f"{exp_id} RNA prep protocol")
                    if 'library_gen_protocol' in record:
                        library_generation_protocol = record['library_gen_protocol']
                        library_generation_protocol_filename = get_filename_from_url(library_generation_protocol,
                                                                                     f"{exp_id} RNA generate protocol")
                    exp_es['RNA-seq'] = {
                        'rnaPreparation3AdapterLigationProtocol': {
                            'url': rna_3_adapter_protocol,
                            'filename': rna_3_adapter_protocol_filename
                        },
                        'rnaPreparation5AdapterLigationProtocol': {
                            'url': rna_5_adapter_protocol,
                            'filename': rna_5_adapter_protocol_filename
                        },
                        'libraryGenerationPcrProductIsolationProtocol': {
                            'url': library_pcr_protocol,
                            'filename': library_pcr_protocol_filename
                        },
                        'preparationReverseTranscriptionProtocol': {
                            'url': rt_protocol,
                            'filename': rt_protocol_filename
                        },
                        'libraryGenerationProtocol': {
                            'url': library_generation_protocol,
                            'filename': library_generation_protocol_filename
                        },
                        'readStrand': record['read_strand'],
                        'rnaPurity260280ratio': record['rna_purity_280_ratio'],
                        'rnaPurity260230ratio': record['rna_purity_230_ratio'],
                        'rnaIntegrityNumber': record['rna_integrity_num']
                    }
                experiments[exp_id] = exp_es

            # if exp_id not in experiments:
            # dataset (study) has mutliple experiments/runs/files/specimens_list so collection information into datasets
            # and process it after iteration of all files
            dataset_id = record['study_accession']
            exps_in_dataset[exp_id] = dataset_id
            es_doc_dataset = dict()
            if dataset_id in datasets:
                es_doc_dataset = datasets[dataset_id]
            else:
                es_doc_dataset['accession'] = dataset_id
                es_doc_dataset['alias'] = record['study_alias']
                es_doc_dataset['title'] = record['study_title']
                es_doc_dataset['secondaryAccession'] = record['secondary_study_accession']
            datasets.setdefault('tmp', {})
            datasets['tmp'].setdefault(dataset_id, {})
            datasets['tmp'][dataset_id].setdefault('specimen', {})
            # noinspection PyTypeChecker
            datasets['tmp'][dataset_id]['specimen'][specimen_biosample_id] = 1

            datasets['tmp'][dataset_id].setdefault('instrument', {})
            # noinspection PyTypeChecker
            datasets['tmp'][dataset_id]['instrument'][record['instrument_model']] = 1

            datasets['tmp'][dataset_id].setdefault('center_name', {})
            # noinspection PyTypeChecker
            datasets['tmp'][dataset_id]['center_name'][record['center_name']] = 1

            datasets['tmp'][dataset_id].setdefault('archive', {})
            # noinspection PyTypeChecker
            datasets['tmp'][dataset_id]['archive'][archive] = 1

            tmp_file = {
                'url': file,
                'name': fullname,
                'fileId': filename,
                'experiment': record['experiment_accession'],
                'type': types[index],
                'size': sizes[index],
                'readableSize': convert_readable(sizes[index]),
                'archive': archive,
                'baseCount': record['base_count'],
                'readCount': record['read_count'],
                'checksum': checksums[index],
                'checksumMethod': 'md5'
            }
            datasets['tmp'][dataset_id].setdefault('file', {})
            # noinspection PyTypeChecker
            datasets['tmp'][dataset_id]['file'][fullname] = tmp_file
            tmp_exp = {
                'accession': record['experiment_accession'],
                'assayType': assay_type,
                'target': experiment_target
            }
            datasets['tmp'][dataset_id].setdefault('experiment', {})
            # noinspection PyTypeChecker
            datasets['tmp'][dataset_id]['experiment'][record['experiment_accession']] = tmp_exp
            datasets[dataset_id] = es_doc_dataset
    # end of loop for record in data:

    logger.info("The dataset list:")
    dataset_ids = sorted(list(studies_from_api.keys()))
    for index, dataset_id in enumerate(dataset_ids):
        num_exps = 0
        # noinspection PyTypeChecker
        if dataset_id in datasets['tmp'] and 'experiment' in datasets['tmp'][dataset_id]:
            # noinspection PyTypeChecker
            num_exps = len(list(datasets['tmp'][dataset_id]["experiment"].keys()))
        printed_index = index + 1
        logger.info(f"{printed_index} {dataset_id} has {studies_from_api[dataset_id]} runs from api and {num_exps} "
                    f"experiments to be processed")
    # datasets contains one artificial value set with the key as 'tmp', so need to -1
    logger.info(f"There are {len(list(datasets.keys())) -  1} datasets to be processed")

    validator = validate_experiment_record.ValidateExperimentRecord(experiments, RULESETS)
    validation_results = validator.validate()
    exp_validation = dict()
    for exp_id in sorted(experiments.keys()):
        exp_es = experiments[exp_id]
        for ruleset in RULESETS:
            if validation_results[ruleset]['detail'][exp_id]['status'] == 'error':
                # TODO logging to error
                # logger.info(f"{exp_id}\t{exps_in_dataset[exp_id]}\tExperiment\terror\t"
                #            f"{validation_results[ruleset]['detail'][exp_id]['message']}")
                pass
            else:
                # only indexing when meeting standard
                exp_validation[exp_id] = STANDARDS[ruleset]
                exp_es['standardMet'] = STANDARDS[ruleset]
                if exp_es['standardMet'] == STANDARD_FAANG:
                    exp_es['versionLastStandardMet'] = ruleset_version
                body = json.dumps(exp_es)
                insert_into_es(es, es_index_prefix, 'experiment', exp_id, body)

                # index into ES so break the loop
                break

    for file_id in files_dict.keys():
        es_file_doc = files_dict[file_id]
        # noinspection PyTypeChecker
        exp_id = es_file_doc['experiment']['accession']
        if exp_id not in exp_validation:
            continue
        es_file_doc['experiment']['standardMet'] = exp_validation[exp_id]
        body = json.dumps(es_file_doc)
        insert_into_es(es, es_index_prefix, 'file', file_id, body)
        indexed_files[file_id] = 1

    for dataset_id in datasets:
        if dataset_id == 'tmp':
            continue
        es_doc_dataset = datasets[dataset_id]
        exps = datasets['tmp'][dataset_id]["experiment"]
        only_valid_exps = dict()
        dataset_standard = STANDARD_FAANG
        experiment_type = dict()
        tech_type = dict()
        for exp_id in exps:
            if exp_id in exp_validation:
                if exp_validation[exp_id] == STANDARD_LEGACY:
                    dataset_standard = STANDARD_LEGACY
                only_valid_exps[exp_id] = exps[exp_id]
                assay_type = exps[exp_id]['assayType']
                tech_type.setdefault(TECHNOLOGIES[assay_type], 0)
                tech_type[TECHNOLOGIES[assay_type]] += 1
                experiment_type.setdefault(assay_type, 0)
                experiment_type[assay_type] += 1
            else:
                pass
        num_valid_exps = len(only_valid_exps.keys())
        if num_valid_exps == 0:
            logger.warning(f"dataset {dataset_id} has no valid experiments, skipped.")
            continue
        es_doc_dataset['standardMet'] = dataset_standard
        specimens_dict = datasets['tmp'][dataset_id]['specimen']
        species = dict()
        specimens_list = list()
        for specimen in specimens_dict:
            specimen_detail = biosample_ids[specimen]
            es_doc_specimen = {
                'biosampleId': specimen_detail['biosampleId'],
                'material': specimen_detail['material'],
                'cellType': specimen_detail['cellType'],
                'organism': specimen_detail['organism']['organism'],
                'sex': specimen_detail['organism']['sex'],
                'breed': specimen_detail['organism']['breed']
            }
            specimens_list.append(es_doc_specimen)
            species[specimen_detail['organism']['organism']['text']] = specimen_detail['organism']['organism']
        es_doc_dataset['specimen'] = sorted(specimens_list, key=lambda k: k['biosampleId'])
        es_doc_dataset['species'] = list(species.values())
        file_arr = datasets['tmp'][dataset_id]['file'].values()
        valid_files = list()
        for file_entry in sorted(file_arr, key=lambda k: k['name']):
            file_id = file_entry['fileId']
            if file_id in indexed_files:
                valid_files.append(file_entry)
        es_doc_dataset['file'] = valid_files
        es_doc_dataset['experiment'] = list(only_valid_exps.values())
        es_doc_dataset['assayType'] = list(experiment_type.keys())
        es_doc_dataset['tech'] = list(tech_type.keys())
        es_doc_dataset['instrument'] = list(datasets['tmp'][dataset_id]['instrument'].keys())
        es_doc_dataset['centerName'] = list(datasets['tmp'][dataset_id]['center_name'].keys())
        es_doc_dataset['archive'] = sorted(list(datasets['tmp'][dataset_id]['archive'].keys()))
        body = json.dumps(es_doc_dataset)
        insert_into_es(es, es_index_prefix, 'dataset', dataset_id, body)
    with open('ena_not_in_biosample.txt', 'a') as w:
        for study in new_errors:
            tmp = new_errors[study]
            for biosample in sorted(tmp.keys()):
                logger.warning(f"{biosample} from {study} does not exist in BioSamples at the moment\n")
                w.write(f"{study}\t{biosample}\n")


def get_ena_data():
    """
    This function will fetch data from ena FAANG data portal ruead_run result
    :return: json representation of data from ena
    """
    # 'https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0&dataPortal=faang&fields=all'
    url = generate_ena_api_endpoint('read_run', 'faang', 'all')
    logger.info(f"Getting data from {url}")
    response = requests.get(url).json()
    return response


def get_all_specimen_ids(host, es_index_prefix):
    """
    This function return dict with all information from the corresponding specimens
    :return: A dict with keys as BioSamples id and values as the data stored in ES
    """
    if not host.endswith(":9200"):
        host = host + ":9200"
    results = dict()
    url = f'http://{host}/{es_index_prefix}_specimen/_search?size=100000&q=standardMet:FAANG'
    response = requests.get(url).json()
    for item in response['hits']['hits']:
        results[item['_id']] = item['_source']
    return results


def get_known_errors():
    """
    This function will read file with association from study to biosample
    :return: dictionary with study as a key and biosample as a values
    """
    known_errors = dict()
    with open('ena_not_in_biosample.txt', 'r') as f:
        for line in f:
            line = line.rstrip()
            study, biosample = line.split("\t")
            known_errors.setdefault(study, {})
            known_errors[study][biosample] = 1
    return known_errors


if __name__ == "__main__":
    main()
