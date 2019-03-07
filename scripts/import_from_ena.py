from elasticsearch import Elasticsearch
import requests
import sys

from validate_sample_record import *
from misc import *

TECHNOLOGIES = {
    'ATAC-seq': 'ATAC-seq',
    'methylation profiling by high throughput sequencing': 'BS-seq',
    'ChIP-seq': 'ChIP-seq',
    'DNase-Hypersensitivity seq': 'DNase-seq',
    'Hi-C': 'Hi-C',
    'microRNA profiling by high throughput sequencing': 'RNA-seq',
    'RNA-seq of coding RNA': 'RNA-seq',
    'RNA-seq of non coding RNA': 'RNA-seq',
    'transcription profiling by high throughput sequencing': 'RNA-seq',
    'whole genome sequencing assay': 'WGS'
}

RULESETS = ["FAANG Experiments", "FAANG Legacy Experiments"]
STANDARDS = {
    'FAANG Experiments': 'FAANG',
    'FAANG Legacy Experiments': 'Legacy'
}
DATA_SOURCES = ['fastq', 'sra', 'cram_index']
DATA_TYPES = ['ftp', 'galaxy', 'aspera']


def main():
    """
    Main function that will import data from ena
    :return:
    """
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    data = get_ena_data()
    biosample_ids = get_all_specimen_ids()
    if not biosample_ids:
        # TODO log to error
        print('BioSample IDs were not imported')
        sys.exit(0)
    known_errors = get_known_errors()
    new_errors = dict()
    ruleset_version = get_ruleset_version()
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
        if assay_type == '':
            if library_strategy == 'Bisulfite-Seq':
                assay_type = 'methylation profiling by high throughput sequencing'
            elif library_strategy == 'DNase-Hypersensitivity':
                assay_type = 'DNase-Hypersensitivity seq'
        if assay_type == 'whole genome sequencing':
            assay_type == 'whole genome sequencing assay'
        if assay_type == 'ATAC-seq':
            if not len(experiment_target) > 0:
                experiment_target = 'open_chromatin_region'
        elif assay_type == '"methylation profiling by high throughput sequencing':
            if not len(experiment_target) > 0:
                experiment_target = 'DNA methylation'
        elif assay_type == 'DNase-Hypersensitivity seq':
            if not len(experiment_target) > 0:
                experiment_target = 'open_chromatin_region'
        elif assay_type == 'Hi-C':
            if not len(experiment_target) > 0:
                experiment_target = 'chromatin'
        elif assay_type == 'whole genome sequencing assay':
            if not len(experiment_target) > 0:
                experiment_target = 'input DNA'
        file_type = ''
        source_type = ''
        try:
            for data_source in DATA_SOURCES:
                for my_type in DATA_TYPES:
                    key_to_check = f"{data_source}_{my_type}"
                    if key_to_check in record and record[key_to_check] != '':
                        file_type = my_type
                        source_type = data_source
                        raise BreakIt
        except:
            pass
        if file_type == '':
            continue
        if source_type == 'fastq':
            archive = 'ENA'
        elif source_type == 'cram_index':
            archive = 'CRAM'
        else:
            archive = 'SRA'
        files = record[f"{source_type}_{file_type}"].split(";")
        types = record['submitted_format'].split(";")
        sizes = record[f"{source_type}_bytes"].split(";")
        checksums = record[f"{source_type}_md5"].split(";")
        for index, file in enumerate(files):
            specimen_biosample_id = record['sample_accession']
            if specimen_biosample_id not in biosample_ids:
                if specimen_biosample_id not in known_errors[record['study_accession']]:
                    new_errors.setdefault(record['study_accession'], {})
                    new_errors[record['study_accession']][specimen_biosample_id] = 1
                continue
            fullname = file.split("/")[-1]
            filename = fullname.split(".")[0]
            es_doc = {
                'specimen': specimen_biosample_id,
                'organism': biosample_ids[record['sample_accession']]['organism']['biosampleId'],
                'species': biosample_ids[record['sample_accession']]['organism']['organism'],
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
                    'target': experiment_target
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
                    'type': record['study_type'],
                    'secondaryAccession': record['secondary_study_accession']
                }
            }
            files_dict[filename] = es_doc
            exp_id = record['experiment_accession']
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
                    'assayType': assay_type,
                    'experimentTarget': experiment_target,
                    'sampleStorage': record['sample_storage'],
                    'sampleStorageProcessing': record['sample_storage_processing'],
                    'samplingToPreparationInterval': {
                        'text': record['sample_prep_interval'],
                        'unit': record['sample_prep_interval_units']
                    },
                    'experimentalProtocol': {
                        'url': experiment_protocol,
                        'filename': experiment_protocol_filename
                    },
                    'extractionProtocol': {
                        'url': extraction_protocol,
                        'filename': extraction_protocol_filename
                    },
                    'libraryPreparationLocation': record['library_prep_location'],
                    'libraryPreparationDate': {
                        'text': record['library_prep_date'],
                        'unit': record['library_prep_date_format']
                    },
                    'sequencingLocation': record['sequencing_location'],
                    'sequencingDate': {
                        'text': record['sequencing_date'],
                        'unit': record['sequencing_date_format']
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
                section_info = dict()
                if assay_type == 'ATAC-seq':
                    transposase_protocol = record['transposase_protocol']
                    transposase_protocol_filename = get_filename_from_url(transposase_protocol,
                                                                          f"{exp_id} ATAC transposase protocol")
                    exp_es['ATAC-seq'] = {
                        'transposaseProtocol': {
                            'url': transposase_protocol,
                            'filename': transposase_protocol_filename
                        }
                    }
                elif assay_type == 'methylation profiling by high throughput sequencing':
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
                            'filanem': pcr_isolation_protocol_filename
                        },
                        'bisulfiteConversionPercent': record['bisulfite_percent'],
                        'restrictionEnzyme': record['restriction_enzyme']
                    }
                    if exp_es['BS-seq']['librarySelection'] == 'RBBS':
                        exp_es['BS-seq']['librarySelection'] = 'RRBS'
                elif assay_type == 'ChIP-seq':
                    chip_protocol = record['chip_protocol']
                    chip_protocol_filename = get_filename_from_url(chip_protocol, f"{exp_id} chip protocol")
                    section_info = {
                        'chipProtocol': {
                            'url': chip_protocol,
                            'filanem': chip_protocol_filename
                        },
                        'libraryGenerationMaxFragmentSizeRange': record['library_max_fragment_size'],
                        'libraryGenerationMinFragmentSizeRange': record['library_min_fragment_size']
                    }
                    if experiment_target.lower() == 'input dna':
                        exp_es['ChiP-seq input DNA'] = section_info
                    else:
                        section_info['chipAntibodyProvider'] = record['chip_ab_provider']
                        section_info['chipAntibodyCatalog'] = record['chip_ab_catalog']
                        section_info['chipAntibodyLot'] = record['chip_ab_lot']
                        exp_es['ChiP-seq histone'] = section_info
                elif assay_type == 'DNase-Hypersensitivity seq"':
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
                elif assay_type == 'Hi-C':
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
                elif assay_type == 'whole genome sequencing assay':
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
                else:
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
                                                                                     f"{exp_id} RNA generation protocol")
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
                        }
                    }
                experiments[exp_id] = exp_es
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
            datasets['tmp'][dataset_id]['specimen'][specimen_biosample_id] = 1

            datasets['tmp'][dataset_id].setdefault('instrument', {})
            datasets['tmp'][dataset_id]['instrument'][record['instrument_model']] = 1

            datasets['tmp'][dataset_id].setdefault('center_name', {})
            datasets['tmp'][dataset_id]['center_name'][record['center_name']] = 1

            datasets['tmp'][dataset_id].setdefault('archive', {})
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
                'readCount': record['read_count']
            }
            datasets['tmp'][dataset_id].setdefault('file', {})
            datasets['tmp'][dataset_id]['file'][fullname] = tmp_file
            tmp_exp = {
                'accession': record['experiment_accession'],
                'assayType': assay_type,
                'target': experiment_target
            }
            datasets['tmp'][dataset_id].setdefault('experiment', {})
            datasets['tmp'][dataset_id]['experiment'][record['experiment_accession']] = tmp_exp
            datasets[dataset_id] = es_doc_dataset
    pass


def get_ena_data():
    """
    This function will fetch data from ena
    :return: json representation of data from ena
    """
    response = requests.get('https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0'
                            '&dataPortal=faang&fields=all').json()
    return response


def get_all_specimen_ids():
    """
    This function return dict with all information from specimens
    :return: json representation of data from specimens
    """
    results = dict()
    response = requests.get('http://wp-np3-e2.ebi.ac.uk:9200/specimen/_search?size=100000').json()
    for item in response['hits']['hits']:
        results[item['_id']] = item['_source']
    return results


def get_known_errors():
    """
    This function will read file with associtation from study to biosample
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
