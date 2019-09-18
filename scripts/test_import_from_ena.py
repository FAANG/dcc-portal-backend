"""
Test cases for import_from_ena script
"""
import unittest
from unittest.mock import patch, Mock, mock_open
from click.testing import CliRunner

import import_from_ena


class TestImportFromEna(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open)
    @patch('import_from_ena.utils.insert_into_es')
    @patch('import_from_ena.validate_total_experiment_records')
    @patch('import_from_ena.get_filename_from_url')
    @patch('import_from_ena.convert_readable')
    @patch('import_from_ena.check_existsence')
    @patch('import_from_ena.get_ruleset_version')
    @patch('import_from_ena.get_known_errors')
    @patch('import_from_ena.get_all_specimen_ids')
    @patch('import_from_ena.get_ena_data')
    @patch('import_from_ena.Elasticsearch')
    @patch('import_from_ena.logger')
    def test_main(self, mock_logger, mock_elasticsearch, mock_get_ena_data, mock_get_all_specimen_ids,
                  mock_get_known_errors, mock_get_ruleset_version, mock_check_existsence, mock_convert_readable,
                  mock_get_filename_from_url, mock_validate_total_experiment_records, mock_insert_into_es, mock_file):
        data_to_return = {'study_accession': 'PRJEB9561', 'secondary_study_accession': 'ERP010682',
                          'sample_accession': 'SAMEA3540911', 'secondary_sample_accession': 'ERS848060',
                          'experiment_accession': 'ERX1096241', 'run_accession': 'ERR1017174',
                          'submission_accession': 'ERA485606', 'tax_id': '9823', 'scientific_name': 'Sus scrofa',
                          'instrument_platform': 'ILLUMINA', 'instrument_model': 'Illumina HiSeq 2000',
                          'library_name': 'pig_NC', 'nominal_length': '130', 'library_layout': 'PAIRED',
                          'library_strategy': 'Bisulfite-Seq', 'library_source': 'GENOMIC',
                          'library_selection': 'Reduced Representation', 'read_count': '19187239',
                          'base_count': '1880349422', 'center_name': 'Choi', 'first_public': '2015-09-16',
                          'last_updated': '2018-11-16',
                          'experiment_title': 'Illumina HiSeq 2000 paired end sequencing; Swine DNA methylation'
                                              ' study using RRBS : FAANG',
                          'study_title': 'Swine DNA methylation study using RRBS : FAANG',
                          'study_alias': 'ena-STUDY-Choi-08-06-2015-16:09:30:780-431',
                          'experiment_alias': 'ena-EXPERIMENT-Choi-16-09-2015-03:17:08:811-1',
                          'run_alias': 'ena-RUN-Choi-16-09-2015-03:17:08:812-1', 'fastq_bytes': '822938429;682783787',
                          'fastq_md5': '25f30eff2d2f4f1c53797beb7170ad43;8708320f5acd1e36bd3f7a0378ab1a75',
                          'fastq_ftp': 'ftp.sra.ebi.ac.uk/vol1/fastq/ERR101/004/ERR1017174/ERR1017174_1.fastq.gz;'
                                       'ftp.sra.ebi.ac.uk/vol1/fastq/ERR101/004/ERR1017174/ERR1017174_2.fastq.gz',
                          'fastq_aspera': 'fasp.sra.ebi.ac.uk:/vol1/fastq/ERR101/004/ERR1017174/ERR1017174_1.fastq.gz;'
                                          'fasp.sra.ebi.ac.uk:/vol1/fastq/ERR101/004/ERR1017174/ERR1017174_2.fastq.gz',
                          'fastq_galaxy': 'ftp.sra.ebi.ac.uk/vol1/fastq/ERR101/004/ERR1017174/ERR1017174_1.fastq.gz;'
                                          'ftp.sra.ebi.ac.uk/vol1/fastq/ERR101/004/ERR1017174/ERR1017174_2.fastq.gz',
                          'submitted_bytes': '765311562;624753533',
                          'submitted_md5': '102637334f05538c915dc39119c790c0;aefb27720c72acc6629ddbb6d7c78fdc',
                          'submitted_ftp': 'ftp.sra.ebi.ac.uk/vol1/run/ERR101/ERR1017174/NC.1.fq.gz;ftp.sra.ebi.ac.uk/'
                                           'vol1/run/ERR101/ERR1017174/NC.2.fq.gz',
                          'submitted_aspera': 'fasp.sra.ebi.ac.uk:/vol1/run/ERR101/ERR1017174/NC.1.fq.gz;'
                                              'fasp.sra.ebi.ac.uk:/vol1/run/ERR101/ERR1017174/NC.2.fq.gz',
                          'submitted_galaxy': 'ftp.sra.ebi.ac.uk/vol1/run/ERR101/ERR1017174/'
                                              'NC.1.fq.gz;ftp.sra.ebi.ac.uk/vol1/run/ERR101/ERR1017174/NC.2.fq.gz',
                          'submitted_format': 'FASTQ;FASTQ', 'sra_bytes': '1122782668', 'sra_md5': 'fc96e032c7e5d9b46'
                                                                                                   'b12fc932b50dab6',
                          'sra_ftp': 'ftp.sra.ebi.ac.uk/vol1/err/ERR101/004/ERR1017174',
                          'sra_aspera': 'fasp.sra.ebi.ac.uk:/vol1/err/ERR101/004/ERR1017174',
                          'sra_galaxy': 'ftp.sra.ebi.ac.uk/vol1/err/ERR101/004/ERR1017174', 'cram_index_ftp': '',
                          'cram_index_aspera': '', 'cram_index_galaxy': '', 'sample_alias': 'SAMEA3540911',
                          'broker_name': '', 'sample_title': 'pig_NC', 'assay_type': 'methylation profiling by high '
                                                                                     'throughput sequencing',
                          'sample_storage': 'frozen, -70 freezer', 'sample_storage_processing': 'cryopreservation in '
                                                                                                'liquid nitrogen '
                                                                                                '(dead tissue)',
                          'sample_prep_interval': '12.0', 'sample_prep_interval_units': 'weeks',
                          'experimental_protocol': '', 'extraction_protocol': 'ftp://ftp.faang.ebi.ac.uk/ftp/protocols/'
                                                                              'samples/KU_SOP_Preperation_of_Genomic_'
                                                                              'DNA_from_Small_Samples_20170516.pdf',
                          'library_prep_location': 'Shenzhen, China', 'library_prep_latitude': '22.54554',
                          'library_prep_longitude': '114.0683', 'library_prep_date': '2013-08-20',
                          'library_prep_date_format': 'YYYY-MM-DD', 'sequencing_location': 'Shenzhen, China',
                          'sequencing_latitude': '22.54554', 'sequencing_longitude': '114.0683',
                          'sequencing_date': '2013-08-20', 'sequencing_date_format': 'YYYY-MM-DD',
                          'experiment_target': 'DNA methylation', 'bisulfite_protocol': 'http://www.zymoresearch.com/'
                                                                                        'downloads/dl/file/id/197/'
                                                                                        'd5007d.pdf',
                          'pcr_isolation_protocol': 'https://www.epigenesys.eu/images/stories/protocols/pdf/'
                                                    '20160127163832_p70.pdf',
                          'bisulfite_percent': '99.6', 'chip_protocol': '', 'chip_ab_provider': '',
                          'chip_ab_catalog': '', 'chip_ab_lot': '', 'library_max_fragment_size': '',
                          'library_min_fragment_size': '', 'rna_prep_3_protocol': '', 'rna_prep_5_protocol': '',
                          'library_pcr_isolation_protocol': '', 'rt_prep_protocol': '', 'library_gen_protocol': '',
                          'read_strand': '', 'rna_purity_280_ratio': '', 'rna_purity_230_ratio': '',
                          'rna_integrity_num': '', 'dnase_protocol': '', 'transposase_protocol': '',
                          'restriction_enzyme': 'MspI', 'restriction_site': '', 'faang_library_selection': 'RRBS',
                          'hi_c_protocol': ''}
        mock_get_ena_data.return_value = [
            data_to_return
        ]
        mock_get_all_specimen_ids.return_value = {
            'SAMEA3540911': {'derivedFrom': 'SAMEA103886117', 'cellType': {'ontologyTerms': 'http://purl.obolibrary.org'
                                                                                            '/obo/UBERON_0002509',
                                                                           'text': 'mesenteric lymph node'},
                             'project': 'FAANG', 'customField': [], 'availability': None, 'organism': {'breed': {
                    'ontologyTerms': 'http://purl.obolibrary.org/obo/LBO_0000358', 'text': 'Duroc'},
                    'biosampleId': 'SAMEA103886117', 'healthStatus': [{'ontologyTerms': 'http://purl.obolibrary.org/obo/'
                                                                                        'PATO_0000461',
                                                                       'text': 'normal'}], 'sex': {
                        'ontologyTerms': 'PATO_0000384', 'text': 'male'}, 'organism': {
                        'ontologyTerms': 'http://purl.obolibrary.org/obo/NCBITaxon_9823', 'text': 'Sus scrofa'}},
                             'id_number': '103886115', 'organization': [{'URL': 'http://www.bbsrc.ac.uk/',
                                                                         'name': 'BBSRC', 'role': 'funder'},
                                                                        {'URL': 'http://www.ebi.ac.uk/',
                                                                         'name': 'EMBL-EBI', 'role': 'curator'},
                                                                        {'URL': 'http://www.roslin.ed.ac.uk/',
                                                                         'name': 'The Roslin Institute and Royal Dick '
                                                                                 'School of Veterinary Studies',
                                                                         'role': 'biomaterial provider'},
                                                                        {'URL': 'http://www.roslin.ed.ac.uk/',
                                                                         'name': 'The Roslin Institute and Royal Dick '
                                                                                 'School of Veterinary Studies',
                                                                         'role': 'institution'}],
                             'releaseDate': '2016-10-13', 'standardMet': 'FAANG', 'specimenFromOrganism': {
                    'specimenSize': {'unit': None, 'text': None}, 'specimenVolume': {'unit': None, 'text': None},
                    'organismPart': {'ontologyTerms': 'http://purl.obolibrary.org/obo/UBERON_0002509',
                                     'text': 'mesenteric lymph node'}, 'specimenCollectionDate': {'unit': 'YYYY-MM-DD',
                                                                                                  'text': '2013-08-02'},
                    'specimenCollectionProtocol': {'filename': 'ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.'
                                                               'pdf',
                                                   'url': 'http://ftp.faang.ebi.ac.uk/ftp/protocols/samples/ROSLIN_SOP_'
                                                          'Harvest_of_Large_Animal_Tissues_20160516.pdf'},
                    'developmentalStage': {'ontologyTerms': 'http://purl.obolibrary.org/obo/UBERON_0000112',
                                           'text': 'juvenile stage'}, 'specimenWeight': {'unit': None, 'text': None},
                    'gestationalAgeAtSampleCollection': {'unit': None, 'text': None},
                    'numberOfPieces': {'unit': None, 'text': None}, 'animalAgeAtCollection': {'unit': 'month',
                                                                                              'text': '4'},
                    'healthStatusAtCollection': [{'ontologyTerms': 'http://purl.obolibrary.org/obo/PATO_0000461',
                                                  'text': 'normal'}], 'fastedStatus': 'fed'},
                             'updateDate': '2018-06-06', 'name': 'SUS_RI_DUR22-51', 'alternativeId': [],
                             'description': 'LN mesenteric', 'versionLastStandardMet': '3.6',
                             'biosampleId': 'SAMEA103886115', 'etag': '"04f308eda221c7a8873f00945efaeeb0e"',
                             'material': {'ontologyTerms': 'http://purl.obolibrary.org/obo/OBI_0001479',
                                          'text': 'specimen from organism'}}}
        mock_check_existsence.return_value = None
        mock_get_filename_from_url.return_value = None
        mock_get_ruleset_version.return_value = None
        mock_convert_readable.return_value = None
        runner = CliRunner()
        result = runner.invoke(import_from_ena.main, '--es_hosts wp-np3-e2:9200;wp-np3-e3:9200 --es_index_prefix '
                                                     'faang_build_3_')
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(mock_logger.info.call_count, 9)
        self.assertEqual(mock_logger.warning.call_count, 0)
        self.assertEqual(mock_logger.error.call_count, 0)
        self.assertEqual(mock_elasticsearch.call_count, 1)
        self.assertEqual(mock_get_ena_data.call_count, 1)
        self.assertEqual(mock_get_all_specimen_ids.call_count, 1)
        self.assertEqual(mock_get_known_errors.call_count, 1)
        self.assertEqual(mock_get_ruleset_version.call_count, 1)
        self.assertEqual(mock_check_existsence.call_count, 13)
        self.assertEqual(mock_convert_readable.call_count, 4)
        self.assertEqual(mock_get_filename_from_url.call_count, 3)
        self.assertEqual(mock_validate_total_experiment_records.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 4)
        self.assertEqual(mock_file.call_count, 1)
        mock_file.assert_called_with('ena_not_in_biosample.txt', 'a')

    def test_determine_file_and_source(self):
        record = {
            'fastq_ftp': 'test'
        }
        self.assertEqual(import_from_ena.determine_file_and_source(record), ('ftp', 'fastq'))
        record = {}
        self.assertEqual(import_from_ena.determine_file_and_source(record), ('', ''))

    @patch('import_from_ena.requests')
    def test_get_ena_data(self, mock_requests):
        import_from_ena.get_ena_data()
        self.assertEqual(mock_requests.get.call_count, 1)
        mock_requests.get.assert_called_with('https://www.ebi.ac.uk/ena/portal/api/search/'
                                             '?result=read_run&format=JSON&limit=0&dataPortal=faang&fields=all')

    @patch('import_from_ena.requests')
    def test_get_all_specimen_ids(self, mock_requests):
        tmp =mock_requests.get.return_value
        tmp.json.return_value = {
            'hits': {
                'hits': [
                    {
                        '_id': '_id',
                        '_source': '_source'
                    }
                ]
            }
        }
        results = import_from_ena.get_all_specimen_ids('wp-np3-e2', 'faang_build_3')
        self.assertEqual(results, {'_id': '_source'})
        self.assertEqual(mock_requests.get.call_count, 1)
        mock_requests.get.assert_called_with('http://wp-np3-e2:9200/faang_build_3_specimen/_search?size=100000')

    @patch('builtins.open', new_callable=mock_open, read_data="test\ttest")
    def test_get_known_errors(self, mock_file):
        result = import_from_ena.get_known_errors()
        mock_file.assert_called_with('ena_not_in_biosample.txt', 'r')
        self.assertEqual(result, {'test': {'test': 1}})

    def test_check_existsence(self):
        item = {
            'test': [],
            'test3': 'test3'
        }
        self.assertEqual(import_from_ena.check_existsence(item, 'test'), None)
        self.assertEqual(import_from_ena.check_existsence(item, 'test3'), 'test3')
        self.assertEqual(import_from_ena.check_existsence(item, 'test2'), None)
