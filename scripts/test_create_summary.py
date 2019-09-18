"""
Test cases for create_summary
"""
import unittest
from unittest.mock import Mock
from unittest.mock import patch

import create_summary


class TestCreateSummary(unittest.TestCase):
    def test_create_organism_summary(self):
        inner_value = {'_score': 1.0, '_index': 'faang_build_3_organism', '_type': '_doc', '_source': {
            'pregnancyLength': {'text': '280', 'unit': 'days'},
            'organization': [{'URL': 'https://www.iastate.edu/', 'name': 'Iowa State University', 'role': 'funder'},
                             {'URL': 'https://www.iastate.edu/', 'name': 'Iowa State University',
                              'role': 'institution'}, {'URL': 'https://www.ncsu.edu/',
                                                       'name': 'North Carolina State University',
                                                       'role': 'biomaterial provider'},
                             {'URL': 'https://www.ncsu.edu/', 'name': 'North Carolina State University',
                              'role': 'funder'}], 'biosampleId': 'SAMEA4999491', 'breed': {'text': 'Angus',
                                                                                           'ontologyTerms':
                                                                                               'LBO_0000017'},
            'updateDate': '2018-09-12', 'healthStatus': [{'text': 'normal', 'ontologyTerms': 'PATO_0000461'}],
            'pedigree': None, 'project': 'FAANG', 'deliveryTiming': None, 'birthLocationLatitude': {'text': None,
                                                                                                    'unit': None},
            'alternativeId': [], 'customField': [{'name': 'Submission description', 'value': 'RNAseq data from '
                                                                                             'blood and muscle samples '
                                                                                             'of calves (Bos taurus)'},
                                                 {'name': 'Submission title', 'value': 'ISU-FAANG-SERAO-Bovine-100818'},
                                                 {'name': 'Submission identifier', 'value': 'GSB-517'}],
            'releaseDate': '2018-09-12', 'name': 'BTA_ISU_A9', 'deliveryEase': None, 'material': {'text': 'organism',
                                                                                                  'ontologyTerms':
                                                                                                      'OBI_0100026'},
            'birthLocation': None, 'organism': {'text': 'Bos taurus', 'ontologyTerms': '9913'}, 'birthWeight': {
                'text': None, 'unit': None}, 'etag': '"05d3f61a4afbb0909e968c453cb028d04"', 'standardMet': 'FAANG',
            'id_number': '4999491', 'description': 'calve female, 110 days of age, angus', 'availability': None,
            'birthDate': {'text': '2015-02-27', 'unit': 'YYYY-MM-DD'}, 'versionLastStandardMet': '3.6',
            'sex': {'text': 'female', 'ontologyTerms': 'PATO_0000383'}, 'birthLocationLongitude': {'text': None,
                                                                                                   'unit': None},
            'placentalWeight': {'text': None, 'unit': None}}, '_id': 'SAMEA4999491'}
        return_value = {
            'hits': {
                'hits': [
                    inner_value
                ]
            }
        }
        with patch('create_summary.requests') as mock_requests:
            tmp = mock_requests.get.return_value
            tmp.json.return_value = return_value
            es_instance = Mock()
            logger = Mock()
            test_object = create_summary.CreateSummary(es_instance, logger)
            test_object.create_organism_summary()
            self.assertEqual(mock_requests.get.call_count, 1)
            mock_requests.get.assert_called_with('http://test.faang.org/api/organism/_search/?size=100000')
            es_instance.index.assert_called_with(body='{"sexSummary": [{"name": "female", "value": 1}], '
                                                      '"paperPublishedSummary": [{"name": "yes", "value": 0}, '
                                                      '{"name": "no", "value": 1}], '
                                                      '"standardSummary": [{"name": "FAANG", "value": 1}], '
                                                      '"organismSummary": [{"name": "Bos taurus", "value": 1}], '
                                                      '"breedSummary": [{"speciesName": "Bos taurus", '
                                                      '"speciesValue": [{"breedsName": "Angus", "breedsValue": 1}]}]}',
                                                 doc_type='_doc', id='summary_organism', index='summary_organism')

    def test_create_specimen_summary(self):
        inner_value = {'_score': 1.0, '_index': 'faang_build_8_specimen', '_type': '_doc', '_source': {
            'project': 'FAANG', 'organization': [{'URL': 'http://www.baif.org.in/', 'name': 'BAIF',
                                                  'role': 'biomaterial provider '},
                                                 {'URL': 'http://www.baif.org.in/', 'name': 'BAIF',
                                                  'role': 'institution '},
                                                 {'URL': 'http://www.bbsrc.ac.uk/', 'name': 'BBSRC', 'role': 'funder '},
                                                 {'URL': 'http://www.ccmb.res.in/',
                                                  'name': 'Centre for Cellular and Molecular Biology',
                                                  'role': 'biomaterial provider '},
                                                 {'URL': 'http://www.ccmb.res.in/',
                                                  'name': 'Centre for Cellular and Molecular Biology',
                                                  'role': 'institution '}, {'URL': 'http://www.ebi.ac.uk/',
                                                                            'name': 'EMBL-EBI', 'role': 'curator '},
                                                 {'URL': 'http://www.dbtindia.nic.in/',
                                                  'name': 'Indian Department of Biotechnology', 'role': 'funder '},
                                                 {'URL': 'http://www.roslin.ed.ac.uk/',
                                                  'name': 'The Roslin Institute and Royal Dick School of '
                                                          'Veterinary Studies', 'role': 'biomaterial provider '},
                                                 {'URL': 'http://www.roslin.ed.ac.uk/',
                                                  'name': 'The Roslin Institute and Royal Dick School of '
                                                          'Veterinary Studies', 'role': 'institution '}],
            'biosampleId': 'SAMEA103886898', 'cellType': {'text': 'saliva-secreting gland',
                                                          'ontologyTerms': 'http://purl.obolibrary.org/obo/'
                                                                           'UBERON_0001044'},
            'organism': {'sex': {'text': 'male', 'ontologyTerms': 'PATO_0000384'}, 'biosampleId': 'SAMEA103886572',
                         'organism': {'text': 'Bubalus bubalis',
                                      'ontologyTerms': 'http://purl.obolibrary.org/obo/NCBITaxon_89462'},
                         'healthStatus': [], 'breed': {'text': 'Bhadawari',
                                                       'ontologyTerms': 'http://purl.obolibrary.org/obo/LBO_0001046'}},
            'alternativeId': [], 'updateDate': '2018-06-06', 'specimenFromOrganism': {'specimenCollectionDate': {
                'text': '2015-03-31', 'unit': 'YYYY-MM-DD'}, 'numberOfPieces': {'text': None, 'unit': None},
                'specimenCollectionProtocol': {'url': 'http://ftp.faang.ebi.ac.uk/ftp/protocols/samples/'
                                                      'ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.pdf',
                                               'filename': 'ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.pdf'},
                'specimenSize': {'text': None, 'unit': None}, 'healthStatusAtCollection': [],
                'developmentalStage': {'text': 'adult', 'ontologyTerms': 'http://www.ebi.ac.uk/efo/EFO_0001272'},
                'animalAgeAtCollection': {'text': '5', 'unit': 'year'}, 'specimenPictureUrl': [], 'fastedStatus': None,
                'organismPart': {'text': 'saliva-secreting gland',
                                 'ontologyTerms': 'http://purl.obolibrary.org/obo/UBERON_0001044'},
                'specimenWeight': {'text': None, 'unit': None}, 'gestationalAgeAtSampleCollection': {'text': None,
                                                                                                     'unit': None},
                'specimenVolume': {'text': None, 'unit': None}}, 'standardMet': 'FAANG', 'id_number': '103886898',
            'description': 'salivary glands from an adult male, 5 years old, Bhadawari, India', 'material': {
                'text': 'specimen from organism', 'ontologyTerms': 'http://purl.obolibrary.org/obo/OBI_0001479'},
            'etag': '"0d6a4c87f796682502924b21467b34421"', 'availability': None, 'derivedFrom': 'SAMEA103886572',
            'customField': [], 'releaseDate': '2016-10-18', 'name': 'BBU_RI_BHAM1_Sal'}, '_id': 'SAMEA103886898'}
        return_value = {
            'hits': {
                'hits': [
                    inner_value
                ]
            }
        }
        with patch('create_summary.requests') as mock_requests:
            tmp = mock_requests.get.return_value
            tmp.json.return_value = return_value
            es_instance = Mock()
            logger = Mock()
            test_object = create_summary.CreateSummary(es_instance, logger)
            test_object.create_specimen_summary()
            self.assertEqual(mock_requests.get.call_count, 1)
            mock_requests.get.assert_called_with('http://test.faang.org/api/specimen/_search/?size=100000')
            es_instance.index.assert_called_with(body='{"sexSummary": [{"name": "male", "value": 1}], '
                                                      '"paperPublishedSummary": [{"name": "yes", "value": 0}, '
                                                      '{"name": "no", "value": 1}], '
                                                      '"standardSummary": [{"name": "FAANG", "value": 1}], '
                                                      '"cellTypeSummary": [{"name": "saliva-secreting gland", '
                                                      '"value": 1}], "organismSummary": [{"name": "Bubalus bubalis", '
                                                      '"value": 1}], "materialSummary": [{'
                                                      '"name": "specimen from organism", "value": 1}], '
                                                      '"breedSummary": [{"speciesName": "Bubalus bubalis", '
                                                      '"speciesValue": [{"breedsName": "Bhadawari", '
                                                      '"breedsValue": 1}]}]}', doc_type='_doc', id='summary_specimen',
                                                 index='summary_specimen')

    def test_create_dataset_summary(self):
        with patch('create_summary.requests') as mock_requests:
            es_instance = Mock()
            logger = Mock()
            test_object = create_summary.CreateSummary(es_instance, logger)
            test_object.create_dataset_summary()
            self.assertEqual(mock_requests.get.call_count, 1)
            mock_requests.get.assert_called_with('http://test.faang.org/api/dataset/_search/?size=100000')
            self.assertEqual(es_instance.index.call_count, 1)

    def test_create_file_summary(self):
        inner_value = {'_score': 1.0, '_index': 'faang_build_8_file', '_type': '_doc', '_source': {
            'size': '2151112762', 'run': {'alias': '170223_NS500422_0446_AHT2MCBGXY-C-3-PecMus_S3.R2.fastq.gz',
                                          'instrument': 'NextSeq 500', 'accession': 'SRR6713582',
                                          'platform': 'ILLUMINA'}, 'name': 'SRR6713582_1.fastq.gz', 'type': 'fastq.gz',
            'checksumMethod': 'md5', 'readableSize': '2.0GB', 'specimen': 'SAMN08476464', 'readCount': '53707995',
            'submission': 'SRA658981', 'checksum': '7e176cb507a1f2ccc36f06a327cc6f46', 'species': {
                'text': 'Gallus gallus', 'ontologyTerms': 'http://purl.obolibrary.org/obo/NCBITaxon_9031'},
            'archive': 'ENA', 'updateDate': '2018-02-13', 'url': 'ftp.sra.ebi.ac.uk/vol1/fastq/SRR671/002/SRR6713582/'
                                                                 'SRR6713582_1.fastq.gz', 'experiment': {
                'target': 'open_chromatin_region', 'accession': 'SRX3687019', 'assayType': 'ATAC-seq',
                'standardMet': 'Legacy'}, 'study': {'alias': 'PRJNA433154', 'secondaryAccession': 'SRP132746',
                                                    'title': 'Gallus gallus ATAC-seq', 'accession': 'PRJNA433154',
                                                    'type': 'ATAC-seq'}, 'releaseDate': '2018-02-13',
            'baseCount': '8001231325'}, '_id': 'SRR6713582_1'}
        return_value = {
            'hits': {
                'hits': [
                    inner_value
                ]
            }
        }
        with patch('create_summary.requests') as mock_requests:
            tmp = mock_requests.get.return_value
            tmp.json.return_value = return_value
            es_instance = Mock()
            logger = Mock()
            test_object = create_summary.CreateSummary(es_instance, logger)
            test_object.create_file_summary()
            self.assertEqual(mock_requests.get.call_count, 1)
            mock_requests.get.assert_called_with('http://test.faang.org/api/file/_search/?size=100000')
            es_instance.index.assert_called_with(body='{"standardSummary": [{"name": "Legacy", "value": 1}], '
                                                      '"paperPublishedSummary": [{"name": "yes", "value": 0}, '
                                                      '{"name": "no", "value": 1}], '
                                                      '"specieSummary": [{"name": "Gallus gallus", "value": 1}], '
                                                      '"assayTypeSummary": [{"name": "ATAC-seq", "value": 1}]}',
                                                 doc_type='_doc', id='summary_file', index='summary_file')