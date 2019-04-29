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
