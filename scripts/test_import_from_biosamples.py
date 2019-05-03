"""
Test cases for import_from_biosamples
"""
import unittest
from unittest.mock import patch, Mock, mock_open
from datetime import date

import import_from_biosamples


class TestImportFromBiosamples(unittest.TestCase):
    def test_get_existing_etags(self):
        return_value = {
            'hits': {
                'hits': [
                    {
                        '_source': {
                            'etag': 1,
                            'biosampleId': 1
                        }
                    },
                    {
                        '_source': {
                            'etag': 2,
                            'biosampleId': 2
                        }
                    }
                ]
            }
        }
        with patch('import_from_biosamples.requests') as mock_requests:
            tmp = mock_requests.get.return_value
            tmp.json.return_value = return_value
            results = import_from_biosamples.get_existing_etags('wp-np3-e2:9200', 'faang_build_3_')
            mock_requests.get.assert_called_with(
                'http://wp-np3-e2:9200/faang_build_3_specimen/_search?_source=biosampleId,'
                'etag&sort=biosampleId&size=100000')
            self.assertEqual(results, {1: 1, 2: 2})

    @patch('import_from_biosamples.check_is_faang')
    @patch('import_from_biosamples.fetch_single_record')
    @patch('builtins.open', new_callable=mock_open, read_data="test\ttest")
    def test_fetch_records_by_project_via_etag(self, mock_file, mock_fetch_single_record, mock_check_is_faang):
        import_from_biosamples.logger = Mock()
        import_from_biosamples.fetch_records_by_project_via_etag({'test': 'test'})
        self.assertEqual(mock_file.call_count, 1)
        self.assertEqual(import_from_biosamples.logger.info.call_count, 2)
        today = date.today().strftime('%Y-%m-%d')
        mock_file.assert_called_with(f'etag_list_{today}.txt', 'r')

        mock_fetch_single_record.return_value = {}
        mock_check_is_faang.return_value = False
        import_from_biosamples.fetch_records_by_project_via_etag({'test2': 'test2'})
        self.assertEqual(import_from_biosamples.logger.info.call_count, 4)

        mock_fetch_single_record.return_value = {
            'characteristics': {
                'Material': [
                    {
                        'text': 'organism'
                    }
                ]
            }
        }
        mock_check_is_faang.return_value = True
        import_from_biosamples.fetch_records_by_project_via_etag({'test2': 'test2'})
        self.assertEqual(import_from_biosamples.logger.info.call_count, 7)

    @patch('import_from_biosamples.check_is_faang')
    def test_fetch_records_by_project(self, mock_check_is_faang):
        mock_check_is_faang.return_value = False
        import_from_biosamples.logger = Mock()
        import_from_biosamples.ETAGS_CACHE = {1: 1}
        with patch('import_from_biosamples.requests') as mock_requests:
            tmp = mock_requests.get.return_value
            tmp.json.return_value = {
                '_embedded': {
                    'samples': [
                        {
                            'characteristics': {
                                'Material': [
                                    {'text': 'organism'}
                                ]
                            },
                            'accession': 1
                        }
                    ]
                },
                '_links': {}
            }
            import_from_biosamples.fetch_records_by_project()
            self.assertEqual(mock_requests.get.call_count, 1)
            self.assertEqual(import_from_biosamples.logger.info.call_count, 2)

            mock_check_is_faang.return_value = True
            import_from_biosamples.fetch_records_by_project()
            self.assertEqual(import_from_biosamples.logger.info.call_count, 5)

    def test_fetch_single_record(self):
        with patch('import_from_biosamples.requests') as mock_requests:
            tmp = mock_requests.get.return_value
            tmp.json.return_value = dict()
            with patch('import_from_biosamples.ETAGS_CACHE') as mock_etags_cache:
                mock_etags_cache.__getitem__.return_value = dict()
                results = import_from_biosamples.fetch_single_record(1)
            mock_requests.get.assert_called_with(
                'https://www.ebi.ac.uk/biosamples/samples/1.json?curationdomain=self.FAANG_DCC_curation')
            self.assertEqual(results, {'etag': {}})

    def test_check_is_faang(self):
        item_true = {
            'characteristics': {
                'project': [
                    {
                        'text': 'FAANG'
                    }
                ]
            }
        }
        item_false = {
            'characteristics': {
                'project': [
                    {
                    }
                ]
            }
        }
        self.assertEqual(import_from_biosamples.check_is_faang(item_true), True)
        self.assertEqual(import_from_biosamples.check_is_faang(item_false), False)

    def test_deal_with_decimal_degrees(self):
        item = {
            'characteristics': {
                'Material': [
                    {'text': 'organism'}
                ]
            }
        }
        self.assertEqual(import_from_biosamples.deal_with_decimal_degrees(item), item)

        item['characteristics']['birth location latitude'] = [{'unit': 'decimal degrees'}]
        item['characteristics']['birth location longitude'] = [{'unit': 'decimal degrees'}]
        self.assertEqual(import_from_biosamples.deal_with_decimal_degrees(item), item)

        item['characteristics']['birth location latitude'] = [{'unit': 'decimal degree'}]
        item['characteristics']['birth location longitude'] = [{'unit': 'decimal degree'}]
        item['accession'] = 1
        with patch('import_from_biosamples.requests') as mock_requests:
            import_from_biosamples.deal_with_decimal_degrees(item)
            mock_requests.get.assert_called_with('https://www.ebi.ac.uk/biosamples/samples/'
                                                 '1.json?curationdomain=self.FAANG_DCC_curation')

        item['characteristics']['Material'][0]['text'] = 'specimen'
        self.assertEqual(import_from_biosamples.deal_with_decimal_degrees(item), item)

    @patch('import_from_biosamples.add_organism_info_for_specimen')
    @patch('import_from_biosamples.get_alternative_id')
    @patch('import_from_biosamples.parse_relationship')
    @patch('import_from_biosamples.get_health_status')
    @patch('import_from_biosamples.extract_custom_field')
    @patch('import_from_biosamples.populate_basic_biosample_info')
    @patch('import_from_biosamples.check_existence')
    @patch('import_from_biosamples.insert_into_es')
    def test_process_organisms(self, mock_insert_into_es, mock_check_existence, mock_populate_basic_biosample_info,
                               mock_extract_custom_field, mock_get_health_status, mock_parse_relationship,
                               mock_get_alternative_id, mock_add_organism_info_for_specimen):
        es_instance = Mock()
        es_index_prefix = 'test'
        import_from_biosamples.ORGANISM = {'test': {'characteristics': {}}}
        import_from_biosamples.process_organisms(es_instance, es_index_prefix)
        self.assertEqual(mock_check_existence.call_count, 22)
        self.assertEqual(mock_insert_into_es.call_count, 1)
        self.assertEqual(mock_populate_basic_biosample_info.call_count, 1)
        self.assertEqual(mock_extract_custom_field.call_count, 1)
        self.assertEqual(mock_get_health_status.call_count, 1)
        self.assertEqual(mock_parse_relationship.call_count, 1)
        self.assertEqual(mock_get_alternative_id.call_count, 1)
        self.assertEqual(mock_add_organism_info_for_specimen.call_count, 1)

    @patch('import_from_biosamples.insert_into_es')
    @patch('import_from_biosamples.get_alternative_id')
    @patch('import_from_biosamples.extract_custom_field')
    @patch('import_from_biosamples.populate_basic_biosample_info')
    @patch('import_from_biosamples.get_filename_from_url')
    @patch('import_from_biosamples.check_existence')
    @patch('import_from_biosamples.parse_relationship')
    def test_process_specimens(self, mock_parse_relationship, mock_check_existence, mock_get_filename_from_url,
                               mock_populate_basic_biosample_info, mock_extract_custom_field, mock_get_alternative_id,
                               mock_insert_into_es):
        es_instance = Mock()
        es_index_prefix = 'test'
        mock_parse_relationship.return_value = {
            'derivedFrom': {
                'test': 'test'
            }
        }
        import_from_biosamples.SPECIMEN_FROM_ORGANISM = {'test': {'characteristics': {}}}
        import_from_biosamples.process_specimens(es_instance, es_index_prefix)
        self.assertEqual(mock_parse_relationship.call_count, 1)
        self.assertEqual(mock_check_existence.call_count, 22)
        self.assertEqual(mock_get_filename_from_url.call_count, 1)
        self.assertEqual(mock_populate_basic_biosample_info.call_count, 1)
        self.assertEqual(mock_extract_custom_field.call_count, 1)
        self.assertEqual(mock_get_alternative_id.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 1)

    @patch('import_from_biosamples.insert_into_es')
    @patch('import_from_biosamples.get_alternative_id')
    @patch('import_from_biosamples.extract_custom_field')
    @patch('import_from_biosamples.populate_basic_biosample_info')
    @patch('import_from_biosamples.fetch_single_record')
    @patch('import_from_biosamples.get_filename_from_url')
    @patch('import_from_biosamples.check_existence')
    @patch('import_from_biosamples.parse_relationship')
    def test_process_cell_specimens(self, mock_parse_relationship, mock_check_existence, mock_get_filename_from_url,
                                    mock_fetch_single_record, mock_populate_basic_biosample_info,
                                    mock_extract_custom_field, mock_get_alternative_id, mock_insert_into_es):
        es_instance = Mock()
        es_index_prefix = 'test'
        mock_parse_relationship.return_value = {
            'derivedFrom': {
                'test': 'test'
            }
        }
        import_from_biosamples.CELL_SPECIMEN = {'test': {'characteristics': {}}}
        import_from_biosamples.process_cell_specimens(es_instance, es_index_prefix)
        self.assertEqual(mock_parse_relationship.call_count, 1)
        self.assertEqual(mock_check_existence.call_count, 4)
        self.assertEqual(mock_get_filename_from_url.call_count, 1)
        self.assertEqual(mock_fetch_single_record.call_count, 0)
        self.assertEqual(mock_populate_basic_biosample_info.call_count, 1)
        self.assertEqual(mock_extract_custom_field.call_count, 1)
        self.assertEqual(mock_get_alternative_id.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 1)

    @patch('import_from_biosamples.populate_basic_biosample_info')
    @patch('import_from_biosamples.insert_into_es')
    @patch('import_from_biosamples.get_alternative_id')
    @patch('import_from_biosamples.fetch_single_record')
    @patch('import_from_biosamples.get_filename_from_url')
    @patch('import_from_biosamples.check_existence')
    @patch('import_from_biosamples.parse_relationship')
    def test_process_cell_cultures(self, mock_parse_relationship, mock_check_existence, mock_get_filename_from_url,
                                   mock_fetch_single_record, mock_get_alternative_id, mock_insert_into_es,
                                   mock_populate_basic_biosample_info):
        es_instance = Mock()
        es_index_prefix = 'test'
        mock_parse_relationship.return_value = {
            'derivedFrom': {
                'test': 'test'
            }
        }
        import_from_biosamples.CELL_CULTURE = {'test': {'characteristics': {}}}
        import_from_biosamples.process_cell_cultures(es_instance, es_index_prefix)
        self.assertEqual(mock_parse_relationship.call_count, 2)
        self.assertEqual(mock_check_existence.call_count, 9)
        self.assertEqual(mock_get_filename_from_url.call_count, 1)
        self.assertEqual(mock_fetch_single_record.call_count, 2)
        self.assertEqual(mock_get_alternative_id.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 1)
        self.assertEqual(mock_populate_basic_biosample_info.call_count, 1)

    @patch('import_from_biosamples.insert_into_es')
    @patch('import_from_biosamples.get_alternative_id')
    @patch('import_from_biosamples.extract_custom_field')
    @patch('import_from_biosamples.populate_basic_biosample_info')
    @patch('import_from_biosamples.get_filename_from_url')
    @patch('import_from_biosamples.check_existence')
    @patch('import_from_biosamples.parse_relationship')
    def test_process_pool_specimen(self, mock_parse_relationship, mock_check_existence, mock_get_filename_from_url,
                                   mock_populate_basic_biosample_info, mock_extract_custom_field,
                                   mock_get_alternative_id, mock_insert_into_es):
        es_instance = Mock()
        es_index_prefix = 'test'
        mock_parse_relationship.return_value = {
            'derivedFrom': {
                'test': 'test'
            }
        }
        import_from_biosamples.POOL_SPECIMEN = {'test': {'characteristics': {}}}
        import_from_biosamples.SPECIMEN_FROM_ORGANISM = {'test': {}}
        import_from_biosamples.SPECIMEN_ORGANISM_RELATIONSHIP = {'test': 'test'}
        import_from_biosamples.ORGANISM_FOR_SPECIMEN = {'test': {
            'organism': {
                'text': 'text',
                'ontologyTerms': 'ontologyTerms'
            },
            'sex': {
                'text': 'text',
                'ontologyTerms': 'ontologyTerms'
            },
            'breed': {
                'text': 'text',
                'ontologyTerms': 'ontologyTerms'
            }
        }}
        import_from_biosamples.process_pool_specimen(es_instance, es_index_prefix)
        self.assertEqual(mock_parse_relationship.call_count, 1)
        self.assertEqual(mock_check_existence.call_count, 9)
        self.assertEqual(mock_get_filename_from_url.call_count, 1)
        self.assertEqual(mock_populate_basic_biosample_info.call_count, 1)
        self.assertEqual(mock_extract_custom_field.call_count, 1)
        self.assertEqual(mock_get_alternative_id.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 1)

    @patch('import_from_biosamples.insert_into_es')
    @patch('import_from_biosamples.get_alternative_id')
    @patch('import_from_biosamples.extract_custom_field')
    @patch('import_from_biosamples.populate_basic_biosample_info')
    @patch('import_from_biosamples.get_filename_from_url')
    @patch('import_from_biosamples.check_existence')
    @patch('import_from_biosamples.parse_relationship')
    def test_process_cell_lines(self, mock_parse_relationship, mock_check_existence, mock_get_filename_from_url,
                                mock_populate_basic_biosample_info, mock_extract_custom_field, mock_get_alternative_id,
                                mock_insert_into_es):
        es_instance = Mock()
        es_index_prefix = 'test'
        mock_parse_relationship.return_value = {
            'derivedFrom': ['test']
        }
        import_from_biosamples.CELL_LINE = {'test': {'characteristics': {}}}
        import_from_biosamples.process_cell_lines(es_instance, es_index_prefix)
        self.assertEqual(mock_parse_relationship.call_count, 1)
        self.assertEqual(mock_check_existence.call_count, 22)
        self.assertEqual(mock_get_filename_from_url.call_count, 1)
        self.assertEqual(mock_populate_basic_biosample_info.call_count, 1)
        self.assertEqual(mock_extract_custom_field.call_count, 1)
        self.assertEqual(mock_get_alternative_id.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 1)

    def test_check_existence(self):
        item = {
            'characteristics': {
                'test': [
                    {
                        'text': 1,
                        'unit': 1,
                        'ontologyTerms': [1]
                    }
                ]
            }
        }
        self.assertEqual(import_from_biosamples.check_existence(item, 'test', 'text'), 1)
        self.assertEqual(import_from_biosamples.check_existence(item, 'test', 'unit'), 1)
        self.assertEqual(import_from_biosamples.check_existence(item, 'test', 'ontologyTerms'), 1)
        self.assertEqual(import_from_biosamples.check_existence(item, 'test2', 'ontologyTerms'), None)

    def test_populate_basic_biosample_info(self):
        doc = dict()
        item = {
            'name': 'name',
            'accession': 'SAMEA12345',
            'etag': 'etag',
            'organization': [
                {}
            ],
            'release': '2019-01-01',
            'update': '2019-01-01'
        }
        should_be_equal = {
            'name': 'name',
            'biosampleId': 'SAMEA12345',
            'description': None,
            'etag': 'etag',
            'id_number': '12345',
            'material': {'ontologyTerms': None, 'text': None},
            'organization': [{'URL': None, 'name': None, 'role': None}],
            'project': None,
            'releaseDate': '2019-01-01',
            'updateDate': '2019-01-01',
            'availability': None
        }
        self.assertEqual(import_from_biosamples.populate_basic_biosample_info(doc, item), should_be_equal)

    def test_extract_custom_field(self):
        doc = dict()
        item = {
            'characteristics': {}
        }
        material_type = 'test'
        import_from_biosamples.logger = Mock()
        self.assertEqual(import_from_biosamples.extract_custom_field(doc, item, material_type), {})
        self.assertEqual(import_from_biosamples.logger.error.call_count, 1)

        item['characteristics'] = {
            'test1': 'test1',
            'test2': ['test2'],
            'test3': {
                'text': 'text',
                'unit': 'unit',
                'ontologyTerms': 'ontologyTerms'
            }
        }
        material_type = 'organism'
        should_be_equal = {
            'customField': [
                {'name': 'test1', 'value': 'test1'},
                {'name': 'test2', 'value': 'test2'},
                {
                    'name': 'test3',
                    'ontologyTerms': 'ontologyTerms',
                    'unit': 'unit',
                    'value': 'text'
                }
            ]
        }
        self.assertEqual(import_from_biosamples.extract_custom_field(doc, item, material_type), should_be_equal)

    def test_get_health_status(self):
        item1 = {
            'characteristics': {
                'health status': [
                    {
                        'text': 'text',
                        'ontologyTerms': ['test']
                    }
                ]
            }
        }
        item2 = {
            'characteristics': {
                'health status at collection': [
                    {
                        'text': 'text',
                        'ontologyTerms': ['test']
                    }
                ]
            }
        }
        value_to_return = [{'text': 'text', 'ontologyTerms': 'test'}]
        self.assertEqual(import_from_biosamples.get_health_status(item1), value_to_return)
        self.assertEqual(import_from_biosamples.get_health_status(item2), value_to_return)
        with patch('import_from_biosamples.logger') as mock_logger:
            item3 = {
                'characteristics': {}
            }
            self.assertEqual(import_from_biosamples.get_health_status(item3), [])
            self.assertEqual(mock_logger.debug.call_count, 1)

    def test_parse_relationship(self):
        item = {}
        self.assertEqual(import_from_biosamples.parse_relationship(item), {})

        item['accession'] = 'accession'
        item['relationships'] = [
            {
                'type': 'EBI equivalent BioSample',
                'source': 'accession',
                'target': 'target'
            }
        ]
        should_return = {
            'EBI equivalent BioSample': {'target': 1},
            'ebiEquivalentBiosample': {'target': 1}
        }
        self.assertEqual(import_from_biosamples.parse_relationship(item), should_return)

        item['relationships'][0]['source'] = 'source'
        should_return = {
            'EBI equivalent BioSample': {'source': 1},
            'ebiEquivalentBiosample': {'source': 1}
        }
        self.assertEqual(import_from_biosamples.parse_relationship(item), should_return)

        item['relationships'][0]['type'] = 'type'
        self.assertEqual(import_from_biosamples.parse_relationship(item), {'type': {'target': 2}})

    def test_get_alternative_id(self):
        relationships = {
            'sameAs': [1, 2, 3],
            'EBI equivalent BioSample': [4, 5, 6]
        }
        self.assertEqual(import_from_biosamples.get_alternative_id(relationships), [1, 2, 3, 4, 5, 6])

    @patch('import_from_biosamples.get_health_status')
    @patch('import_from_biosamples.check_existence')
    def test_add_organism_info_for_specimen(self, mock_check_existence, mock_get_health_status):
        item = {
            'accession': 'test'
        }
        import_from_biosamples.add_organism_info_for_specimen('test', item)
        self.assertEqual(mock_check_existence.call_count, 6)
        self.assertEqual(mock_get_health_status.call_count, 1)

    def test_parse_date(self):
        self.assertEqual(import_from_biosamples.parse_date('2018.19.22'), '2018.19.22')
        self.assertEqual(import_from_biosamples.parse_date('2018-19-22'), '2018-19-22')

    @patch('import_from_biosamples.validate_total_sample_records')
    @patch('import_from_biosamples.utils.insert_into_es')
    def test_insert_into_es(self, mock_insert_into_es, mock_validate_total_sample_records):
        data = {
            'test': {}
        }
        return_value = {
            'test': {
                'detail': {
                    'test': {
                        'status': 'error',
                        'type': 'error',
                        'message': 'error'
                    },
                }
            }
        }
        mock_validate_total_sample_records.return_value = return_value
        import_from_biosamples.RULESETS = ['test']
        import_from_biosamples.logger = Mock()
        import_from_biosamples.insert_into_es(data, 'faang_build_3_', 'organism', 'es')
        self.assertEqual(mock_validate_total_sample_records.call_count, 1)
        mock_validate_total_sample_records.assert_called_with({'test': {}}, 'organism', ['test'])
        self.assertEqual(import_from_biosamples.logger.error.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 1)
        mock_insert_into_es.assert_called_with('es', 'faang_build_3_', 'organism', 'test', '{}')

        return_value['test']['detail']['test']['status'] = 'not error'
        mock_validate_total_sample_records.return_value = return_value
        import_from_biosamples.STANDARDS['test'] = 'test'
        import_from_biosamples.insert_into_es(data, 'faang_build_3_', 'organism', 'es')
        self.assertEqual(mock_validate_total_sample_records.call_count, 2)
        self.assertEqual(import_from_biosamples.logger.error.call_count, 1)
        self.assertEqual(mock_insert_into_es.call_count, 2)
        mock_insert_into_es.assert_called_with('es', 'faang_build_3_', 'organism', 'test', '{"standardMet": "test"}')

    def test_clean_elasticsearch(self):
        return_value_to_be_cleaned = {
            'hits': {
                'hits': [
                    {
                        '_id': 1,
                        '_source': {
                            'standardMet': 'FAANG'
                        }
                    }
                ]
            }
        }
        return_value_not_to_be_cleaned = {
            'hits': {
                'hits': [
                    {
                        '_id': 1,
                        '_source': {
                            'standardMet': 'Legacy (basic)'
                        }
                    }
                ]
            }
        }
        es_instance = Mock()
        es_instance.search.return_value = return_value_to_be_cleaned
        import_from_biosamples.clean_elasticsearch('test', es_instance)
        es_instance.search.assert_called_with(_source='_id,standardMet', index='test', size=100000)
        self.assertEqual(es_instance.delete.call_count, 1)

        es_instance.search.return_value = return_value_not_to_be_cleaned
        import_from_biosamples.clean_elasticsearch('test', es_instance)
        self.assertEqual(es_instance.delete.call_count, 1)
