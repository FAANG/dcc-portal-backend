"""
Test cases for fetch_articles
"""
import unittest
from unittest.mock import Mock
from unittest.mock import patch
import io

import fetch_articles


class TestFetchArticles(unittest.TestCase):
    def test_print_statement_with_true(self):
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            fetch_articles.print_statement('test')
        self.assertIn('test', fake_stdout.getvalue())

    def test_add_new_pair(self):
        target_dict = dict()
        id_to_check = 1
        target_list = [1, 2, 3]
        should_be_equal = {
            1: target_list
        }
        fetch_articles.add_new_pair(target_dict, id_to_check, target_list)
        self.assertEqual(target_dict, should_be_equal)
        fetch_articles.add_new_pair(target_dict, id_to_check, target_list)
        self.assertEqual(target_dict, should_be_equal)
        fetch_articles.add_new_pair(target_dict, id_to_check, [4, 5, 6])
        should_be_equal[1] = [1, 2, 3, 4, 5, 6]
        self.assertEqual(target_dict, should_be_equal)

    def test_update_records(self):
        record_dict = {
            1: [
                {
                    'pmcid': 'pmcid',
                    'doi': 'doi',
                    'title': 'title',
                    'year': 'year',
                    'journal': 'journal'
                }
            ]
        }
        es_instance = Mock()
        fetch_articles.update_records(record_dict, 'specimen', es_instance)
        self.assertEqual(es_instance.update.call_count, 1)
        es_instance.update.assert_called_with(body={'doc': {'paperPublished': 'true',
                                                            'publishedArticles': [{'pubmedId': 'pmcid', 'doi': 'doi',
                                                                                   'title': 'title', 'year': 'year',
                                                                                   'journal': 'journal'}]}},
                                              doc_type='_doc', id=1, index='specimen')

    def test_retrieve_ids(self):
        es_instance = Mock()
        es_instance.search.return_value = {
            'hits': {
                'hits': [
                    {
                        '_id': 1
                    },
                    {
                        '_id': 2
                    }
                ]
            }
        }
        result = fetch_articles.retrieve_ids('organism', es_instance)
        self.assertEqual(result, [1, 2])
        self.assertEqual(es_instance.search.call_count, 1)
