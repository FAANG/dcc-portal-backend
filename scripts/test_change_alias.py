"""
Test cases for change_alias module
"""
import unittest
from unittest.mock import Mock
from unittest.mock import patch
import io

import change_alias


class TestChangeAlias(unittest.TestCase):
    def test_run_with_same_prefix(self):
        es_staging = Mock()
        es_staging.indices.get_alias.return_value = {'faang_build_3_organism': {'aliases': {'organism': {}}},
                                                     'faang_build_3_file': {'aliases': {'file': {}}},
                                                     'faang_build_3_specimen': {'aliases': {'specimen': {}}},
                                                     'faang_build_3_dataset': {'aliases': {'dataset': {}}},
                                                     'faang_build_3_experiment': {'aliases': {'experiment': {}}}}
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            change_alias_object = change_alias.ChangeAliases('faang_build_3_', es_staging)
            change_alias_object.run()
        self.assertEqual(es_staging.indices.get_alias.call_count, 1)
        es_staging.indices.get_alias.assert_called_with(name='organism,file,specimen,dataset,experiment')
        self.assertIn('faang_build_3_experiment -> experiment', fake_stdout.getvalue())
        self.assertIn('faang_build_3_organism -> organism', fake_stdout.getvalue())
        self.assertIn('faang_build_3_file -> file', fake_stdout.getvalue())
        self.assertIn('faang_build_3_specimen -> specimen', fake_stdout.getvalue())
        self.assertIn('faang_build_3_dataset -> dataset', fake_stdout.getvalue())
        self.assertIn('Prefix is already in use, exiting!', fake_stdout.getvalue())

    def test_run_with_new_prefix(self):
        es_staging = Mock()
        es_staging.indices.get_alias.return_value = {'faang_build_3_organism': {'aliases': {'organism': {}}},
                                                     'faang_build_3_file': {'aliases': {'file': {}}},
                                                     'faang_build_3_specimen': {'aliases': {'specimen': {}}},
                                                     'faang_build_3_dataset': {'aliases': {'dataset': {}}},
                                                     'faang_build_3_experiment': {'aliases': {'experiment': {}}}}
        with patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            change_alias_object = change_alias.ChangeAliases('faang_build_8_', es_staging)
            change_alias_object.run()
        es_staging.indices.get_alias.return_value = {'faang_build_8_organism': {'aliases': {'organism': {}}},
                                                     'faang_build_8_file': {'aliases': {'file': {}}},
                                                     'faang_build_8_specimen': {'aliases': {'specimen': {}}},
                                                     'faang_build_8_dataset': {'aliases': {'dataset': {}}},
                                                     'faang_build_8_experiment': {'aliases': {'experiment': {}}}}
        self.assertEqual(es_staging.indices.get_alias.call_count, 2)
        es_staging.indices.get_alias.assert_called_with(name='organism,file,specimen,dataset,experiment')
        self.assertEqual(es_staging.indices.update_aliases.call_count, 5)
        es_staging.indices.update_aliases.assert_called_with(body={'actions':
                                                                       [{'remove': {
                                                                           'index': 'faang_build_3_experiment',
                                                                           'alias': 'experiment'}},
                                                                           {'add': {
                                                                               'index': 'faang_build_8_experiment',
                                                                               'alias': 'experiment'}}]})
