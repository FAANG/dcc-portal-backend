"""
Test cases for import_from_ena script
"""
import unittest
from unittest.mock import patch, Mock, mock_open

import import_from_ena


class TestImportFromEna(unittest.TestCase):
    def test_determine_file_and_source(self):
        pass

    def test_get_ena_data(self):
        pass

    def test_get_all_specimen_ids(self):
        pass

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
