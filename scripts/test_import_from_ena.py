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

    def test_get_known_errors(self):
        pass

    def test_check_existsence(self):
        item = {
            'test': [],
            'test3': 'test3'
        }
        self.assertEqual(import_from_ena.check_existsence(item, 'test'), None)
        self.assertEqual(import_from_ena.check_existsence(item, 'test3'), 'test3')
        self.assertEqual(import_from_ena.check_existsence(item, 'test2'), None)
