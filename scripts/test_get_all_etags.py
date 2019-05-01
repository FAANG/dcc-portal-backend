"""
Test cases for get_all_etags
"""
import unittest
from unittest.mock import patch

import get_all_etags


class TestGetAllEtags(unittest.TestCase):
    def test_fetch_biosample_ids(self):
        with patch('get_all_etags.requests') as mock_requests:
            get_all_etags.fetch_biosample_ids()
            self.assertEqual(mock_requests.get.call_count, 1)
