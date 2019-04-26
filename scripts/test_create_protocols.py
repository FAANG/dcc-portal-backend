"""
Test cases for create_protocols module
"""
import unittest
from unittest.mock import Mock

import create_protocols


class TestCreateProtocols(unittest.TestCase):
    def test_create_sample_protocol(self):
        inner_value = {
            '_id': 'SAMEA103886115',
            '_source': {
                'specimenFromOrganism': {
                    'specimenCollectionProtocol': {
                        'filename': 'ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.pdf',
                        'url': 'http://ftp.faang.ebi.ac.uk/ftp/protocols/samples/'
                               'ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.pdf'
                    },
                },
                'cellType': {
                    'text': 'mesenteric lymph node'
                },
                'organism': {
                    'organism': {
                        'text': 'Sus scrofa'
                    },
                    'breed': {
                        'text': 'Duroc'
                    }
                },
                'derivedFrom': 'SAMEA103886117'
            }
        }
        return_value = {
            'hits': {
                'hits': [inner_value,]
            }
        }
        es_staging = Mock()
        es_staging.search.return_value = return_value
        logger = Mock()
        test_object = create_protocols.CreateProtocols(es_staging, logger)
        test_object.create_sample_protocol()
        self.assertEqual(es_staging.search.call_count, 1)
        es_staging.index.assert_called_with(body={
            'specimen': [{'id': 'SAMEA103886115', 'organismPartCellType': 'mesenteric lymph node',
                          'organism': 'Sus scrofa', 'breed': 'Duroc', 'derivedFrom': 'SAMEA103886117'}],
            'universityName': 'Roslin Institute (Edinburgh, UK)', 'protocolDate': '2016',
            'protocolName': 'Harvest of Large Animal Tissues',
            'key': 'ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.pdf',
            'url': 'http://ftp.faang.ebi.ac.uk/ftp/protocols/samples/'
                   'ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.pdf',
            'protocolType': 'samples'}, doc_type='_doc', id='ROSLIN_SOP_Harvest_of_Large_Animal_Tissues_20160516.pdf',
            index='protocol_samples')
