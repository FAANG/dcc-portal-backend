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

    def test_create_experiment_protocol(self):
        inner_value = {'experimentTarget': 'polyA RNA', 'experimentalProtocol': {
            'filename': 'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
            'url': 'https://support.illumina.com/content/dam/illumina-support/documents/documentation/'
                   'chemistry_documentation/samplepreps_truseq/truseqstrandedmrna/'
                   'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf'}, 'sequencingLocationLatitude': {
            'unit': 'decimal degrees', 'text': '55.923954'}, 'extractionProtocol': {
            'filename': 'ROSLIN_SOP_Isolation_of_RNA_from__preserved_tissue_20161108.pdf',
            'url': 'ftp://ftp.faang.ebi.ac.uk/ftp/protocols/samples/'
                   'ROSLIN_SOP_Isolation_of_RNA_from__preserved_tissue_20161108.pdf'},
                       'sequencingLocationLongitude': {'unit': 'decimal degrees', 'text': '-3.173114'},
                       'sampleStorageProcessing': 'cryopreservation, other', 'sampleStorage': 'RNAlater, frozen',
                       'libraryPreparationLocation': 'Edinburgh Genomics, The University of Edinburgh, EH9 3FL, '
                                                     'Edinburgh, Scotland', 'assayType': 'transcription profiling by '
                                                                                         'high throughput sequencing',
                       'standardMet': 'FAANG', 'libraryPreparationLocationLongitude': {'unit': 'decimal degrees',
                                                                                       'text': '-3.173114'},
                       'libraryPreparationDate': {'unit': '', 'text': 'not provided'},
                       'sequencingLocation': 'Edinburgh Genomics, The University of Edinburgh, EH9 3FL, Edinburgh, '
                                             'Scotland', 'libraryPreparationLocationLatitude': {
                'unit': 'decimal degrees', 'text': '55.923954'}, 'versionLastStandardMet': '3.6',
                       'accession': 'ERX2403514', 'samplingToPreparationInterval': {'unit': 'days', 'text': '120.0'},
                       'RNA-seq': {'rnaPurity260280ratio': '2.04', 'rnaPreparation3AdapterLigationProtocol': {
                           'filename': 'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
                           'url': 'https://support.illumina.com/content/dam/illumina-support/documents/'
                                  'documentation/chemistry_documentation/samplepreps_truseq/truseqstrandedmrna/'
                                  'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf'},
                                   'libraryGenerationPcrProductIsolationProtocol': {
                                       'filename': 'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
                                       'url': 'https://support.illumina.com/content/dam/illumina-support/'
                                              'documents/documentation/chemistry_documentation/samplepreps_truseq/'
                                              'truseqstrandedmrna/truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf'
                                   }, 'readStrand': 'mate 2 sense', 'rnaIntegrityNumber': '8.5',
                                   'rnaPurity260230ratio': '2.19', 'libraryGenerationProtocol': {
                               'filename': 'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
                               'url': 'https://support.illumina.com/content/dam/illumina-support/documents/'
                                      'documentation/chemistry_documentation/samplepreps_truseq/truseqstrandedmrna/'
                                      'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf'},
                                   'rnaPreparation5AdapterLigationProtocol': {
                                       'filename': 'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
                                       'url': 'https://support.illumina.com/content/dam/illumina-support/documents/'
                                              'documentation/chemistry_documentation/samplepreps_truseq/'
                                              'truseqstrandedmrna/truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf'
                                   }, 'preparationReverseTranscriptionProtocol': {
                               'filename': 'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
                               'url': 'https://support.illumina.com/content/dam/illumina-support/documents/'
                                      'documentation/chemistry_documentation/samplepreps_truseq/truseqstrandedmrna/'
                                      'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf'}},
                       'sequencingDate': {'unit': '', 'text': 'not provided'}}
        return_value = {
            'hits': {
                'hits': [
                    {
                        '_source': inner_value
                    }
                ]
            }
        }
        es_staging = Mock()
        es_staging.search.return_value = return_value
        logger = Mock()
        test_object = create_protocols.CreateProtocols(es_staging, logger)
        test_object.create_experiment_protocol()
        self.assertEqual(es_staging.search.call_count, 1)
        es_staging.index.assert_called_with(body={'name': 'preparationReverseTranscriptionProtocol',
                                                  'experimentTarget': 'polyA RNA',
                                                  'assayType': 'transcription profiling by high throughput sequencing',
                                                  'key': 'preparationReverseTranscriptionProtocol-'
                                                         'transcriptionprofilingbyhighthroughputsequencing-polyARNA',
                                                  'url': 'https://support.illumina.com/content/dam/illumina-support/'
                                                         'documents/documentation/chemistry_documentation/'
                                                         'samplepreps_truseq/truseqstrandedmrna/'
                                                         'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
                                                  'filename': 'truseq-stranded-mrna-sample-prep-guide-15031047-e.pdf',
                                                  'experiments': [{'accession': 'ERX2403514',
                                                                   'sampleStorage': 'RNAlater, frozen',
                                                                   'sampleStorageProcessing':
                                                                       'cryopreservation, other'}]},
                                            doc_type='_doc', id='preparationReverseTranscriptionProtocol'
                                                                '-transcriptionprofilingbyhighthroughputsequencing'
                                                                '-polyARNA', index='protocol_files')