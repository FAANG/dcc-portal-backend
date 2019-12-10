import unittest
import import_from_ena


class TestImportFromEna(unittest.TestCase):
    def test_replace_alias_with_accession(self):
        # correct study, given accession, return the same accession
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31483', 'ERX3212573'), 'ERX3212573')
        # correct study, given alias, return the correct accession
        self.assertEqual(import_from_ena.replace_alias_with_accession
                         ('PRJEB31483', 'ISU-USDA-FAANG-AlvMac-ChIPseq-C10CON2H-27me3'), 'ERX3212573')
        # wrong study, given accession, return empty string
        self.assertEqual(import_from_ena.replace_alias_with_accession
                         ('PRJEB31482', 'ISU-USDA-FAANG-AlvMac-ChIPseq-C10CON2H-27me3'), '')
        # wrong study, given alias, return empty string
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31482', 'ERX3212573'), '')
