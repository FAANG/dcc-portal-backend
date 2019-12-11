import unittest
import import_from_ena


class TestImportFromEna(unittest.TestCase):
    def test_replace_alias_with_accession(self):
        # correct study, given accession, return the same accession
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31483', 'ERX3212573'), 'ERX3212573')
        # correct study, given alias, return the correct accession
        self.assertEqual(import_from_ena.replace_alias_with_accession
                         ('PRJEB31483', 'ISU-USDA-FAANG-AlvMac-ChIPseq-C10CON2H-27me3'), 'ERX3212573')
        # wrong study, given alias, return empty string as expect to be within same study
        self.assertEqual(import_from_ena.replace_alias_with_accession
                         ('PRJEB31482', 'ISU-USDA-FAANG-AlvMac-ChIPseq-C10CON2H-27me3'), '')
        # wrong study, given accession, return accession as could refer to any valid experiment accession
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31482', 'ERX3212573'), 'ERX3212573')
        # NCBI and DDBJ experiment accession also accepted
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31482', 'SRX357350'), 'SRX357350')
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31482', 'DRX000228'), 'DRX000228')
        # wrong experiment accession
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31482', 'ERR357350'), '')
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31482', 'SSRX357350'), '')
        self.assertEqual(import_from_ena.replace_alias_with_accession('PRJEB31482', 'DRX000228wrong'), '')

