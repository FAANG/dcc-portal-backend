import unittest
from fetch_articles import determine_article_id, parse_field


class TestFetchArticle(unittest.TestCase):
    def test_determine_article_id(self):
        # Nicole's paper related to PRJEB35307
        epmc_hit = {
            'id': '31861495', 'source': 'MED', 'pmid': '31861495', 'doi': '10.3390/genes11010003',
            'title': 'Functionally Annotating Regulatory Elements in the Equine Genome Using Histone Mark ChIP-Seq.',
            'authorString': 'Kingsley NB, Kern C, Creppe C, Hales EN, Zhou H, Kalbfleisch TS, MacLeod JN, Petersen JL, '
                            'Finno CJ, Bellone RR.',
            'journalTitle': 'Genes (Basel)', 'issue': '1', 'journalVolume': '11', 'pubYear': '2019',
            'journalIssn': '2073-4425', 'pubType': 'journal article', 'isOpenAccess': 'N', 'inEPMC': 'N', 'inPMC': 'N',
            'hasPDF': 'N', 'hasBook': 'N', 'hasSuppl': 'N', 'citedByCount': 0, 'hasReferences': 'N',
            'hasTextMinedTerms': 'N', 'hasDbCrossReferences': 'N', 'hasLabsLinks': 'N', 'hasTMAccessionNumbers': 'N',
            'firstIndexDate': '2019-12-28', 'firstPublicationDate': '2019-12-18'
        }
        # without pmcid, use pmid as literature record id
        self.assertEqual(determine_article_id(epmc_hit), '31861495')
        # without pmcid and pmid, use doi
        epmc_hit.pop('pmid')
        self.assertEqual(determine_article_id(epmc_hit), '10.3390_genes11010003')
        # without all three, use the id used by ePMC
        epmc_hit.pop('doi')
        self.assertEqual(determine_article_id(epmc_hit), '31861495')
        # even no ePMC id，then this publication can not be used, return empty
        epmc_hit.pop('id')
        self.assertEqual(determine_article_id(epmc_hit), '')
        # pmcid has the highest priority
        epmc_hit['pmcid'] = 'PMC888888'
        epmc_hit['pmid'] = '31861495'
        self.assertEqual(determine_article_id(epmc_hit), 'PMC888888')

    def test_parse_field(self):
        epmc_hit = {
            'id': '31861495', 'source': 'MED', 'pmid': '31861495', 'doi': '10.3390/genes11010003',
            'title': 'Functionally Annotating Regulatory Elements in the Equine Genome Using Histone Mark ChIP-Seq.',
            'authorString': 'Kingsley NB, Kern C, Creppe C, Hales EN, Zhou H, Kalbfleisch TS, MacLeod JN, Petersen JL, '
                            'Finno CJ, Bellone RR.',
            'journalTitle': 'Genes (Basel)', 'issue': '1', 'journalVolume': '11', 'pubYear': '2019',
            'journalIssn': '2073-4425', 'pubType': 'journal article', 'isOpenAccess': 'N', 'inEPMC': 'N', 'inPMC': 'N',
            'hasPDF': 'N', 'hasBook': 'N', 'hasSuppl': 'N', 'citedByCount': 0, 'hasReferences': 'N',
            'hasTextMinedTerms': 'N', 'hasDbCrossReferences': 'N', 'hasLabsLinks': 'N', 'hasTMAccessionNumbers': 'N',
            'firstIndexDate': '2019-12-28', 'firstPublicationDate': '2019-12-18'
        }
        es_doc = dict()
        # the api key does not exist, no change made to es_doc
        es_doc = parse_field(es_doc, epmc_hit, 'pubmidId', 'pubmidId')
        self.assertEqual(len(es_doc), 0)
        es_doc = parse_field(es_doc, epmc_hit, 'pubmidId', 'pmid')
        self.assertEqual(len(es_doc), 1)
        es_doc = parse_field(es_doc, epmc_hit, 'doi', 'doi')
        expected = {
            'pubmidId': '31861495',
            'doi': '10.3390/genes11010003'
        }
        self.assertDictEqual(es_doc, expected)



if __name__ == '__main__':
    unittest.main()
