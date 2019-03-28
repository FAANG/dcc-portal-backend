import unittest
import misc


class TestMisc(unittest.TestCase):
    def test_get_filename_from_url(self):
        self.assertEqual(misc.get_filename_from_url("", "acc"), "")
        self.assertEqual(misc.get_filename_from_url("ftp://ftp.ebi.ac.uk/ROSLIN_SOP_analysis_20190318.pdf", "acc"),
                         "ROSLIN_SOP_analysis_20190318.pdf")
        self.assertEqual(misc.get_filename_from_url("http://website.com/a.pdf", "acc"), "a.pdf")
        self.assertEqual(misc.get_filename_from_url("http://pdf.acrobat.com/S187403", "acc"),
                         "http://pdf.acrobat.com/S187403")
        self.assertEqual(misc.get_filename_from_url("http://www.acrobat.pdfreader.com/", "acc"),
                         "http://www.acrobat.pdfreader.com/")

    def test_to_lower_camel_case(self):
        self.assertEqual(misc.to_lower_camel_case('country'), 'country')
        self.assertEqual(misc.to_lower_camel_case('Disease'), 'disease')
        self.assertEqual(misc.to_lower_camel_case('Physiological status'), 'physiologicalStatus')
        self.assertEqual(misc.to_lower_camel_case('test__string'), 'testString')
        self.assertEqual(misc.to_lower_camel_case('test _1'), 'test1')

    def test_to_lower_camel_case_types(self):
        self.assertRaises(TypeError, misc.to_lower_camel_case, 34)
        self.assertRaises(TypeError, misc.to_lower_camel_case, True)

    def test_from_lower_camel_case(self):
        self.assertEqual(misc.from_lower_camel_case('country'), 'country')
        self.assertEqual(misc.from_lower_camel_case('disease'), 'disease')
        self.assertEqual(misc.from_lower_camel_case('physiologicalStatus'), 'physiological status')
        self.assertEqual(misc.from_lower_camel_case('testString'), 'test string')
        self.assertEqual(misc.from_lower_camel_case('test1'), 'test1')

    def test_from_lower_camel_case_types(self):
        self.assertRaises(TypeError, misc.from_lower_camel_case, 34)
        self.assertRaises(TypeError, misc.from_lower_camel_case, True)
