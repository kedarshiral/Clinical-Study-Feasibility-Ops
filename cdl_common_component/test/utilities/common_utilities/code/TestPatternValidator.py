# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import unittest
import os,sys

CURRENT_DIR = os.getcwd()
sys.path.insert(0, os.path.join(CURRENT_DIR, 'utilities/common_utilities/code/'))
from Pattern_Validator import PatternValidator

"""
Command for nosetests
nosetests test\utilities\common_utilities\code\TestPattern_Validator.py --with-coverage
"""

class TestPattern_Validator(unittest.TestCase):

    def setUp(self):
        self.source = "Q2"
        self.pattern = "1234.txt"
        self.pattern1 = "Customer_Affiliation_F_QNYYYY_YYYYMMDD_HHMMSS"
        self.pattern2 = "Customer_Affiliation_F_QNYYYY_MMYYYYDD_HHMMSS"
        self.pattern3 = "Customer_Affiliation_F_QN_YYYYMMDD_HHMMSS"

    def test_tc01_blank_source(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="", pattern=self.pattern)
        self.assertEqual(pattern_validator_result, False)

    def test_tc02_Q1check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="Q1", pattern=self.pattern)
        self.assertEqual(pattern_validator_result, False)

    def test_tc03_c_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="_C_", pattern=self.pattern1)
        self.assertEqual(pattern_validator_result, False)

    def test_tc04_FACT_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="FACT", pattern=self.pattern1)
        self.assertEqual(pattern_validator_result, False)

    def test_tc05_LRX_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="LRX.PROD", pattern=self.pattern1)
        self.assertEqual(pattern_validator_result, False)

    def test_tc06_NEW_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="NEW.PROD", pattern=self.pattern1)
        self.assertEqual(pattern_validator_result, False)

    def test_tc07_SAN_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="SAN.CUST", pattern=self.pattern1)
        self.assertEqual(pattern_validator_result, False)

    def test_tc08_MMYYYY_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="ABC.XYZ", pattern=self.pattern2)
        self.assertEqual(pattern_validator_result, False)

    def test_tc09_N_YYYYMM_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="ABC.XYZ", pattern=self.pattern3)
        self.assertEqual(pattern_validator_result, False)

    def test_tc10_same_source_and_pattern_check(self):
        pattern_validator_object = PatternValidator()
        pattern_validator_result = pattern_validator_object.patter_validator(source="ABC.XYZ", pattern="ABC.XYZ")
        self.assertEqual(pattern_validator_result, True)

unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromTestCase(PatternValidator))