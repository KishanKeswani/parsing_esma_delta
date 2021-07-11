# Unittests For Function calculate_second_max
# To Run : python -m unittest parsing_script_test.py
import unittest
import pandas as pd


class verifyOutputFile(unittest.TestCase):
    df = pd.read_csv('output_data.csv')

    def test_column_names(self):
        df = pd.read_csv('output_data.csv')
        self.assertTrue("FinInstrmGnlAttrbts.Id" in df.columns, True)
        self.assertTrue("FinInstrmGnlAttrbts.FullNm" in df.columns, True)
        self.assertTrue("Issr" in df.columns, True)

    def test_count(self):
        df = pd.read_csv('output_data.csv')
        self.assertTrue(len(df) > 100000, True)
        count_uniq_cmdty = df['FinInstrmGnlAttrbts.CmmdtyDerivInd'].nunique()
        self.assertTrue(count_uniq_cmdty == 2, True)
        self.assertTrue(df['Issr'].nunique() > 250, True)
