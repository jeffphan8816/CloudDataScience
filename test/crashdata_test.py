from unittest import TestCase
from crashdata_functions import clean_dataframe
import pandas as pd


class CrashDataTests(TestCase):

    def __init__(self, methodName: str = "crash data tests") -> None:
        super().__init__(methodName)

    """
    Test that data loads correctly
    """
    def test_load(self):
        try:
            df = clean_dataframe(
                '../dataset/sudo_tasmania_crash_2010_2020.csv')
            expectedCols = ['crash_date', 'latitude',
                            'light_condition', 'longitude', 'severity']
            for col in expectedCols:
                self.assertTrue(col in df.columns)
            self.assertEqual(len(expectedCols), len(df.columns))
        except:
            self.assertTrue(False)

    """
    Test raise error on bad path
    """
    def test_bad_path(self):
        try:
            df = clean_dataframe(
                './sudo_tasmania_crash_2010_2020.csv')
            self.assertTrue(False)
        except ValueError:
            self.assertTrue(True)
        except:
            self.assertTrue(False)

    """
    Test that severity col is converted to int
    """
    def test_severity_col(self):
        df = clean_dataframe(
            '../dataset/sudo_tasmania_crash_2010_2020.csv')
        self.assertTrue(df['severity'].apply(
            lambda severity: type(severity) == int).all())
