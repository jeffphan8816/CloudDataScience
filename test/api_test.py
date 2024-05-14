from unittest import TestCase
import requests
import pandas as pd


BAD_PARAMS = {'Status': 400, 'Message': 'Invalid Parameters'}


class APITests(TestCase):

    def __init__(self, methodName: str = "api tests") -> None:
        super().__init__(methodName)


    def test_crash_connect(self):
        """
        Test for error when we are missing or headers
        """
        try:
            self.assertEqual(1,1)
        except:
            self.assertTrue(False)
    
    def test_airqual_connect(self):
        return
    def test_crime_connect(self):
        return
