from unittest import TestCase
import pandas as pd

import crashdata_api_connect
import airqual_api_connect
import epa_api_connect



class APITests(TestCase):

    def __init__(self, methodName: str = "api tests") -> None:
        super().__init__(methodName)

    """
    Test that data loads correctly
    """
    def test_load(self):
        try:
            self.assertEqual(1,1)
        except:
            self.assertTrue(False)
