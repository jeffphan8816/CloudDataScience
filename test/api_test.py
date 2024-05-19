from unittest import TestCase
import requests
import json
import pandas as pd

RUN_FROM = 'bastion'

if RUN_FROM == 'bastion' : URL, HEADERS = 'http://fission:31001/', None
if RUN_FROM == 'uni_wifi': URL, HEADERS =  'http://172.26.135.52:9090/', {'HOST': 'fission'}

BAD_PARAMS_STR = "{'Status': 400, 'Message': 'Invalid Parameters'}"
ERROR_STR = "{'Status': 500, 'Message': 'Internal Server Error'}"
EMPTY_STR = "{'Status': 200, 'Data': []}"



class APITests(TestCase):

    def __init__(self, methodName: str = "api tests") -> None:
        super().__init__(methodName)

    def test_station_list_api(self):
        """
        Test for the get request
        """
        resp = requests.get(URL+'stations').text
        dic_stations = json.loads(resp)
        df_stations = pd.DataFrame.from_records(dic_stations['Data'], index='Station ID')
        self.assertEqual(df_stations.loc[23034,'Station Name'],'ADELAIDE AIRPORT')


    def test_weather_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'weather/23034/2014').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Invalid station_id
        resp = requests.get(URL+'weather/STATION_ID/2014/2015').text
        self.assertEqual(resp,EMPTY_STR)

        # Invalid start_year
        resp = requests.get(URL+'weather/23034/START_YEAR/2015').text
        self.assertEqual(resp,ERROR_STR)

        # Invalid end_year
        resp = requests.get(URL+'weather/23034/2014/END_YEAR').text
        self.assertEqual(resp,ERROR_STR)

        # Valid parameters
        resp = requests.get(URL+'weather/23034/2014/2015').text
        self.assertEqual(resp,'')


    def test_crash_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'crashes/23034/5000').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Invalid station_id
        resp = requests.get(URL+'crashes/STATION_ID/5000/3000').text
        self.assertEqual(resp,EMPTY_STR)

        # Invalid size
        resp = requests.get(URL+'crashes/23034/SIZE/3000').text
        self.assertEqual(resp,ERROR_STR)

        # Size too big
        resp = requests.get(URL+'crashes/23034/10500/3000').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Invalid radius_km
        resp = requests.get(URL+'crashes/23034/5000/RADIUS_KM').text
        self.assertEqual(resp,ERROR_STR)

        # Negative radius_km
        resp = requests.get(URL+'crashes/23034/5000/-3000').text
        self.assertEqual(resp,ERROR_STR)

        # Valid parameters
        resp = requests.get(URL+'crashes/23034/5000/3000').text
        self.assertEqual(resp,'')


    def test_crime_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'crime/23034/5000').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Invalid station_id
        resp = requests.get(URL+'crime/STATION_ID/5000/3000').text
        self.assertEqual(resp,EMPTY_STR)

        # Invalid size
        resp = requests.get(URL+'crime/23034/SIZE/3000').text
        self.assertEqual(resp,ERROR_STR)

        # Size too big
        resp = requests.get(URL+'crime/23034/10500/3000').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Invalid radius_km
        resp = requests.get(URL+'crime/23034/5000/RADIUS_KM').text
        self.assertEqual(resp,ERROR_STR)

        # Negative radius_km
        resp = requests.get(URL+'crime/23034/5000/-3000').text
        self.assertEqual(resp,ERROR_STR)

        # Valid parameters
        resp = requests.get(URL+'crime/23034/5000/3000').text
        self.assertEqual(resp,'')


    def test_airqual_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'epa/23034/5000').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Invalid station_id
        resp = requests.get(URL+'epa/STATION_ID/5000/3000').text
        self.assertEqual(resp,EMPTY_STR)

        # Invalid size
        resp = requests.get(URL+'epa/23034/SIZE/3000').text
        self.assertEqual(resp,ERROR_STR)

        # Size too big
        resp = requests.get(URL+'epa/23034/10500/3000').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Invalid radius_km
        resp = requests.get(URL+'epa/23034/5000/RADIUS_KM').text
        self.assertEqual(resp,ERROR_STR)

        # Negative radius_km
        resp = requests.get(URL+'epa/23034/5000/-3000').text
        self.assertEqual(resp,ERROR_STR)

        # Valid parameters
        resp = requests.get(URL+'epa/23034/5000/3000').text
        self.assertEqual(resp,'')


    def test_stream_api(self):
        # Missing header
        resp = requests.get(URL+'stream').text
        self.assertEqual(resp,BAD_PARAMS_STR)


    def test_models_api(self):
        # Missing header
        resp = requests.get(URL+'models').text
        self.assertEqual(resp,BAD_PARAMS_STR)

        # No predictors, returning model coefs
        resp = requests.get(URL+'models/test').json()
        self.assertAlmostEqual(resp['intercept'], 3.)
        self.assertAlmostEqual(resp['coef'][0], 1.)
        self.assertAlmostEqual(resp['coef'][1], 2.)

        # Too many predictors (here 3 instead of 2)
        params = {'predictors': '1.1,2.2,3.3'}
        resp = requests.get(URL+'models/test', params=params).text
        self.assertEqual(resp,BAD_PARAMS_STR)


        # Valid parameters
        params = {'predictors': '1.1,2.2'}
        resp = requests.get(URL+'models/test', params=params).json()
        self.assertAlmostEqual(resp['prediction'])