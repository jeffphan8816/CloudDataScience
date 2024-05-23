from unittest import TestCase
import requests
import json
import pandas as pd

RUN_FROM = 'uni_wifi'

if RUN_FROM == 'bastion' : URL, HEADERS = 'http://fission:31001/', None
if RUN_FROM == 'uni_wifi': URL, HEADERS =  'http://172.26.135.52:9090/', {'HOST': 'fission'}

PAGE_NOT_FOUND_STR = '404 page not found\n'
BAD_PARAMS_STR = '{"Status": 400, "Message": "Invalid Parameters"}'
ERROR_STR = '{"Status": 500, "Message": "Internal Server Error"}'
EMPTY_STR = '{"Status": 200, "Data": []}'


def get_stream_to_pd(api: str, station_id: str, size: int, radius_km: int, verb=False) -> pd.DataFrame:
    resp_dict = json.loads(requests.get(URL+api+f'/{station_id}/{size}/{radius_km}', headers=HEADERS).text)
    print(resp_dict)

    count=0
    status, token, new_data = resp_dict['Status'], resp_dict['Token'], resp_dict['Data']
    data = [new_data[i]['_source'] for i in range(len(new_data))]
    if verb : print(f'Called {api} api, fetched {len(new_data)} lines')


    while (status == 200) and (new_data != []) :
        count+=1
        resp_dict = json.loads(requests.get(URL+f'stream/'+token, headers=HEADERS).text)
        status, token, new_data = resp_dict['Status'], resp_dict['Token'], resp_dict['Data']
        if verb : print(f'Called stream {count} times, fetched {len(new_data)} new lines')
        data += [new_data[i]['_source'] for i in range(len(new_data))]

    if verb: print(f'Fetched a total of {len(data)}lines')
    return pd.DataFrame.from_records(data)


class APITests(TestCase):

    def __init__(self, methodName: str = "api tests") -> None:
        super().__init__(methodName)

    def test_station_list_api(self):
        """
        Test for the get request
        """
        resp = requests.get(URL+'stations').json()
        df_stations = pd.DataFrame.from_records(resp['Data'], index='Station ID')
        self.assertEqual(df_stations.loc[23034,'Station Name'],'ADELAIDE AIRPORT')


    def test_weather_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'weather/23034/2014').text
        self.assertEqual(resp,PAGE_NOT_FOUND_STR)

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
        resp = requests.get(URL+'weather/94250/2014/2015').json()
        df_weather = pd.DataFrame.from_records(resp['Data'], index='Date')
        self.assertEqual(df_weather.loc['03/01/2014','UV'],'28.70')


    def test_crash_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'crashes/23034/5000').text
        self.assertEqual(resp,PAGE_NOT_FOUND_STR)

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
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Valid parameters
        resp = requests.get(URL+'crashes/23034/5000/800').json()
        df_crash = pd.DataFrame.from_records(resp['Data'], index='_id')
        self.assertEqual(df_crash.loc['5XZkcI8B_XhVKXBOfiMP','_source']['crash_date'],
                         '2014-06-19T00:00:00.000+0000')


    def test_crime_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'crime/23034/5000').text
        self.assertEqual(resp,PAGE_NOT_FOUND_STR)

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
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Valid parameters
        resp = requests.get(URL+'crime/95003/5000/500').json()
        df_crime = pd.DataFrame.from_records(resp['Data'], index='_id')
        self.assertEqual(df_crime.loc['mYMPVo8BeqktFCObjzke']['_source']['reported_date'],'2020-03-20T00:00:00')


    def test_airqual_api(self):
        """
        Test for error when we are missing or invalid headers,
        test result for one valid (station_id,size,radius) parameters
        """

        # Missing header
        resp = requests.get(URL+'epa/23034/5000').text
        self.assertEqual(resp,PAGE_NOT_FOUND_STR)

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
        self.assertEqual(resp,BAD_PARAMS_STR)

        # Valid parameters
        resp = requests.get(URL+'epa/23034/5000/500').json()
        df_airqual = pd.DataFrame.from_records(resp['Data'], index='_id')
        self.assertAlmostEqual(df_airqual.loc['cYG8k48ByK62b84DjfYj']['_source']['value'],7.45)


    def test_stream_api(self):
        df_crime_full = get_stream_to_pd(api='crime', station_id='95003', size=5, radius_km=500, verb=True)
        self.assertEqual(df_crime_full.shape[0],7)

    # def test_models_api(self):
    #     # Missing header
    #     resp = requests.get(URL+'models').text
    #     self.assertEqual(resp,BAD_PARAMS_STR)

    #     # No predictors, returning model coefs
    #     resp = requests.get(URL+'models/test').json()
    #     self.assertAlmostEqual(resp['intercept'], 3.)
    #     self.assertAlmostEqual(resp['coef'][0], 1.)
    #     self.assertAlmostEqual(resp['coef'][1], 2.)

    #     # Too many predictors (here 3 instead of 2)
    #     params = {'predictors': '1.1,2.2,3.3'}
    #     resp = requests.get(URL+'models/test', params=params).text
    #     self.assertEqual(resp,BAD_PARAMS_STR)


    #     # Valid parameters
    #     params = {'predictors': '1.1,2.2'}
    #     resp = requests.get(URL+'models/test', params=params).json()
    #     self.assertAlmostEqual(resp['prediction'])