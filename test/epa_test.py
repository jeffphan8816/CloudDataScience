from unittest import TestCase
from datetime import datetime
import pandas as pd

from epa_to_kafka import fetch_epa, produce_kafka_buffer_message, \
                         produce_kafka_confirm_message
from epa_kafka_to_es import consume_kafka_message, clean_kafka_data, \
                            fetch_and_clean_ES_data, accepting_new_data, \
                            upload_to_ES, reset_kafka_confirm_message

# Constant url of the epa
URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'
KEY = '96ff8ef9e03048e2bd2fa342a5d80587'

RUN_FROM = 'bastion'

if RUN_FROM == 'bastion' : ES_URL, ES_HEADERS = 'https://elasticsearch.elastic.svc.cluster.local:9200', None
if RUN_FROM == 'uni_wifi' : ES_URL, ES_HEADERS = 'https://172.26.135.52:9200', {'HOST': 'elasticsearch'}

ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'

BOOTSTRAP_KAFKA = 'kafka-kafka-bootstrap.kafka.svc:9092'
if RUN_FROM == 'bastion' : BOOTSTRAP_KAFKA = 'kafka-kafka-bootstrap.kafka.svc:9092'
if RUN_FROM == 'uni_wifi' : BOOTSTRAP_KAFKA = 'https://172.26.135.52:9092'

TOPIC_NAME = 'airquality-kafka'
CONFIRM_TOPIC_NAME = 'airquality-uploaded-kafka'

class Airquality_EPA_to_Kafka_Tests(TestCase):
    
    def test_fetch_epa(self):

        bad_keys_dict1 = {'records':[{'not_parameters':1}]}
        bad_keys_dict2 = {'records':[{'parameters':[{'not_name':1,'timeSeriesReadings':2}],
                                      'geometry':{'coordinates':3}}]}
        bad_keys_dict3 = {'records':[{'parameters':[{'name':1,'not_timeSeriesReadings':2}],
                                      'geometry':{'coordinates':3}}]}
        bad_keys_dict4 = {'records':[{'parameters':[{'name':1,'timeSeriesReadings':{'not_readings':4}}],
                                      'geometry':{'coordinates':3}}]}
        
        good_keys_dict = {'records':[{'parameters':[{'name':1,'timeSeriesReadings':{'readings':{'since':'2010-11-22T10:45:33Z',
                                                                                                'until':'2011-12-23T11:46:34Z',
                                                                                                'averageValue':5}}}],
                                      'geometry':{'coordinates':3}}]}


        self.assertEqual(fetch_epa(bad_keys_dict1), [])
        self.assertEqual(fetch_epa(bad_keys_dict2), [])
        self.assertEqual(fetch_epa(bad_keys_dict3), [])
        self.assertEqual(fetch_epa(bad_keys_dict4), [])

        self.assertEqual(fetch_epa(good_keys_dict), [{'name':1,'location':3,
                                                      'start':datetime(2010, 11, 22, 10, 45, 33),
                                                      'end':datetime(2011, 12, 23, 11, 46, 34),
                                                      'value':5}])



class Airquality_Kafka_to_ES_Tests(TestCase):

    def test_clean_kafka_data(self):
        self.assertEqual(clean_kafka_data({'not_records':0}),None)

        data = {'records':[{'geometry': {'coordinates':('long','lat')},
                    'parameters':[{'name':'particule', 
                                  'timeSeriesReadings':[{'readings':[{'since':'2024-01-01T00:00:00Z',
                                                                     'until':'2024-01-01T01:00:00Z',
                                                                     'averageValue':'value'}]}]}]}]}
        result_string = "[{'measure_name': 'particule', 'location': ('lat', 'long'), 'start': Timestamp('2024-01-01 00:00:00'), 'end': Timestamp('2024-01-01 01:00:00'), 'value': 'value'}]"
        self.assertEqual(str(clean_kafka_data(data).to_dict(orient='records')),result_string)    


    def test_accepting_new_data(self):
        current_records =  [{'measure_name': 'P1', 'location': (0,0), 'end': 0},
                            {'measure_name': 'P1', 'location': (0,0), 'end': 1},
                            {'measure_name': 'P1', 'location': (1,1), 'end': 1},
                            {'measure_name': 'P2', 'location': (0,0), 'end': 1},
                            {'measure_name': 'P2', 'location': (1,1), 'end': 1}]
        new_records =  [{'measure_name': 'P1', 'location': (0,0), 'end': 2},
                        {'measure_name': 'P2', 'location': (0,0), 'end': 1},
                        {'measure_name': 'P2', 'location': (1,1), 'end': 2}]
        current_data = pd.DataFrame(current_records)
        new_data = pd.DataFrame(new_records)

        result_string = "[{'measure_name': 'P1', 'location': (0, 0), 'end': 2}, {'measure_name': 'P2', 'location': (1, 1), 'end': 2}]"
        self.assertEqual(str(accepting_new_data(new_data,current_data).to_dict(orient='records')),result_string)
