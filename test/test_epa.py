from unittest import TestCase
from datetime import datetime
import pandas as pd

from backend.fission.functions.epa_to_kafka import fetch_epa, produce_kafka_buffer_message, \
                         produce_kafka_confirm_message
from backend.fission.functions.epa_kafka_to_es import consume_kafka_message, clean_kafka_data, \
                            fetch_and_clean_ES_data, accepting_new_data, \
                            upload_to_ES, reset_kafka_confirm_message


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
