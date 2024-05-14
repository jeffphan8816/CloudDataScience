import datetime
from elasticsearch import Elasticsearch
from flask import request
import json

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
STATION_HEADER = 'X-Fission-Params-Station'
START_HEADER = 'X-Fission-Params-Start'
END_HEADER = 'X-Fission-Params-End'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)


def main():
    # Check parameters
    if STATION_HEADER not in request.headers:
        return BAD_PARAMS
    if START_HEADER not in request.headers:
        return BAD_PARAMS
    if END_HEADER not in request.headers:
        return BAD_PARAMS

    # Get parameters
    try:
        station = request.headers[STATION_HEADER]
        start = int(request.headers[START_HEADER])
        end = int(request.headers[END_HEADER])
    except:
        return ERROR

    # Convert days to years
    start_date = datetime.datetime(start, 1,  1)
    end_date = datetime.datetime(end, 1, 1)

    # First get station lat and long
    try:
        station_results = es.search(index='station_locations', body={
            'size': 1,
            'query': {
                'match': {
                    'Station ID': station
                }
            }
        })

        if len(station_results) == 0:
            return EMPTY
        station_results_list = [station_results['hits']['hits'][i]['_source']
                                for i in range(len(station_results['hits']['hits']))]
        if len(station_results_list) == 0:
            return EMPTY

        station_name = station_results_list[0]['Station Name']
    except:
        return ERROR

    # Then get weather results
    try:
        weather_results = es.search(index='weather_past_obs', body={
            'size': 10000,
            'query': {
                'bool': {
                    'must': [
                        {
                            'match': {
                                'Station Name': station_name
                            }
                        }
                    ],
                    'filter': {
                        'range': {
                            'Date': {
                                'gte': start_date.strftime('%d/%m/%Y'),
                                'lte': end_date.strftime('%d/%m/%Y')
                            }
                        }
                    }
                }
            }
        })

        out = {}
        out['Status'] = 200
        out['Data'] = [weather_results['hits']['hits'][i]['_source']
                       for i in range(len(weather_results['hits']['hits']))]
        return json.dumps(out)
    except:
        return ERROR
