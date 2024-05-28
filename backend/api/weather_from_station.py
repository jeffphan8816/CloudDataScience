# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

import datetime
from elasticsearch import Elasticsearch
from flask import request
import json
import os


def main():
    """
    The purpose of this function is to get the weather for a station in a range of years
    The url is /weather/<Station>/<Start_Year>/<End_Year>

    Will return a json string with a 'Status' and 'Data' or 'Message' field depending on the status
    """

    # Setup
    config = {}
    for key in os.listdir('/secrets/default/es'):
        with open(os.path.join('/secrets/default/es', key), 'rt') as file:
            config[key] = file.read()

    STATION_HEADER = 'X-Fission-Params-Station'
    START_HEADER = 'X-Fission-Params-Start'
    END_HEADER = 'X-Fission-Params-End'
    BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
    ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
    EMPTY = json.dumps({'Status': 200, 'Data': []})

    es = Elasticsearch([config['URL']], basic_auth=(
        config['USER'], config['PASS']), verify_certs=False, headers={'HOST': config['HOST']})

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
