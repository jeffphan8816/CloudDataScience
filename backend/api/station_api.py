# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

import json
from elasticsearch import Elasticsearch
import os

def main():
    """
    The purpose of this function is to return all the historical stations
    The url is /stations

    Will return a json string with a 'Status' and 'Data' or 'Message' field depending on the status
    """

    # Setup
    config = {}
    for key in os.listdir('/secrets/default/es'):
        with open(os.path.join('/secrets/default/es', key), 'rt') as file:
            config [key] = file.read()

    ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
    EMPTY = json.dumps({'Status': 200, 'Data': []})

    es = Elasticsearch([config['URL']], basic_auth=(
        config['USER'], config['PASS']), verify_certs=False, headers={'HOST': config['HOST']})

    # Try to return all station data
    try:
        station_results = es.search(index='station_locations', body={
            'size': 10000,
            'query': {
                'match_all': {}
            }
        })
        station_result_list = [station_results['hits']['hits'][i]['_source']
                               for i in range(len(station_results['hits']['hits']))]
        if len(station_result_list) == 0:
            return EMPTY
        return json.dumps({'Status': 200, 'Data': station_result_list})
    except:
        return ERROR
