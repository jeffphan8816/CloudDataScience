from flask import request
import json
from elasticsearch import Elasticsearch
import os

def main():
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
