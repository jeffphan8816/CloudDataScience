from flask import request
import json
from elasticsearch import Elasticsearch
import os

config = {}
for key in os.listdir('/secrets/default/es'):
    with open(os.path.join('/secrets/default/es', key), 'rt') as file:
        config [key] = file.read()
        
SIZE_HEADER = 'X-Fission-Params-Size'
RADIUS_HEADER = 'X-Fission-Params-Radius'
STATION_HEADER = 'X-Fission-Params-Station'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})
SCROLL = '5m'

es = Elasticsearch([config['URL']], basic_auth=(
    config['USER'], config['PASS']), verify_certs=False, headers={'HOST': config['HOST']})


def main():
   # Check parameters
    if SIZE_HEADER not in request.headers:
        return BAD_PARAMS
    if STATION_HEADER not in request.headers:
        return BAD_PARAMS
    if RADIUS_HEADER not in request.headers:
        return BAD_PARAMS

    try:
        # Get parameters
        size = int(request.headers[SIZE_HEADER])
        if size > 10000:
            return BAD_PARAMS
        station = request.headers[STATION_HEADER]
        radius = float(request.headers[RADIUS_HEADER])
        if radius < 0:
            return BAD_PARAMS
    except:
        return ERROR

    # First get coordinates for station
    try:
        station_results = es.search(index='station_locations', body={
            'size': 1,
            'query': {
                'match': {
                    'Station ID': station
                }
            }
        })
        station_result_list = [station_results['hits']['hits'][i]['_source']
                               for i in range(len(station_results['hits']['hits']))]
        if len(station_result_list) == 0:
            return EMPTY
        lon = station_result_list[0]['location'][0]
        lat = station_result_list[0]['location'][1]
    except:
        return ERROR

    # Then get crashes in the radius
    try:
        results = es.search(index='crashes', body={
            'size': size,
            'query': {
                'bool': {
                    'must': [
                        {'match_all': {}}
                    ],
                    'filter': {
                        'geo_distance': {
                            'distance': str(radius) + 'km',
                            'location': {
                                'lon': lon,
                                'lat': lat
                            }
                        }
                    }
                },
            },
        }, scroll=SCROLL)

        out = {}
        out['Status'] = 200
        out['Token'] = results['_scroll_id']
        out['Data'] = results['hits']['hits']
        if len(out['Data']) == 0:
            out['Token'] = 'END'
        return json.dumps(out)
    except:
        return ERROR
