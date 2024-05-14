from flask import request
import json
from elasticsearch import Elasticsearch

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
SIZE_HEADER = 'X-Fission-Params-Size'
RADIUS_HEADER = 'X-Fission-Params-Radius'
STATION_HEADER = 'X-Fission-Params-Station'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})
SCROLL = '1m'

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)


def main():
    # Check parameters
    if SIZE_HEADER not in request.headers:
        return BAD_PARAMS
    if STATION_HEADER not in request.headers:
        return BAD_PARAMS
    if RADIUS_HEADER not in request.headers:
        return BAD_PARAMS

    # Get parameters
    try:
        size = int(request.headers[SIZE_HEADER])
        if size > 10000:
            return BAD_PARAMS
        station = request.headers[STATION_HEADER]
        radius = float(request.headers[RADIUS_HEADER])
        if radius < 0:
            return BAD_PARAMS
    except:
        return ERROR

    # First get coordinates of station
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

    # Next get the epa data
    try:
        epa_results = es.search(index='airquality', body={
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
        out['Token'] = epa_results['_scroll_id']
        out['Data'] = epa_results['hits']['hits']
        return json.dumps(out)
    except:
        return ERROR
