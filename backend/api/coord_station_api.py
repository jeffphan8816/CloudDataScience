from flask import request
import json
from elasticsearch import Elasticsearch
import os

config = {}
for key in os.listdir('/secrets/default/es'):
    with open(os.path.join('/secrets/default/es', key), 'rt') as file:
        config[key] = file.read()

LON_HEADER = 'X-Fission-Params-Lon'
LAT_HEADER = 'X-Fission-Params-Lat'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})
SCROLL = '5m'

es = Elasticsearch([config['URL']], basic_auth=(
    config['USER'], config['PASS']), verify_certs=False, headers={'HOST': config['HOST']})


def main():
    # Check parameters
    if LON_HEADER not in request.headers:
        return BAD_PARAMS
    if LAT_HEADER not in request.headers:
        return BAD_PARAMS

    # Get parameters
    try:
        lon = float(request.headers[LON_HEADER])
        lat = float(request.headers[LAT_HEADER])
    except:
        return ERROR
    
        # Next get the epa data
    try:
        station = es.search(index='station_locations', body={
            'size': 1,
            'query': {
                'bool': {
                    'must': [
                        {'match_all': {}}
                    ],
                },
                "sort": [
                    {
                        "_geo_distance": {
                            "location": {
                                "lat": lat,
                                "lon": lon
                            },
                            "order": "asc",
                            "unit": "km",
                            "mode": "min",
                            "distance_type": "arc",
                        }
                    }
                ]
            },
        })
        
        as_list = station['hits']['hits']
        if len(as_list) == 0:
            return json.dumps({'Status': 200, 'Data': []})
        else:
            return json.dumps({'Status': 200, 'Data': as_list[0]['_soure']})
    except:
        return ERROR
