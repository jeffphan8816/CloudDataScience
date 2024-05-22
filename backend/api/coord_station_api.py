from flask import request
import json
from elasticsearch import Elasticsearch
import os

def main():
    """
    The purpose of this function is to get the station closest to a given latitude and latitude
    The url is /stations/<Longitude>/<Latitude>

    Will return a json string with a 'Status' and 'Data' or 'Message' field depending on the status
    """

    # Setup
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
        station = es.search(index='current_bom_stations', body={
            'size': 1,
            'query': {
                'match_all': {}
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
            })
        
        as_list = station['hits']['hits']
        if len(as_list) == 0:
            return json.dumps({'Status': 200, 'Data': []})
        else:
            return json.dumps({'Status': 200, 'Data': as_list[0]['_source']})
    except:
        return ERROR

