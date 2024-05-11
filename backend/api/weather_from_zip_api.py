from flask import request
import json
from elasticsearch import Elasticsearch
import datetime

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
MONTH_HEADER = 'X-Fission-Params-Month'
DAY_HEADER = 'X-Fission-Params-Day'
YEAR_HEADER = 'X-Fission-Params-Year'
ZIP_HEADER = 'X-Fission-Params-Zip'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)

def main():
    if MONTH_HEADER not in request.headers:
        return BAD_PARAMS
    if DAY_HEADER not in request.headers:
        return BAD_PARAMS
    if YEAR_HEADER not in request.headers:
        return BAD_PARAMS
    if ZIP_HEADER not in request.headers:
        return BAD_PARAMS

    zip = request.headers[ZIP_HEADER]
    coord_results = es.search(index='locations', body={
        'size': 1,
        'query': {
            "match": { "postcode": zip }
        }
    })
    coord_list = [coord_results['hits']['hits'][i]['_source']
        for i in range(len(coord_results['hits']['hits']))]

    if len(coord_list) == 0:
        return json.dumps({"Status": 404, "Message": "Zip code not found"})

    long = coord_list[0]['location'][0]
    lat = coord_list[0]['location'][1]
    month = int(request.headers[MONTH_HEADER])
    day = int(request.headers[DAY_HEADER])
    year = int(request.headers[YEAR_HEADER])
    date = datetime.datetime(year, month,  day)

    # Find station
    try:
        # With help from https://stackoverflow.com/questions/69520933/how-to-get-the-nearest-locationgeo-point-to-a-given-geo-point-using-elasticsea
        # Get closest station
        stations = es.search(index='station_locations', body={
            'size': 1,
            'sort': [
                {
                    '_geo_distance': {
                        'location': {
                            'lat': lat,
                            'lon': long
                        },
                        'order': 'asc',
                        'unit': 'km',
                        'mode': 'min',
                        'distance_type': 'arc',
                        'ignore_unmapped': True
                    }
                }
            ],
        })
        if len(stations) == 0:
            return json.dumps({'status': 200, 'data': []})

        station = [stations['hits']['hits'][i]['_source']
                    for i in range(len(stations['hits']['hits']))][0]

    except:
        return ERROR

    # Find weather
    station['Station Name'] = 'OLYMPIC PARK'
    results = es.search(index='weather_past_obs', body={
        'size': 1,
        'query': {
            'bool': {
                'must': [
                    {'match': {'Station Name': station['Station Name']}},
                    {'match': {'Date': date.strftime('%d/%m/%Y')}}
                ]
            }
        }
    })
    result_list = [results['hits']['hits'][i]['_source']
                for i in range(len(results['hits']['hits']))]

    out = {'status': 200, 'data': result_list}
    return json.dumps(out)
