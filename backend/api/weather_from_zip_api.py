from flask import request
import json
from elasticsearch import Elasticsearch
import datetime

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
LONG_HEADER = 'X-Fission-Params-Long'
LAT_HEADER = 'X-Fission-Params-Lat'
MONTH_HEADER = 'X-Fission-Params-Month'
DAY_HEADER = 'X-Fission-Params-Day'
YEAR_HEADER = 'X-Fission-Params-Year'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
NOT_FOUND = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)


def main():
    if LONG_HEADER not in request.headers:
        return BAD_PARAMS
    if LAT_HEADER not in request.headers:
        return BAD_PARAMS
    if MONTH_HEADER not in request.headers:
        return BAD_PARAMS
    if DAY_HEADER not in request.headers:
        return BAD_PARAMS
    if YEAR_HEADER not in request.headers:
        return BAD_PARAMS

    long = request.headers[LONG_HEADER]
    lat = request.headers[LAT_HEADER]
    month = request.headers[MONTH_HEADER]
    day = request.headers[DAY_HEADER]
    year = request.headers[YEAR_HEADER]

    date = datetime.datetime(year, month,  day)

    try:
        # With help from https://stackoverflow.com/questions/69520933/how-to-get-the-nearest-locationgeo-point-to-a-given-geo-point-using-elasticsea
        results = es.search(index='crashes', body={
            'size': 1,
            'sort': [
                {
                    '_geo_distance': {
                        'location-field': [long, lat],
                        'order': 'asc',
                        'unit': 'km',
                        'mode': 'min',
                        'distance_type': 'arc',
                        'ignore_unmapped': True
                    }
                }
            ],
            'query': {
                'date': date
            }
        })
        as_list = [results['hits']['hits'][i]['_source']
                   for i in range(len(results['hits']['hits']))]
        out = {'status': 200, 'data': as_list}
        return json.dumps(out)
    except:
        return NOT_FOUND
