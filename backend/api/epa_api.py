from flask import request
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
METRIC_HEADER = 'X-Fission-Params-Metric'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
NOT_FOUND = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)


def main():
    if METRIC_HEADER not in request.headers:
        return BAD_PARAMS
    metric = request.header[METRIC_HEADER]

    try:
        results = scan(es, index='airquality', query={
            'query': {
                'measure_name': metric
            }
        })

        data = []
        for result in results:
            data.append(result['_source'])

        out = {'status': 200, 'data': data}
        return json.dumps(out)
    except:
        return NOT_FOUND
