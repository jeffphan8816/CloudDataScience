from flask import request
import json
from elasticsearch import Elasticsearch

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
START_HEADER = 'X-Fission-Params-Start'
SIZE_HEADER = 'X-Fission-Params-Size'
METRIC_HEADER = 'X-Fission-Params-Measure'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
NOT_FOUND = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)


def main():
    if START_HEADER not in request.headers or SIZE_HEADER not in request.headers or METRIC_HEADER not in request.headers:
        return BAD_PARAMS

    start = request.headers[START_HEADER]
    size = request.headers[SIZE_HEADER]
    metric = request.headers[METRIC_HEADER]

    try:
        results = es.search(index='airquality', body={
            'from': start,
            'size': size,
            'query': {
                'match': {
                    'measure_name': metric
                }
            }
        })
        as_list = [results['hits']['hits'][i]['_source']
                   for i in range(len(results['hits']['hits']))]
        out = {'status': 200, 'data': as_list}
        return json.dumps(out)
    except:
        return NOT_FOUND
