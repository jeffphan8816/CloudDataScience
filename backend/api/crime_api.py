from flask import request
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
NOT_FOUND = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)


def main():
    try:
        results = scan(es, index='crime', query={
            'query': {
                'match_all': {}
            }
        })

        data = []
        for result in results:
            data.append(result['_source'])

        out = {'status': 200, 'data': data}
        return json.dumps(out)
    except:
        return NOT_FOUND
