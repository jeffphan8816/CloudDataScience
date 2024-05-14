from flask import request
import json
from elasticsearch import Elasticsearch

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
TOKEN_HEADER = 'X-Fission-Params-Token'
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})
SCROLL = '1m'

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)


def main():
    # Check parameters
    if TOKEN_HEADER not in request.headers:
        return BAD_PARAMS

    # Get token
    token = request.headers[TOKEN_HEADER]

    # Try to get next batch
    try:
        results = es.scroll(scroll_id=token, scroll=SCROLL)
        out = {}
        out['Status'] = 200
        out['Token'] = results['_scroll_id']
        out['Data'] = results['hits']['hits']
        return json.dumps(out)
    except:
        return ERROR
