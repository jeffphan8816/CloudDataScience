from flask import request
import json
from elasticsearch import Elasticsearch
import os


def main():
    """
    The purpose of this function is to stream the next page of data for a given token
    The url is /stream/<Station>/<Token>

    Will return a json string with a 'Status' and 'Data' or 'Message' field depending on the status.
    It will also have a 'Token' that will contain the token for the next request of 'END'.
    """

    # Setup
    config = {}
    for key in os.listdir('/secrets/default/es'):
        with open(os.path.join('/secrets/default/es', key), 'rt') as file:
            config[key] = file.read()

    TOKEN_HEADER = 'X-Fission-Params-Token'
    BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
    ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
    EMPTY = json.dumps({'Status': 200, 'Data': []})
    SCROLL = '5m'

    es = Elasticsearch([config['URL']], basic_auth=(
        config['USER'], config['PASS']), verify_certs=False, headers={'HOST': config['HOST']})

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
        if len(results['hits']['hits']) <= 0:
            es.clear_scroll(scroll_id=out['Token'])
            out['Token'] = 'END'
        return json.dumps(out)
    except:
        return ERROR
