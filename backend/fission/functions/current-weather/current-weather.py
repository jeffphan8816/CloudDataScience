
# Jupyter - sends lat/lon to API


# API get current weather - GET ?lat= & lon=

# -> Send GET request to closest weather station - Send lat/lon -> receive the row from ES with deatils from current_bom_stations

# -> Call the current weather JSON endpoint
#     -> filter
#     -> clean


# -> Call LLM text (POST)
#     -> describe the current weather

# -> Call LLM dalle (POST)
#     ->

import json
import logging
import requests

from elasticsearch import Elasticsearch
from flask import request

from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed
from cleanweather import clean_weather

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TODO: replace with creds
# ELASTIC_URL = 'https://elasticsearch.elastic.svc.cluster.local:9200'
ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "cloudcomp"
ES_HEADERS = {'HOST': 'elasticsearch'}

@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def get_closest_station(lon, lat):

    FISSION_URL = "http://172.26.135.52:9090/"
    FISSION_HEADERS = {"HOST": "fission"}

    url = f"{FISSION_URL}/stations/{lon}/{lat}"

    response = requests.get(url, headers=FISSION_HEADERS)

    station_details = response.json()

    return station_details["Data"]

@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def get_station_by_name(name) -> dict:

    es = Elasticsearch([ELASTIC_URL], basic_auth=(
        ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)

    if not es.ping():
        raise ValueError("Connection failed")

    index_name = "current_bom_stations"

    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"Station Name": name}},
                ]
            }
        }
    }

    response = es.search(index=index_name, body=query, size=1000)

    return response["hits"]["hits"][0]["_source"]

def get_station_by_id(station_id) -> dict:

    es = Elasticsearch([ELASTIC_URL], basic_auth=(
        ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)

    if not es.ping():
        raise ValueError("Connection failed")

    index_name = "current_bom_stations"

    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"Station ID": station_id}},
                ]
            }
        }
    }

    response = es.search(index=index_name, body=query, size=1000)

    return response["hits"]["hits"][0]["_source"]

@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def fetch_weather(url):

    response = requests.get(url).json()
    out = response["observations"]["data"][0]

    return out

def main():

    # This function can receive one of the following arguments:
    # - lat and lon
    # - name of the station
    # - id of the station

    if "lat" in request.args and "lon" in request.args:
        lat = request.args.get("lat")
        lon = request.args.get("lon")

        station_details = get_closest_station(lon=lon, lat=lat)

    elif "name" in request.args:
        name = request.args.get("name")
        station_details = get_station_by_name(name)

    elif "id" in request.args:
        station_id = request.args.get("id")
        station_details = get_station_by_id(station_id)
    else:
        return "Invalid request"

    raw_weather = fetch_weather(station_details["json_url"])

    cleaned_weather = clean_weather(raw_weather)
    # clean_weather = raw_weather

    # return json.dumps(clean_weather)
    return json.dumps({'Status': 200, 'Data': cleaned_weather})


# TODO:  check executor type

# station_details = get_closest_station(143, 36)
# fetch_weather(station_details["json_url"])

# station_details = get_station_by_name("Kerang")
# fetch_weather(station_details["json_url"])

# station_details = get_station_by_id(80128)
# fetch_weather(station_details["json_url"])

