import os
import asyncio
import json
import logging
import requests

import aiohttp
import numpy as np
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from flask import request
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed
from typing import Any, Dict

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_data_to_es(data: Dict[str, Any]):

    current_dir = os.path.dirname(__file__)

    # Get explicit mapping of the column types for Elasticsearch
    with open(os.path.join(current_dir, "bom_es_mapping.json"), "r") as f:
        mapping = json.load(f)

    # TODO: replace with dot env
    url = 'https://elasticsearch.elastic.svc.cluster.local:9200'
    user = "elastic"
    password = "cloudcomp"

    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)

    if not es.ping():
        raise ValueError("Connection failed")

    # Define the index name
    index_name = "weather_hourly_obs"

    # Check if the index exists
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    actions = []
    for record in data:
        station_name = record["name"]
        timestamp = record["aifstime_utc"]

        document_id = f"{station_name}_{timestamp}"

        document = record
        document['station_name'] = station_name
        document['timestamp'] = timestamp

        action = {
            "_index": index_name,
            "_id": document_id,
            "_source": document
        }
        actions.append(action)

    if actions:
        bulk(es, actions)
        logger.info("Data inserted to Elasticsearch")

    return True



def ingest_data(data: Dict[str, Any]):
    try:
        insert_data_to_es(data)
    except Exception as e:
        print(f"Failed to ingest data: {str(e)}")


def main():

    print(request)
    data = request.get_json()
    print(data)
    print(type(data))

    try:
        # If data is a string, parse it to a Python list/dict
        if isinstance(data, str):
            data = json.loads(data)
        print(type(data))  # Ensure it's now a list/dict

        ingest_data(data)

    except Exception as e:
        print(f"Failed to ingest data: {e}")
        return "Failed", 400

    return "Done"
