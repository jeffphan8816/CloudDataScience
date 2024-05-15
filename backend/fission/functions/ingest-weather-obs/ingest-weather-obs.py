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

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def insert_data_to_es(df: pd.DataFrame):
    """
    Inserts the data from a pandas DataFrame into an Elasticsearch index.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame containing the data to insert into the Elasticsearch index.

    Returns:
    --------
    bool
        Returns True if the data is successfully inserted into the Elasticsearch index.

    Raises:
    -------
    ValueError
        If the connection to the Elasticsearch server fails.

    Notes:
    ------
    The function reads the Elasticsearch mapping from a JSON file, establishes a connection
    to the Elasticsearch server, checks if the index exists, creates it if it doesn't, and
    inserts the data from the DataFrame into the index. The index name is hardcoded as
    'stream_weather_data'. The Elasticsearch server URL, username, and password are also hardcoded.
    """

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

    # Remove NaN values as can't be inserted into ES
    df.replace({np.nan: None}, inplace=True)

    # Insert data to ES - Use bulk method here?
    for i, row in df.iterrows():

        station_name = row["name"]
        timestamp = row["aifstime_utc"]

        document_id = f"{station_name}_{timestamp}"

        document = row.to_dict()
        document['station_name'] = station_name
        document['timestamp'] = timestamp

        es.index(index=index_name, body=document, id=document_id)

    logger.info("Data inserted to Elasticsearch")

    return True


def ingest_data(data):
    try:
        # data = context.request.json
        data = pd.DataFrame(data)
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
