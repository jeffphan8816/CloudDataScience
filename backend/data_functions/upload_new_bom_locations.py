"""
Upload new station locations to ES

Because there's not a 1:1 match with the old stations, we need to upload the new stations
as a new index to ES to make sure the live querying matches with current locations
"""

import json
import pandas as pd

if __name__ == '__main__':
    with open("../../data/bom_stations.json", "r") as f:
        bom_stations = json.load(f)

    with open("../../data/all_stations_from_es.json", "r") as f:
        all_stations_from_es = json.load(f)

    bom_stations[0]
    all_stations_from_es[0]

    # Rename - what a mess...
    mapping = {
        "Station ID": "old_ID",
        "Station Name": "old_name",
        "ID": "new_ID",
        "name": "new_name",
    }
    bom_stations = [{mapping.get(k, k): v for k, v in station.items()} for station in bom_stations]

    mapping = {
        "new_ID": "Station ID",
        "new_name": "Station Name",
    }
    bom_stations = [{mapping.get(k, k): v for k, v in station.items()} for station in bom_stations]
    bom_stations[0]

    # Somehow have dups
    ids = set()
    out = []

    for station in bom_stations:
        station['Station ID'] = int(station['Station ID'])
        station['Lon'] = float(station['Lon'])
        station['Lat'] = float(station['Lat'])
        station['location'] = [station['Lon'], station['Lat']]

        if station['Station ID'] not in ids:
            out.append(station)
            ids.add(station['Station ID'])

    # Checking
    all_stations_from_es[0]
    [station for station in out if station['old_name'] == 'BROOME AIRPORT']

    # Upload to ES

    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk

    ELASTIC_URL = 'https://172.26.135.52:9200'
    ELASTIC_USER = "elastic"
    ELASTIC_PASSWORD = "cloudcomp"
    ES_HEADERS = {'HOST': 'elasticsearch'}
    BATCH_SIZE = 100

    es = Elasticsearch([ELASTIC_URL], basic_auth=(
        ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)

    if not es.ping():
        raise ValueError("Connection failed")

    index_name = 'current_bom_stations'

    actions = []
    for document in out:

        document_id = document['Station ID']

        es.index(index=index_name, id=document_id, document=document)

    #     action = {
    #         "_index": index_name,
    #         "_id": document_id,
    #         "_source": document
    #     }
    #     actions.append(action)

    # if actions:
    #     bulk(es, actions)
