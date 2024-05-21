import json
import requests

from flask import request

def process_data(data):

    # Deal with Nan values and cleaning

    # Remove NaN values as can't be inserted into ES
    # df.replace({np.nan: None}, inplace=True)

    return data

def post_to_ingest(data):

    FISSION_URL = "http://172.26.135.52:9090/"
    FISSION_HEADERS = {"HOST": "fission"}

    ingest_url = f"{FISSION_URL}/ingest-weather-obs"

    response = requests.post(ingest_url, json=data, headers=FISSION_HEADERS)

    return response


def main():

    data = request.get_json()

    try:
        # If data is a string, parse it to a Python list/dict
        if isinstance(data, str):
            data = json.loads(data)

        processed_data = process_data(data)

        post_to_ingest(processed_data)

    except Exception as e:
        return json.dumps({"Status": 500, "Message": "An error occurred"})

    return "Done"