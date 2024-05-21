"""
Loop through all stations by region for hourly weather trigger

"""

import os
import json
import logging
import requests

from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_state_regions():
    current_dir = os.path.dirname(__file__)
    # current_dir = "./backend/fission/functions/fetch-weather-obs/"
    with open(os.path.join(current_dir, "bom_groupings.json"), "r") as f:
        bom_lookup = json.load(f)

    return bom_lookup

@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def batch_request(state, region):

    FISSION_URL = "http://172.26.135.52:9090/"
    FISSION_HEADERS = {"HOST": "fission"}

    fetch_url = f"{FISSION_URL}/fetch-weather-obs"

    request_url = f"{fetch_url}?state={state}&region={region}"

    response = requests.get(request_url, headers=FISSION_HEADERS)

    return response


def main():

    # Get the list of states and regions
    state_regions = get_state_regions()

    # Loop through each state and region
    for state, regions in state_regions.items():
        for region in regions["Regions"]:
            print(f"Processing state: {state}, region: {region}")

            #TODO: lets be nice and slow down the requests once all enabled


            response = batch_request(state, region)
            return "Done" #TODO update after testsing

    return "Done"