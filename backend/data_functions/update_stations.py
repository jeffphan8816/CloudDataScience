"""
Update station data

The original station location upload based on prior weather data, but there is a mismatch between
these and the current station data from the BOM website.

What orginiated as a simple script to update the station data in the database has evolved into
a more complex script as had to scrape from the BOM website as not all ids match (and need
location data for the new stations for the ap).
"""

import json
import requests
import time

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "cloudcomp"
ES_HEADERS = {'HOST': 'elasticsearch'}

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)

if not es.ping():
    raise ValueError("Connection failed")

# Define the index name
index_name = "station_locations"

def get_all_stations():
    response = es.search(
        index=index_name,
        body={
            "query": {
                "match_all": {}
            }
        },
        size=10000
    )

    return response['hits']['hits']

# Fetch all existing stations
all_stations = get_all_stations()

# Save all stations before ES goes down again....
with open('./data/all_stations_from_es.json', 'w') as json_file:
    json.dump(all_stations, json_file)

# --------------------

def scrape_station_details(url):
    """
    These details aren't in the JSON data endpoint, so we need to scrape the BOM website to get them...

    At the top of the page, there is a div with class 'p-id' that contains the p-id value
    which looks to be the old Station ID

    The station details are in a table with class 'stationdetails', which seems to match the old data structure
    """

    response = requests.get(url)
    html_content = response.content

    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract the p-id value - This looks to be what the old Station ID was
    p_id_div = soup.find('div', class_='p-id')
    p_id_value = p_id_div.text.strip() if p_id_div else 'N/A'

    # Extract station details from the table with class 'stationdetails'
    table = soup.find('table', class_='stationdetails')
    rows = table.find_all('tr')

    station_details = {}

    for row in rows:
        columns = row.find_all('td')
        for column in columns:
            text = column.get_text(strip=True)
            if text == "Station Details":
                continue
            if ':' in text:
                key, value = map(str.strip, text.split(':', 1))
                station_details[key] = value

    # Add the p-id to the station details
    station_details['p-id'] = p_id_value

    return station_details

# Get the new data from the BOM lookups
with open("./data/bom_groupings.json", "r") as f:
    bom_groupings = json.load(f)

bom_stations = []

for state in bom_groupings:
    print(state)
    with open(f"./data/bom_lookup_{state.lower()}.json", "r") as f:
        bom_lookup = json.load(f)

    for region in bom_groupings[state]["Regions"]:
        print(region)

        for station in bom_lookup[region]:

            # Add the json_url endpoint for the station
            station["json_url"] = (
                station["url"].replace("shtml", "json").replace("/products/", "/fwo/")
            )

            # Add the state and region
            station["State"] = state
            station["Region"] = region

            # Get the old station details
            # details = scrape_station_details(station["url"])
            # time.sleep(1) # Sleep for 1 second to avoid rate limiting

            # details["Station Name"] = details.pop("Name")
            # details["Station ID"] = details.pop("p-id")

            # station.update(details)

            bom_stations.append(station)


with open('./data/bom_stations.json', 'w') as json_file:
    json.dump(bom_stations, json_file)
