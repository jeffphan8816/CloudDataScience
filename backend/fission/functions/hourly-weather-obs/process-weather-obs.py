import json
import requests

from collections import OrderedDict
from flask import request

def process_data(raw_data):

    renames = {
        "name": "Station Name",
        "rel_hum": "Humid",
        "air_temp": "Temp",
        "apparent_t": "Apparent Temp",
        "wind_spd_kmh": "WindSpeed",
        "rain_trace": "Rain",
    }

    other_renames = {
        "sort_order": None,
        "wmo": "ID",
        "history_product": "Old ID",
        "local_date_time": "Date-Time",
        "local_date_time_full": "Full Date-Time",
        "aifstime_utc": "UTC Time",
        "lat": "Latitude",
        "lon": "Longitude",
        "cloud": "Cloud",
        "cloud_base_m": "Cloud Base (m)",
        "cloud_oktas": "Cloud (oktas)",
        "cloud_type": "Cloud Type",
        "cloud_type_id": "Cloud Type ID",
        "delta_t": "DeltaT (C)",
        "gust_kmh": "Gust (km/h)",
        "gust_kt": "Gust (kt)",
        "dewpt": "Dew Point (C)",
        "press": "Pressure (hPa)",
        "press_msl": "Pressure MSL (hPa)",
        "press_qnh": "Pressure QNH (hPa)",
        "press_tend": "Pressure Trend",
        "sea_state": "Sea State",
        "swell_dir_worded": "Swell Direction",
        "swell_height": "Swell Height",
        "swell_period": "Swell Period",
        "vis_km": "Visibility (km)",
        "weather": "Weather",
        "wind_dir": "Wind Direction",
        "wind_spd_kmh": "Wind Speed (km/h)",
        "wind_spd_kt": "Wind Speed (kt)"
    }

    rename_mapping = {**renames, **other_renames}

    renamed_data = {}

    for key, value in raw_data.items():
        new_key = rename_mapping.get(key)
        if new_key:
            if new_key in renamed_data:
                renamed_data[new_key] = [renamed_data[new_key], value]
            else:
                renamed_data[new_key] = value
        else:
            renamed_data[key] = value

    key_order = [
        "Station Name", 'ID', "Humid", "Temp", "Apparent Temp", "WindSpeed", "Rain",

        'Date-Time', 'Full Date-Time', 'UTC Time',
        'Latitude', 'Longitude', 'Cloud', 'Cloud Base (m)', 'Cloud (oktas)', 'Cloud Type', 'Cloud Type ID',
        'DeltaT (C)', 'Gust (km/h)', 'Gust (kt)', 'Temp', 'Dew Point (C)',
        'Pressure (hPa)', 'Pressure MSL (hPa)', 'Pressure QNH (hPa)', 'Pressure Trend',
        'Sea State', 'Swell Direction', 'Swell Height', 'Swell Period', 'Visibility (km)',
        'Weather', 'Wind Direction', 'Wind Speed (km/h)', 'Wind Speed (kt)'
        ]


    ordered_data = OrderedDict()

    for key in key_order:
        if key in renamed_data:
            ordered_data[key] = renamed_data[key]

    return ordered_data

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