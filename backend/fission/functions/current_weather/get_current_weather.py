
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

import requests

from flask import request

def get_closest_station(lat, lon):

    station_details = requests.post("http://localhost:8080/api/v1/fetch-weather-obs", params={"lat": lat, "lon": lon})

    return station_details


@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def fetch_weather(url):

    response = requests.get(url).json()
    out = response["observations"]["data"][0]

    # Add info on the response
    out["response_success"] = True
    out["response"] = response.status

    return out


    mappings = {
        "properties": {
            "Station Name": {"type": "keyword"},
            "State": {"type": "keyword"},
            "Date": {"type": "date", "format": "dd/MM/yyyy"},
            "Evapo-Rain": {"type": "float"},
            "Rain": {"type": "float"},
            "Pan-Rain": {"type": "float"},  # Changed to keyword for potentially non-numeric values
            "Max Temp": {"type": "float"},
            "Min Temp": {"type": "float"},
            "Max Humid": {"type": "integer"},  # Changed to integer for whole numbers
            "Min Humid": {"type": "integer"},
            "WindSpeed": {"type": "float"},
            "UV": {"type": "float"},
            "source": {"type": "keyword"},
        }
    }


def clean_weather():
    {
	"name": "Melbourne (Olympic Park)",
	"local_date_time": "20/10:00pm",
	"aifstime_utc": "20240520120000",
	"air_temp": 10.9, Temperature
	"apparent_t": 10.8, Apparent Temp

	"cloud": "-",
	"cloud_base_m": null,
	"cloud_oktas": null,
	"cloud_type_id": null,
	"cloud_type": "-",
	"delta_t": 0.7,
	"gust_kmh": 0,
	"gust_kt": 0,
	"dewpt": 9.5,
	"press": 1030.1,
	"press_qnh": 1030.1,
	"press_msl": 1030.1,
	"press_tend": "-",
	"rain_trace": "0.8", Rain since 9am
	"rel_hum": 91,
	"sea_state": "-",
	"swell_dir_worded": "-",
	"swell_height": null,
	"swell_period": null,
	"vis_km": "10",
	"weather": "-",
	"wind_dir": "CALM", Wind Direction
	"wind_spd_kmh": 0,
	"wind_spd_kt": 0
},






def main():

    lat = request.args.get("lat", "VIC")
    lon = request.args.get("lon", "CENTRAL")

    station_details = get_closest_station(lat, lon)

    raw_weather = fetch_weather(station_details["json_url"])

    clean_weather = clean_weather(raw_weather)




    return "Done"

