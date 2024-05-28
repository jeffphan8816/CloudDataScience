# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

from collections import OrderedDict

def clean_weather(raw_data):

    # Rename variables to match previous data
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

    # renamed_data
    # renamed_data.keys()

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
