import os
import asyncio
import json
import logging
import requests

import aiohttp
import numpy as np
import pandas as pd
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
async def fetch_weather(session, url):
    """
    Asynchronously fetches weather data from a given URL using an aiohttp session.

    Parameters:
    -----------
    session : aiohttp.ClientSession
        The aiohttp session to use for the request.
    url : str
        The URL to fetch the weather data from.

    Returns:
    --------
    dict
        A dictionary containing the weather data and information about the response.
        If the request is successful, the dictionary contains the weather data and
        'response_success' is set to True. If an error occurs, 'response_success' is
        set to False and the 'response' key contains the response object.

    Raises:
    -------
    Does not raise any exceptions, but logs them and returns a dictionary indicating
    the request was not successful.
    """
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            df = data["observations"]["data"][0]

            # Add info on the response
            df["response_success"] = True
            df["response"] = response.status

            return df

    except (aiohttp.ClientError, aiohttp.http_exceptions.HttpProcessingError) as e:
        logger.error(f"HTTP error encountered when fetching weather data: {e}")
        return {
            "response_success": False,
            "response": response.status,
        }  # Note: Return this instead of raising an error

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {
            "response_success": False,
            "response": response.status,
        }  # As above handling for other unforeseen errors


async def get_selected_weather(urls: str) -> list:
    """
    Asynchronously fetches weather data from a list of URLs using an aiohttp session.

    Parameters:
    -----------
    urls : list of str
        The list of URLs to fetch the weather data from.

    Returns:
    --------
    list
        A list of dictionaries containing the weather data and information about the responses.
        Each dictionary corresponds to one URL from the input list. If the request for a URL is
        successful, the corresponding dictionary contains the weather data and 'response_success'
        is set to True. If an error occurs, 'response_success' is set to False and the 'response'
        key contains the response object.

    Raises:
    -------
    Does not raise any exceptions, but logs them and returns dictionaries indicating
    the requests were not successful.
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results


def get_weather_data(selected_areas: list) -> pd.DataFrame:
    """
    Fetches weather data for the specified areas, converts the data into a pandas DataFrame,
    and joins the DataFrame with the original DataFrame of areas.

    Parameters:
    -----------
    selected_areas : list of str
        The list of areas to fetch the weather data for.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing the weather data for the specified areas. The DataFrame has
        the same columns as the original DataFrame of areas, plus additional columns for
        the weather data.

    Raises:
    -------
    Does not raise any exceptions, but logs them and returns an empty DataFrame if an error occurs.
    """

    current_dir = os.path.dirname(__file__)

    # Read in the lookup for all weather stations
    with open(os.path.join(current_dir, "bom_lookup.json"), "r") as f:
        bom_lookup = json.load(f)

    # TODO: Placeholder - will need to be updated to handle more than one area
    selected_areas = ["Melbourne"]
    selected_bom_areas = bom_lookup[selected_areas[0]]

    df = pd.DataFrame(selected_bom_areas)
    df["json_url"] = (
        df["url"].str.replace("shtml", "json").str.replace("/products/", "/fwo/")
    )

    # Handles both scenarios now
    # df["json_url"] = df["json_url"].str.replace("IDV", "asdf")
    # df["json_url"][0] = df["json_url"][0].replace("IDV", "asdf")

    # Running the asynchronous tasks
    urls = df["json_url"].tolist()
    loop = asyncio.get_event_loop()
    observations = loop.run_until_complete(get_selected_weather(urls))

    # Convert observations to DataFrame and join with original df
    df_observation = pd.DataFrame(observations)
    df = df.join(df_observation, rsuffix="_obs")

    # Remove NaN values as can't be inserted into ES
    df.replace({np.nan: None}, inplace=True)

    return df

@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def post_to_ingest(data):

    FISSION_URL = 'http://172.26.135.52:9090/'
    FISSION_HEADERS = {'HOST': 'fission'}

    ingest_url = f'{FISSION_URL}/ingest-weather-obs'
    json_data = data.to_json(orient='records')

    response = requests.post(ingest_url, json=json_data, headers=FISSION_HEADERS)

    return response


def main():

    df = get_weather_data(["Melbourne"])
    print(df)

    post_to_ingest(df)

    return "Done"
