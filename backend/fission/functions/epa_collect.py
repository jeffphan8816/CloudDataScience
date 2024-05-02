from datetime import datetime
from elasticsearch import Elasticsearch
import json
import pandas as pd
import requests

# Constant url of the epa
URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'

def fetch_epa() -> list[dict]:
    """
    Get the current data from the EPA

    @returns a list of data dictionaries
    """
    headers = {
        'Cache-Control': 'no-cache',
        'X-API-Key': '96ff8ef9e03048e2bd2fa342a5d80587',
        'User-agent': 'CloudCluster'
    }
    resp = requests.get(URL, headers=headers)
    data = json.loads(resp.text)
    out = []

    # Go thorugh each record returned
    if 'records' not in data.keys():
        return out
    for record in data['records']:
        # Pull out location
        if 'geometry' not in record.keys():
            continue
        if 'coordinates' not in record['geometry'].key():
            continue
        location = record['geometry']['coordinates']
        # Then get the parameters
        if 'parameters' not in record.keys():
            continue
        parameters = record['parameters']
        for parameter in parameters:
            # Pull out name
            if 'name' not in parameter.keys():
                continue
            name = parameter['name']
            if 'timeSeriesReadings' not in parameter.keys():
                continue
            for series in parameter['timeSeriesReadings']:
                # Get all the readings
                if 'readings' not in series.keys():
                    continue
                for reading in series['readings']:
                    # Check for required data
                    cont = True
                    for key in ['since', 'until', 'averageValue']:
                        if key not in reading.keys():
                             cont = False
                    if not cont:
                        continue
                    # Build dictionary and append
                    toAdd = {}
                    toAdd['name'] = name
                    toAdd['location'] = location
                    toAdd['start'] = datetime.strptime(
                        reading['since'], '%Y-%m-%dT%H:%M:%SZ')
                    toAdd['end'] = datetime.strptime(
                        reading['since'], '%Y-%m-%dT%H:%M:%SZ')
                    toAdd['value'] = reading['averageValue']
                    out.append(toAdd)
    return out



def accepting_new_data(new_data, current_data) -> list:
    """
    Extract new data based on api return and current data

    @param new_data is the data pulled from the EPA as a DataFrame
    @param current_data is the data in elastic search as a DataFrame
    @returns a list of what data needs to be inserted
    """
    latest_current_df = current_data.groupby(['name', 'location'])['end'].max()
    kept_data = new_data.copy()

    for index in new_data.index:

        name = new_data.loc[index, 'name']
        # Need to convert coorinates to tuple
        location = (new_data.loc[index,'location'][0] , new_data.loc[index,'location'][1])

        # Check for collisions
        if name in latest_current_df.index:
            if location in latest_current_df[new_data.loc[index,'name']].index :
                if new_data.loc[index,'end'] <= latest_current_df[new_data.loc[index,'name']][new_data.loc[index,'location']] :
                    kept_data.drop(index, axis='index')
    
    return kept_data

def upload(data, es):
    print(data)
#   try:
    es.index(index="airquality", document=data)
    return True
    # except : 
    #     upload(data, es)


def main(): 
    """
    Pull the most recent data from the EPA and check what needs to be inserted
    """
    url = 'https://elasticsearch:31001'
    user = "elastic"
    password = "cloudcomp"

    # Connect to database
    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)
    if not es.ping():
        raise ValueError("Connection failed")

    # Fetch data
    new_data = fetch_epa()
    df_new_data = pd.DataFrame.from_records(new_data,index=range(len(new_data))) 


    # Get existing data after first date of new data
    # oldest_start_new_data = df_new_data['start'].min()
    datetime_str = '05/01/24T02:00:00'
    oldest_start_new_data = datetime.strptime(datetime_str, '%m/%d/%yT%H:%M:%S')
    query_res = es.search(index='airquality', body = {
        "query": {
            "range": {
                "end": {
                    "gte": oldest_start_new_data,
                }
            }
        }
    }) 

    # Clean up the data returned from elastic search, convert list to tuple
    query_list = [query_res['hits']['hits'][i]['_source'] for i in range(len(query_res['hits']['hits']))]
    current_data = pd.DataFrame.from_records(query_list,index=range(len(query_list)))
    current_data['location'] = current_data['location'].apply(lambda location  : (location[0],location[1]))
    # Convert date strings
    current_data['start'] = current_data['start'].apply(lambda s : datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
    current_data['end'] = current_data['end'].apply(lambda s : datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))

    # Switch coordinate order for new data and move to tuple
    df_new_data['location'] = df_new_data['location'].apply(lambda location  : (location[1],location[0]))

    # Remove collisions from new data
    to_upload = accepting_new_data(df_new_data, current_data)

    for line in to_upload :
         # upload(line,es)


if __name__ == '__main__' :
    main()
