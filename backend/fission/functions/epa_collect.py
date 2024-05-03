from datetime import datetime
from elasticsearch import Elasticsearch
import json
import logging
import pandas as pd
import requests
from elasticsearch.helpers import bulk

# Constant url of the epa
URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'

logger = logging.getLogger('epa_collect.py')
logging.basicConfig(level=logging.INFO)

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
        if 'coordinates' not in record['geometry'].keys():
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
                    toAdd['measure_name'] = name
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
    print(current_data['measure_name'].unique())
    print(current_data['location'].unique())
    latest_current_df = current_data.groupby(['measure_name', 'location'])['end'].max()
    print(latest_current_df)
    kept_data = new_data.copy()

    for index in new_data.index:

        name = new_data.loc[index, 'measure_name']
        # Need to convert coorinates to tuple
        location = (new_data.loc[index,'location'][0] , new_data.loc[index,'location'][1])

        # Check for collisions
        if name in latest_current_df.index:
            if location in latest_current_df[new_data.loc[index,'measure_name']].index :
                if new_data.loc[index,'end'] <= latest_current_df[new_data.loc[index,'measure_name']][new_data.loc[index,'location']] :
                    kept_data.drop(index, axis='index')
    
    return kept_data

def upload(data, es):
    cont = True
    while cont:
        try:
            bulk(es, [data], index='airquality')
            cont = False
            logger.info('Uploaded ' + str(data))
        except : 
            cont = True
            logger.warn('Failed to upload ' + str(data) + ' RETRYING')


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
    query_res = es.search(index='airquality', body = {
        "query": {
            "match_all": {}
        }
    }) 

    # Switch coordinate order for new data and move to tuple
    df_new_data['location'] = df_new_data['location'].apply(lambda location  : (location[1],location[0]))

    # Clean up the data returned from elastic search, convert list to tuple
    query_list = [query_res['hits']['hits'][i]['_source'] for i in range(len(query_res['hits']['hits']))]
    if not len(query_list) == 0:
        current_data = pd.DataFrame.from_records(query_list,index=range(len(query_list)))
        print(current_data['measure_name'].unique())
        current_data['location'] = current_data['location'].apply(lambda location  : (location[0],location[1]))
        # Convert date strings
        current_data['start'] = current_data['start'].apply(lambda s : datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
        current_data['end'] = current_data['end'].apply(lambda s : datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))

        # Remove collisions from new data
        to_upload = accepting_new_data(df_new_data, current_data)
    else:
        to_upload = df_new_data

    print(to_upload)
    print(to_upload['measure_name'].unique())
    for line in to_upload.to_dict(orient='records') :
        upload(line, es)

    return "Done"


if __name__ == '__main__' :
    main()
