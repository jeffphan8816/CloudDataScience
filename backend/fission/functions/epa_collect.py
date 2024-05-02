import requests
import json
from datetime import datetime
from elasticsearch import Elasticsearch

from elasticsearch.helpers import bulk
import pandas as pd

URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'

def fetch_epa() -> list[dict]:
    headers = {
        'Cache-Control': 'no-cache',
        'X-API-Key': '96ff8ef9e03048e2bd2fa342a5d80587',
        'User-agent': 'CloudCluster'
    }
    resp = requests.get(URL, headers=headers)
    data = json.loads(resp.text)
    out = []
    if 'records' not in data.keys():
        return out
    for record in data['records']:
        location = record['geometry']['coordinates']
        if 'parameters' not in record.keys():
            continue
        parameters = record['parameters']
        for parameter in parameters:
            if 'name' not in parameter.keys():
                continue
            name = parameter['name']
            if 'timeSeriesReadings' not in parameter.keys():
                continue
            for series in parameter['timeSeriesReadings']:
                if 'readings' not in series.keys():
                    continue
                for reading in series['readings']:
                    cont = True
                    for key in ['since', 'until', 'averageValue']:
                        if key not in reading.keys():
                             cont = False
                    if not cont:
                        continue
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



def accepting_new_data(new_data, current_data):
    #readings is a list of dictionary, each one is a reading of the air quality 

    latest_current_df = current_data.groupby(['name', 'location'])['end'].max()

    kept_data = new_data.copy()

    for index in new_data.index:

        name = new_data.loc[index, 'name']
        location = (new_data.loc[index,'location'][0] , new_data.loc[index,'location'][1])

        if name in latest_current_df.index:
            if location in latest_current_df[new_data.loc[index,'name']].index :
                if new_data.loc[index,'end'] <= latest_current_df[new_data.loc[index,'name']][new_data.loc[index,'location']] :
                    kept_data.drop(index, axis='index')
    
    print(kept_data.head())
    kept_data['start']
    return kept_data

def upload(data, es):
    print(data)
#   try:
    es.index(index="airquality", document=data)
    return True
    # except : 
    #     upload(data, es)


def main(): 

    url = 'https://elasticsearch:31001'
    user = "elastic"
    password = "cloudcomp"

    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)

    if not es.ping():
        raise ValueError("Connection failed")

    new_data = fetch_epa()
    df_new_data = pd.DataFrame.from_records(new_data,index=range(len(new_data))) 

    # oldest_start_new_data = df_new_data['start'].min()
    datetime_str = '05/01/24T02:00:00'
    oldest_start_new_data = datetime.strptime(datetime_str, '%m/%d/%yT%H:%M:%S')
    print(oldest_start_new_data)

    query_res = es.search(index='airquality', body = {
        "query": {
            "range": {
                "end": {
                    "gte": oldest_start_new_data,
                }
            }
        }
    }) 
    print(query_res)

    query_list = [query_res['hits']['hits'][i]['_source'] for i in range(len(query_res['hits']['hits']))]
    
    current_data = pd.DataFrame.from_records(query_list,index=range(len(query_list)))
    current_data['location'] = current_data['location'].apply(lambda location  : (location[0],location[1]))

    df_new_data['location'] = df_new_data['location'].apply(lambda location  : (location[1],location[0]))

    current_data['start'] = current_data['start'].apply(lambda s : datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
    current_data['end'] = current_data['end'].apply(lambda s : datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))

    print(current_data.head())
    to_upload = accepting_new_data(df_new_data, current_data)

    print(to_upload)

    for line in to_upload :
         # upload(line,es)


if __name__ == '__main__' :
    main()
