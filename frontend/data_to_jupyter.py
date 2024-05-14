import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import matplotlib.pyplot as plt

import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def time_range_query(time_range, date_col_name='date', format_string='%Y-%m-%dT%H:%M:%S'):
    return {'query': {
                   'range': {
                       date_col_name: {
                           'gte': datetime.strptime(time_range[0],'%Y-%m-%d').strftime(format_string), # greater than 
                           'lt':  datetime.strptime(time_range[1],'%Y-%m-%d').strftime(format_string)  # less than
                       }
                   }
               }
           }


def query_to_pandas(query, es, index):
 
    print(query)
    query_list = []
    nb_doc = es.count(index="crimes", body=query)["count"]

    with tqdm(total=nb_doc, desc=f'Fetching {index} index') as prog_bar :
        for doc in scan(es, query=query, index=index):
            query_list.append(doc['_source'])
            prog_bar.update(1)

    return pd.DataFrame.from_records(query_list,index=range(len(query_list)))


def fetch_weather_crime(year_range, run_from='bastion'):

    if run_from == 'bastion' : url = 'https://elasticsearch:31001'
    elif run_from == 'uni_wifi' : url = 'https://172.26.135.52:9200'

    user = 'elastic'
    password = 'cloudcomp'
    headers = {'HOST': 'elasticsearch'}

    es = Elasticsearch([url], headers=headers, basic_auth=(user, password), verify_certs=False)
    if es.ping() :
        print('--Connected to Elastic Search--')
    else :
        raise ValueError('Connection failed')

    query_weather = time_range_query(year_range, date_col_name='Date', format_string='%m/%d/%Y')
    df_weather = query_to_pandas(query_weather, es, 'weather_past_obs')

    df_weather = df_weather.rename(columns={'Date':'date'})
    df_weather['date'] = pd.to_datetime(df_weather['date'])
    df_weather = df_weather.drop(columns=['created_at','source'])

    for col in ['UV','Min Humid','Max Humid','Min Temp','Max Temp',
                'WindSpeed','Rain','Pan-Rain','Evapo-Rain'] :
        df_weather[col] = df_weather[col].astype(float)


    query_crimes = time_range_query(year_range, date_col_name='reported_date')
    df_crimes = query_to_pandas(query_crimes, es, 'crimes')

    df_crimes = df_crimes.rename(columns={'reported_date':'date'})
    df_crimes['date'] = pd.to_datetime(df_crimes['date'])

    return df_crimes, df_weather


def call_fission_function(path, payload, run_from = 'bastion'):

    if run_from == 'bastion' : url = 'https://elasticsearch:31001'
    elif run_from == 'uni_wifi' : url = 'https://172.26.135.52:9200'

    # Make a POST request to the HTTP trigger URL with the payload
    response = requests.get(url+path)
    if response.status_code == 200:
        print("Function executed successfully.")
        print("Response:", response.text)
    else:
        print("Error:", response.text)

