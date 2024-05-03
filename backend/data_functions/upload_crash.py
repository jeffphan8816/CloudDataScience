from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import os
import logging

logger = logging.getLogger('upload_crash.py')
logging.basicConfig(level=logging.INFO)

BATCH_SIZE = 500

def get_crash_data(file_path: str) -> list:
    """
    Load a cleaned dataframe of crash data

    @param file_path: str
    @return pd.DataFrame
    """

    # Check that file exists
    if not os.path.isfile(file_path):
        raise ValueError

    # Read data and clean
    df = pd.read_csv(file_path)
    df = df.rename(columns={' crash_date': 'crash_date', ' latitude': 'latitude',
                            'light_condition': 'light_condition', ' longitude': 'longitude', ' severity': 'severity'})

    df['location'] = list(zip(df['longitude'], df['latitude']))
    df.drop(['latitude','longitude'], axis=1)

    to_keep = ['crash_date', 'latitude',
               'light_condition', 'longitude', 'severity']
    df_reduced = df.drop(set(df.columns)-set(to_keep), axis=1)

    # Try to convert severity to int
    severity_dict = {'Not known': -1, 'Property Damage Only': 0,
                     'Minor': 1, 'First Aid': 2, 'Serious': 3, 'Fatal': 4}
    try:
        df_reduced['severity'] = df_reduced['severity'].apply(
            lambda severity: severity_dict[severity])
    except KeyError:
        pass  # TODO  add in logs

    df_reduced = df_reduced.dropna()

    return df_reduced.to_dict(orient='records')


url = 'https://elasticsearch:31001'
user = "elastic"
password = "cloudcomp"
es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)

if not es.ping():
    raise ValueError("Connection failed")

index_name = 'crashes'


result_list = get_crash_data('../../data/sudo_tasmania_crash_2010_2020.csv')

start_batch = 75
bulk(es, result_list[start_batch*BATCH_SIZE:(start_batch+1)*BATCH_SIZE], index='crashes')


# for i in range(start_batch, len(result_list)//BATCH_SIZE) :
    # cont = True
    # while cont :
    #     try :
            # bulk(es, result_list[i*BATCH_SIZE:(i+1)*BATCH_SIZE], index='crashes')
        #     cont = False
        #     logger.info('Uploaded batch no ' + str(i))

        # except :
        #     cont = True
        #     logger.warn('Failed to upload batch no ' + str(i) + ' RETRYING')
