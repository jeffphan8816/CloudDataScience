"""
The purpose of this file is to upload static crime data to Elastic Search
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
from datetime import datetime
import os
import logging

if __name__ == '__main__':
    logger = logging.getLogger('upload_crime.py')
    logging.basicConfig(level=logging.INFO)

    RUN_FROM = 'uni_wifi'

    if RUN_FROM == 'bastion' : URL, HEADERS = 'https://elasticsearch.elastic.svc.cluster.local:9200', None
    if RUN_FROM == 'uni_wifi' : URL, HEADERS = 'https://172.26.135.52:9200', {'HOST': 'elasticsearch'}

    # ENTER CREDENTIALS BELOW TO USE
    USER = ''
    PASSWORD = ''

    BATCH_SIZE = 500

    def get_crime_data(file_path: str) -> list:
        """
        Load a cleaned dataframe of crime data

        @param file_path: str
        @return pd.DataFrame
        """

        # Check that file exists
        if not os.path.isfile(file_path):
            raise ValueError
    #Reported Date,Suburb - Incident,Postcode - Incident,Offence Level 1 Description,Offence Level 2 Description,
    #Offence Level 3 Description,Offence count

        # Read data and clean
        df = pd.read_csv(file_path)
        df = df.rename(columns={'Reported Date': 'reported_date', 'Suburb - Incident': 'suburb', 
                                'Postcode - Incident': 'postcode', 'Offence Level 1 Description': 'description_1',
                                'Offence Level 2 Description': 'description_2', 'Offence Level 3 Description': 'description_3',
                                'Offence count': 'offence_count'})
        
        df = df.dropna()

        df['reported_date'] = df['reported_date'].apply(lambda date_str : datetime.strptime(date_str, '%d/%m/%Y'))

        return df.to_dict(orient='records')


    es = Elasticsearch(URL, basic_auth=(USER, PASSWORD), headers='HEADERS', verify_certs=False)

    if not es.ping():
        raise ValueError("Connection failed")

    index_name = 'crimes'

    file_names = [f'20{i}-{i+1}-data_sa_crime.csv' for i in range(21,23)]

    for file_name in file_names :

        result_list = get_crime_data(f'../../data/{file_name}')

        for i in range((len(result_list)//BATCH_SIZE)+1) :
            cont = True
            while cont :
                try :
                    bulk(es, result_list[i*BATCH_SIZE:(i+1)*BATCH_SIZE], index='crimes')
                    cont = False
                    logger.info(f'Uploaded batch no {i} in {file_name}')

                except :
                    cont = True
                    logger.warn(f'Failed to upload batch no {i} in {file_name}  RETRYING')
