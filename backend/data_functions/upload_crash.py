# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

"""
The purpose of this file is to upload the static crash data to Elastic Search.
The data is read from a csv file, cleaned, then indexed.
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import os
import logging

if __name__ == '__main__':
    logger = logging.getLogger('upload_crash.py')
    logging.basicConfig(level=logging.INFO)

    BATCH_SIZE = 500

    # ENTER CREDENTIALS BELOW TO USE
    ES_URL = 'https://elasticsearch:31001'
    USER = ""
    PASSWORD = ""
    es = Elasticsearch([ES_URL], basic_auth=(USER, PASSWORD), verify_certs=False)

    if not es.ping():
        raise ValueError("Connection failed")

    INDEX_NAME = 'crashes'

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

        to_keep = ['crash_date', 'location',
                'light_condition', 'severity']
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



    if __name__ == '__main__':
        result_list = get_crash_data('../../data/sudo_tasmania_crash_2010_2020.csv')

        extra = 0
        if not len(result_list) % BATCH_SIZE == 0:
            extra = 1


        for i in range(len(result_list)//BATCH_SIZE + extra) :
            cont = True
            while cont :
                try :
                    bulk(es, result_list[i*BATCH_SIZE:(i+1)*BATCH_SIZE], index=INDEX_NAME)
                    cont = False
                    logger.info('Uploaded batch no ' + str(i))

                except :
                    cont = True
                    logger.warn('Failed to upload batch no ' + str(i) + ' RETRYING')

