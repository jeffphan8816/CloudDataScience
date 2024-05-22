from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import os
import logging

if __name__ == '__main__':
    logger = logging.getLogger('reupload_epa.py')
    logging.basicConfig(level=logging.INFO)

    BATCH_SIZE = 500

    RUN_FROM = 'uni_wifi'

    if RUN_FROM == 'bastion' : ES_URL, ES_HEADERS = 'https://elasticsearch:31001', None
    if RUN_FROM == 'uni_wifi' : ES_URL, ES_HEADERS = 'https://172.26.135.52:9200', {'HOST': 'elasticsearch'}

    # ENTER CREDENTIALS BELOW TO USE
    USER = ""
    PASSWORD = ""
    es = Elasticsearch([ES_URL], basic_auth=(USER, PASSWORD), headers=ES_HEADERS, verify_certs=False)

    if not es.ping():
        raise ValueError("Connection failed")

    INDEX_NAME = 'airquality'

    def get_crash_data(file_path: str) -> list:
        """
        Load a cleaned dataframe of the locally download airquality data

        @param file_path: str
        @return pd.DataFrame
        """

        # Check that file exists
        if not os.path.isfile(file_path):
            raise ValueError

        # Read data and clean
        df = pd.read_csv(file_path)

        df['location'] = df['location'].apply(lambda loc_str: tuple([float(coordinate) 
                                                            for coordinate in loc_str[1:-1].split(',')]))
        type(df['start'])
        logging.info(f'{type(df["created_at"][0])=}')
        logging.info(f'{type(df["start"][0])=}')
        logging.info(f'{type(df["end"][0])=}')
        logging.info(f'{type(df["location"][0])=}')
        logging.info(f'{type(df["measure_name"][0])=}')
        logging.info(f'{type(df["value"][0])=}')

        return df.to_dict(orient='records')


    if __name__ == '__main__':
        result_list = get_crash_data('../../data/airquality_2024-05-17_13-30.csv')

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
