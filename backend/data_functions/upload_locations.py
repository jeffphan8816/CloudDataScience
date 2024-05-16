import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "cloudcomp"
DATA_PATH = '../../data/australian_postcodes.csv'
ES_HEADERS = {'HOST': 'elasticsearch'}
BATCH_SIZE = 100

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS)
if not es.ping():
    raise ValueError("Connection failed")

if __name__ == '__main__':
    df = pd.read_csv(DATA_PATH)
    df = df.drop(df.columns.difference(
        ['locality', 'state', 'lat', 'long', 'postcode']), axis=1)

    to_upload = []
    for row in df.to_dict(orient='records'):
        data = {}
        data['suburb'] = row['locality'].lower()
        data['state'] = row['state'].lower()
        data['location'] = (row['long'], row['lat'])
        data['postcode'] = row['postcode']
        to_upload.append(data)

    print('Upload size: ', len(to_upload))
    for doc in to_upload:
        print(doc)
        es.index(index='locations', document=doc)

