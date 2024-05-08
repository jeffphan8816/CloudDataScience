import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

ELASTIC_URL = 'http://172.26.135.52:5601'
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "cloudcomp"
DATA_PATH = '../../data/australian_postcodes.csv'

es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False)
if not es.ping():
    raise ValueError("Connection failed")

if __name__ == '__main__':
    df = pd.read_csv(DATA_PATH)
    df = df.drop(df.columns.difference(
        ['locality', 'state', 'lat', 'long']), axis=1)
    
    to_upload = []
    for row in df.to_dict(orient='records'):
        data = {}
        data['suburb'] = row['locality'].lower()
        data['state'] = row['state'].lower()
        data['location'] = (row['lat'], row['long'])
        to_upload.append(data)

    print('Upload size: ', len(to_upload))
    bulk(es, to_upload, index='locations')



