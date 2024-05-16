import warnings
warnings.filterwarnings('ignore')

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime
import pandas as pd
from tqdm import tqdm

RUN_FROM = 'uni_wifi'

if RUN_FROM == 'uni_wifi' : URL = 'https://172.26.135.52:9200'
if RUN_FROM == 'bastion' : URL = 'https://elasticsearch.elastic.svc.cluster.local:9200'

USER = 'elastic'
PASSWORD = 'cloudcomp'


def scan_index_to_pandas(es, index, query={'query': {'match_all': {}}}):
    for doc in scan(es, index=index, query=query):
        print(doc)

    nb_doc = es.count(index=index, query=query)['count']
    doc_list = []

    with tqdm(total=nb_doc, desc=f'Fetching {index} index') as prog_bar :
        for doc in scan(es, index=index, query=query):
            doc_list.append(doc['_source'])
            prog_bar.update(1)

    return pd.DataFrame.from_records(doc_list,index=range(len(doc_list)))


if __name__ == '__main__':

    es = Elasticsearch(URL, basic_auth=(USER, PASSWORD), verify_certs=False, request_timeout=60)
    print(es.ping())
    if not es.ping():
        raise ValueError("Connection failed")
    print(es.info())

    df_airqual = scan_index_to_pandas(es,'airquality')

    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    df_airqual.to_csv(f'airquality_{now}.csv', index=False)