from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from crashdata_functions import get_crash_data



url = 'https://elasticsearch:31001'
user = "elastic"
password = "cloudcomp"
es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)

if not es.ping():
    raise ValueError("Connection failed")

index_name = 'crashes'


result_list = get_crash_data('../../data/sudo_tasmania_crash_2010_2020.csv')




bulk(es, result_list, index='crashes')