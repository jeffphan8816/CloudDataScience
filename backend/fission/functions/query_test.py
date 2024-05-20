import requests
import json
from datetime import datetime
from elasticsearch import Elasticsearch

from elasticsearch.helpers import bulk
import pandas as pd




URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'



def main(): 

    url = 'https://elasticsearch:31001'
    user = "elastic"
    password = "cloudcomp"

    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)

    if not es.ping():
        raise ValueError("Connection failed")


    datetime_str = '05/01/24 02:00:00'
    oldest_start_new_data = datetime.strptime(datetime_str, '%m/%d/%y %H:%M:%S')

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



if __name__ == '__main__' :
    main()
