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


    datetime_str = '09/19/22 13:55:26'
    oldest_start_new_data = datetime.strptime(datetime_str, '%m/%d/%y %H:%M:%S')

    print(oldest_start_new_data)

    query_res = es.sql.query(body={'query' : f'SELECT end FROM airquality'}) #TODO WHERE end > {oldest_start_new_data}
    print(query_res)
    print(type(query_res))



if __name__ == '__main__' :
    main()
