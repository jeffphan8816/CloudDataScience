from ftplib import FTP
from elasticsearch import Elasticsearch, ApiError, TransportError
from elasticsearch.helpers import bulk
import warnings
import logging

warnings.filterwarnings("ignore")


def main():
    url = 'https://elasticsearch.elastic.svc.cluster.local:9200'
    user = "elastic"
    password = "cloudcomp"
    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False, request_timeout=30)

    if not es.ping():
        raise ValueError("Connection failed")

    # Define the index name
    index_name = "weather_past_obs"

    # Create the index with a mapping (optional)
    # You can customize the mapping to define data types for each field

    # Check if the index exists
    if not es.indices.exists(index=index_name):
        return {
            "status_code": 404,
            "message": "Index Not Found!"
        }

    # Search for only one document (the last based on sorting)
    search_body = {
        "query": {
            "match_all": {}
        },
        "size": 1,
        "sort": [
            {
                "created_at": {
                    "order": "desc"
                }
            }
        ]
    }

    # Execute the search
    response = es.search(index=index_name, body=search_body)

    # Extract the last document
    last_document = response['hits']['hits'][0]

    print("Last document id :", )
    resp = es.search(
        index=index_name,
        body={"query": {"term": {"_id": last_document['_id']}}},
    )
    resp = resp['hits']['hits'][0]['_source']
    print(resp)
    date = resp['Date'].split('/')
    month = date[2]+date[1]
    source = resp['source']
    print(date)
    print(source)
    print(month)
    # Close FTP connection
    logging.info("finish job")
    return {
        "statusCode": 200
    }
    # except Exception as e:
    #     logging.error(str(e))
