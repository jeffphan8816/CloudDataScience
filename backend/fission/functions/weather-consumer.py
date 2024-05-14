from elasticsearch import Elasticsearch, ApiError, TransportError
from elasticsearch.helpers import bulk
import warnings
from datetime import datetime
from kafka import KafkaConsumer
warnings.filterwarnings("ignore")
from flask import request, current_app
from time import time


def main():
    start_time = time()
    url = 'https://elasticsearch.elastic.svc.cluster.local:9200'
    user = "elastic"
    password = "cloudcomp"
    # # state = request.args.get('state')
    # state = 'tas'
    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False, request_timeout=60)
    if not es.ping():
        raise ValueError("Connection failed")
    # Define the index name
    index_name = "weather_past_obs_kafka"
    # Create the index with a mapping (optional)
    mappings = {
        "properties": {
            "Station Name": {"type": "keyword"},
            "State": {"type": "keyword"},
            "Date": {"type": "date", "format": "dd/MM/yyyy"},
            "Evapo-Rain": {"type": "float"},
            "Rain": {"type": "float"},
            "Pan-Rain": {"type": "float"},  # Changed to keyword for potentially non-numeric values
            "Max Temp": {"type": "float"},
            "Min Temp": {"type": "float"},
            "Max Humid": {"type": "integer"},  # Changed to integer for whole numbers
            "Min Humid": {"type": "integer"},
            "WindSpeed": {"type": "float"},
            "UV": {"type": "float"},
            "source": {"type": "keyword"},
        }
    }
    print(request.get_json(force=True))
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, mappings=mappings,
                          settings={"index": {"default_pipeline": "add_timestamp"}})
    try:
        bulk_data = request.get_json(force=True)  
        resp = bulk(es, bulk_data, index=index_name)
        current_app.logger.info(f'Indexing response: {resp}')
    except Exception as e:
        current_app.logger.error(f'Bulk indexing error: {str(e)}')
    return "ok"
