# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import warnings
from flask import request, current_app
import os
import json
warnings.filterwarnings("ignore")

def main():
    """
    This function is the entry point for the weather-consumer application.
    It receives weather data in bulk format, indexes the data into Elasticsearch,
    and logs the indexing response.

    Returns:
        dict: A dictionary indicating the success of the operation with HTTP Repsonse 200.
    """

    config = {}
    for key in os.listdir('/secrets/default/es'):
        with open(os.path.join('/secrets/default/es', key), 'rt') as file:
            config[key] = file.read()
    config2 = {}
    for key in os.listdir('/secrets/default/kafka'):
        with open(os.path.join('/secrets/default/kafka', key), 'rt') as file:
            config2[key] = file.read()

    es = Elasticsearch([config['URL']], basic_auth=(
        config['USER'], config['PASS']), verify_certs=False, headers={'HOST': config['HOST']})

    
    if not es.ping():
        raise ValueError("Connection failed")
    # Define the index name
    index_name = config2['ES_WEATHER_INDEX']
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
    return {
        "statusCode": 200,
        "body": json.dumps("Completed")
    }
