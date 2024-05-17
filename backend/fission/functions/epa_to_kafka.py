from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import uuid
import logging
import pandas as pd

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Constant url of the epa
URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'
KEY = '96ff8ef9e03048e2bd2fa342a5d80587'

RUN_FROM = 'bastion'

if RUN_FROM == 'bastion' : ES_URL, ES_HEADERS = 'https://elasticsearch.elastic.svc.cluster.local:9200', None
if RUN_FROM == 'uni_wifi' : ES_URL, ES_HEADERS = 'https://172.26.135.52:9200', {'HOST': 'elasticsearch'}

ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "cloudcomp"

BOOTSTRAP_KAFKA = 'kafka-kafka-bootstrap.kafka.svc:9092'
if RUN_FROM == 'bastion' : BOOTSTRAP_KAFKA = 'kafka-kafka-bootstrap.kafka.svc:9092'
if RUN_FROM == 'uni_wifi' : BOOTSTRAP_KAFKA = 'https://172.26.135.52:9092'

TOPIC_NAME = 'airquality-kafka'
CONFIRM_TOPIC_NAME = 'airquality-uploaded-kafka'

PULL_RATE = 100

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def fetch_epa() -> tuple[str,str] :
    """
    Get the current data from the EPA

    @returns two strings, one is the message_id and one is the message
    """
    headers = {
        'Cache-Control': 'no-cache',
        'X-API-Key': KEY,
        'User-agent': 'CloudCluster'
    }
    resp = requests.get(URL, headers=headers)
    
    # Generate a unique ID
    message_id = str(uuid.uuid1())

    buffer_message = str({"message_id": message_id, "body":resp.text})

    return message_id, buffer_message


def produce_kafka_buffer_message(response_txt : str, bootstrap_servers, topic_name) -> bool :
    """
    Stores the EPA response into a Kafka message

    @param is the response of the EPA API
    @returns a string
    """
    # Connect to Kafka and set up a producer client
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                             value_serializer=json_serializer)
    
    try :
        producer.send(topic_name, value=response_txt)

    except Exception as e :
        logging.error(e)
        return False
    
    return True


def produce_kafka_confirm_message(message_id, bootstrap_servers , topic_name) :


    # Connect to Kafka and set up a producer client
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                             value_serializer=json_serializer)
    
    try :
        confirmation = str({"state":"Success", "message_id":message_id})
        producer.send(topic_name, value=confirmation)

    except Exception as e :
        logging.error(e)
        return False
    

def main():
    """
    Pull the most recent data from the EPA and sends it to a Kafka message
    """
    message_id, buffer_message = fetch_epa()
    logging.info('Epa fetched')
    uploaded = produce_kafka_buffer_message(buffer_message, BOOTSTRAP_KAFKA, TOPIC_NAME)
    logging.info('Message Produced, checking if message was received by Kafka')

    if uploaded:
        logging.info('Buffer message sent to Kafka')
        produce_kafka_confirm_message(message_id, BOOTSTRAP_KAFKA, CONFIRM_TOPIC_NAME)
    else : 
        logging.error('Could not send buffer message to Kafka')
    
    return 'Done'

if __name__ == '__main__':
    main()

