from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaProducer, KafkaConsumer
import requests
import json
import uuid
import logging
import pandas as pd




# Constant url of the epa
URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'
KEY = '96ff8ef9e03048e2bd2fa342a5d80587'

ELASTIC_URL = 'https://elasticsearch.elastic.svc.cluster.local:9200'
ELASTIC_USER = "elastic"
ELASTIC_PASSWORD = "cloudcomp"

BOOTSTRAP_KAFKA = 'kafka-kafka-bootstrap.kafka.svc:9092'
TOPIC_NAME = 'airquality-kafka'
CONFIRM_TOPIC_NAME = 'airquality-uploaded-kafka'

PULL_RATE = 100

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

logger = logging.getLogger('epa_collect.py')
logging.basicConfig(level=logging.INFO)


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
    message_id = str(uuid.uuid4())

    buffer_message = str({'message_id': message_id, 'body':resp.text})

    return message_id, buffer_message


def produce_kafka_buffer_message(response_txt : str, bootstrap_servers , topic_name) -> bool :
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
        confirmation = str({'state':'Success', 'message_id':{message_id}})
        producer.send(topic_name, value=confirmation)

    except Exception as e :
        logging.error(e)
        return False


def consume_kafka_message(bootstrap_servers,topic_name) -> dict:
    """
    Consume the most recent message from Kafka topic, 
    and convert to dictionnary
    """
    # Connect to Kafka and set up a consumer client
    consumer = KafkaConsumer(topic_name,
                            auto_offset_reset='earliest',
                            bootstrap_servers=bootstrap_servers,
                            enable_auto_commit=True)
    
    message_text = consumer[0].value.decode('utf-8')  #consumer[0] is the earliest message of the topic
    return json.loads(message_text)


def clean_kafka_data(data : dict) -> pd.DataFrame | None :
    """
    Clean the data stored to a list of records, only keeping seeked columns

    @returns a list of data dictionaries
    """
    cleaned = []
    # Go thorugh each record returned
    if 'records' not in data.keys():
        return None
    for record in data['records']:
        # Pull out location
        if 'geometry' not in record.keys():
            continue
        if 'coordinates' not in record['geometry'].keys():
            continue
        location = record['geometry']['coordinates']
        # Then get the parameters
        if 'parameters' not in record.keys():
            continue
        parameters = record['parameters']
        for parameter in parameters:
            # Pull out name
            if 'name' not in parameter.keys():
                continue
            name = parameter['name']
            if 'timeSeriesReadings' not in parameter.keys():
                continue
            for series in parameter['timeSeriesReadings']:
                # Get all the readings
                if 'readings' not in series.keys():
                    continue
                for reading in series['readings']:
                    # Check for required data
                    cont = True
                    for key in ['since', 'until', 'averageValue']:
                        if key not in reading.keys():
                            cont = False
                    if not cont:
                        continue
                    # Build dictionary and append
                    toAdd = {}
                    toAdd['measure_name'] = name
                    toAdd['location'] = location
                    toAdd['start'] = datetime.strptime(
                        reading['since'], '%Y-%m-%dT%H:%M:%SZ')
                    toAdd['end'] = datetime.strptime(
                        reading['since'], '%Y-%m-%dT%H:%M:%SZ')
                    toAdd['value'] = reading['averageValue']
                    cleaned.append(toAdd)

    df_new_data = pd.DataFrame.from_records(cleaned, index=range(len(cleaned)))
    
    # Switch coordinate order for new data and move to tuple
    df_new_data['location'] = df_new_data['location'].apply(
        lambda location: (location[1], location[0]))
    
    return df_new_data


def fetch_and_clean_ES_data(es, new_data: pd.DataFrame) -> pd.DataFrame | None :
    # Get existing data after first date of new data
    oldest_start_new_data = new_data['start'].min()
    # Will pull PULL_RATE at a time
    query_list = []
    cont = True
    while cont:
        query_res = es.search(index='airquality',  body={
            "query": {
                "range": {
                    "end": {
                        "gte": oldest_start_new_data,
                    }
                }
            },
            "from": len(query_list),
            "size": PULL_RATE})
        # Clean up the data returned from elastic search, convert list to tuple
        cleaned_list = [query_res['hits']['hits'][i]['_source']
                        for i in range(len(query_res['hits']['hits']))]
        if (len(cleaned_list) < PULL_RATE):
            cont = False
        query_list.extend(cleaned_list)

    if len(query_list) == 0:
        return None
    
    # Clean the pulled data
    current_data = pd.DataFrame.from_records(
        query_list, index=range(len(query_list)))
    current_data['location'] = current_data['location'].apply(
        lambda location: (location[0], location[1]))
    # Convert date strings
    current_data['start'] = current_data['start'].apply(
        lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
    current_data['end'] = current_data['end'].apply(
        lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
    
    return current_data


def accepting_new_data(new_data: pd.DataFrame, current_data: pd.DataFrame) -> list[dict]:
    """
    Compute which data to keep and upload, based on time range inclusion, 
    to prevent duplicatas

    @param new_data is the data pulled from the EPA as a DataFrame
    @param current_data is the data in elastic search as a DataFrame
    @returns a list of what data needs to be inserted
    """
    latest_current_df = current_data.groupby(
        ['measure_name', 'location'])['end'].max()
    kept_data = new_data.copy()

    for index in new_data.index:

        name = new_data.loc[index, 'measure_name']
        # Need to convert coorinates to tuple
        location = (new_data.loc[index, 'location'][0],
                    new_data.loc[index, 'location'][1])

        # Check for collisions
        if name in latest_current_df.index:
            if location in latest_current_df[new_data.loc[index, 'measure_name']].index:
                if new_data.loc[index, 'end'] <= latest_current_df[new_data.loc[index, 'measure_name']][new_data.loc[index, 'location']]:
                    kept_data = kept_data.drop(index, axis='index')
    return kept_data


def upload_to_ES(data: list[dict], es: Elasticsearch) -> None:
    """
    Upload the data to elastic search. If fail retry

    @param data is a list of the data to insert
    @param es is the elastic search instance
    """
    cont = True
    while cont:
        try:
            bulk(es, [data], index='airquality')
            cont = False
            logger.info('Uploaded ' + str(data))
        except:
            cont = True
            logger.warn('Failed to upload ' + str(data) + ' RETRYING')


def main_to_Kafka():
    """
    Pull the most recent data from the EPA and sends it to a Kafka message
    """
    message_id, buffer_message = fetch_epa()
    uploaded = produce_kafka_buffer_message(buffer_message, BOOTSTRAP_KAFKA, TOPIC_NAME)
    
    if uploaded:
        logging.info('Buffer message sent to Kafka')
        produce_kafka_confirm_message(message_id, BOOTSTRAP_KAFKA, CONFIRM_TOPIC_NAME)
    else : 
        logging.error('Could not send buffer message to Kafka')
    
    #TODO get the status on the message upload and trigger main_to_ES in accordance


def main_to_ES():
    """
    Pull the most recent message from Kafka, check what needs to be inserted,
    and upload it to ES in accordance
    """
    buffer_message = consume_kafka_message(BOOTSTRAP_KAFKA, TOPIC_NAME)
    confirm_message = consume_kafka_message(BOOTSTRAP_KAFKA, CONFIRM_TOPIC_NAME)

    if confirm_message['message_id'] = buffer_message['message_id']

    df_new_data = clean_kafka_data(buffer_message['body'])

    # Connect to ES database
    es = Elasticsearch([ELASTIC_URL], basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False)
    if not es.ping():
        raise ValueError("Connection failed")    

    if df_new_data is not None :
        
        df_current_data = fetch_and_clean_ES_data(es, df_new_data)
        
        if df_current_data is not None :
            df_to_upload = accepting_new_data(df_new_data, df_current_data)
        
        else : 
            logging.info('ES query result is empty')
            df_to_upload = df_new_data

        # Upload
        for line in df_to_upload.to_dict(orient='records'):
            upload_to_ES(line, es)
    
    else : 
        logging.info('Nothing retrieved from Kafka')

    return "Done"

if __name__ == '__main__':
    main_to_Kafka()
    main_to_ES()
