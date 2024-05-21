from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
import ast
import logging
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

es_config = {}
for key in os.listdir('/secrets/default/es'):
    with open(os.path.join('/secrets/default/es', key), 'rt') as file:
        es_config [key] = file.read()

with open('/secrets/default/kafka/URL', 'rt') as file:
    BOOTSTRAP_KAFKA = file.read()

TOPIC_NAME = 'airquality-kafka'
CONFIRM_TOPIC_NAME = 'airquality-uploaded-kafka'

PULL_RATE = 100

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def consume_kafka_message(bootstrap_servers,topic_name) -> dict:
    """
    Consume the most recent message from Kafka topic, 
    and convert to dictionnary
    """
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, 
                             auto_offset_reset="earliest", 
                             enable_auto_commit=True,
                             value_deserializer=lambda x: json.loads(x.decode("utf-8")), 
                             consumer_timeout_ms=600)
    consumer.subscribe(topic_name)
    partition = TopicPartition(topic_name, 0)
    end_offset = consumer.end_offsets([partition])
    consumer.seek(partition,list(end_offset.values())[0]-1)

    last_message = None

    for message in consumer:
        logging.info(f'Message beginning {message.value}'[:100])
        last_message = message
        break
        
    if last_message is None : logging.error('No message found')

    return ast.literal_eval(last_message.value)


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
                        reading['until'], '%Y-%m-%dT%H:%M:%SZ')
                    toAdd['value'] = reading['averageValue']
                    cleaned.append(toAdd)

    df_new_data = pd.DataFrame.from_records(cleaned, index=range(len(cleaned)))

    df_new_data['start'] = df_new_data['start'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_new_data['end'] = df_new_data['end'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    df_new_data['start'] = df_new_data['start'].apply(lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
    df_new_data['end'] = df_new_data['end'].apply(lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))

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


def accepting_new_data(new_data: pd.DataFrame, current_data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute which data to keep and upload, based on time range inclusion, 
    to prevent duplicatas

    @param new_data is the data pulled from the EPA as a DataFrame
    @param current_data is the data in elastic search as a DataFrame
    @returns a list of what data needs to be inserted
    """
    latest_current_df = current_data.groupby(['measure_name', 'location'])['end'].max()
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
            logging.info('Uploaded ' + str(data))
        except:
            cont = True
            logging.warn('Failed to upload ' + str(data) + ' RETRYING')


def reset_kafka_confirm_message(bootstrap_servers , topic_name) :
    # Connect to Kafka and set up a producer client
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
                             value_serializer=json_serializer)
    
    try :
        confirmation = str({'state':'Reseted', 'message_id':0})
        producer.send(topic_name, value=confirmation)

    except Exception as e :
        logging.error(e)
        return False


def main():
    """
    Pull the most recent message from Kafka, check what needs to be inserted,
    and upload it to ES in accordance
    """
    logging.info('Welcome')
    buffer_message = consume_kafka_message(BOOTSTRAP_KAFKA, TOPIC_NAME)
    logging.info('Buffer Message Loaded')
    confirm_message = consume_kafka_message(BOOTSTRAP_KAFKA, CONFIRM_TOPIC_NAME)
    logging.info('Confirmation Message Loaded')

    if confirm_message['message_id'] != buffer_message['message_id'] :
        raise ValueError('Corresponding confirmation message not found in Kafka')

    reset_kafka_confirm_message(BOOTSTRAP_KAFKA, CONFIRM_TOPIC_NAME)
    logging.info('Confirmation Message Reseted')

    df_new_data = clean_kafka_data(ast.literal_eval(buffer_message['body']))
    logging.info('Buffer message cleaned and converted to a dataFrame')

    
    # Connect to ES database
    es = Elasticsearch(es_config['URL'], basic_auth=(es_config['USER'], es_config['PASS']), headers={'HOST': es_config['HOST']}, verify_certs=False)
    if not es.ping():
        raise ValueError('Connection failed')


    if df_new_data is not None :
        
        df_current_data = fetch_and_clean_ES_data(es, df_new_data)
        logging.info('Fetched Recent Elastic Search data for time range comparison')


        if df_current_data is not None :
            df_to_upload = accepting_new_data(df_new_data, df_current_data)
        
        else : 
            logging.info('ES query result is empty')
            df_to_upload = df_new_data

        # Upload
        for line in df_to_upload.to_dict(orient='records'):
            line['start'] = line['start'].strftime("%Y-%m-%dT%H:%M:%S")
            line['end'] = line['end'].strftime("%Y-%m-%dT%H:%M:%S")
            upload_to_ES(line, es)
        logging.info('Uploaded Data in Elastic Search')
    
    else : 
        logging.info('Nothing retrieved from Kafka')

    return 'Done'


if __name__ == '__main__':
    main()
