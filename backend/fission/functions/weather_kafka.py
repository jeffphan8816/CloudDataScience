# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

from ftplib import FTP
import warnings
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from flask import request
import json
import os
warnings.filterwarnings("ignore")


def main():
    """
    This function fetches weather data from the Bureau of Meteorology FTP server and sends it to a Kafka topic.
    
    Parameters:
    -----------
    None
    
    Returns:
        JSON object with status code and message
    """

    config = {}


    for key in os.listdir('/secrets/default/kafka'):
        with open(os.path.join('/secrets/default/kafka', key), 'rt') as file:
            config[key] = file.read()

    STATE_HEADER = 'X-Fission-Params-State'
    BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
    ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
    EMPTY = json.dumps({'Status': 200, 'Data': []})
    
    # Check parameters
    if STATE_HEADER not in request.headers:
        return BAD_PARAMS
    state = request.headers[STATE_HEADER]
    kafka_topic = config['KAFKA_TOPIC']
    count = 0
    def json_serializer(data):
        return json.dumps(data).encode('utf-8')
    producer = KafkaProducer(bootstrap_servers=config['URL'], value_serializer=json_serializer)
    #use consumer to get the last record
    consumer = KafkaConsumer(kafka_topic,
                             auto_offset_reset="earliest",
                             bootstrap_servers=config['URL'],
                             enable_auto_commit=True)
    partition = TopicPartition(kafka_topic, 0)
    end_offset = consumer.end_offsets([partition])
    resp = []
    if list(end_offset.values())[0] != 0:
        consumer.seek(partition,list(end_offset.values())[0]-1)
        for message in consumer:
            # Process the message
            resp = json.loads(message.value.decode())
            print(resp[-1])
            break  # Stop after processing the first message
    consumer.close()

    if not resp:
        print("start again")
        source = ''
        month = ''
        tmpDate = ''
    else:
        date = resp[-1]['Date'].split('/')
        month = date[2] + date[1]
        tmpDate = resp[-1]['Date']
        source = resp[-1]['source']
    # FTP server details
    try:
        ftp_host = "ftp.bom.gov.au"
        ftp_user = "anonymous"
        ftp_passwd = "" 
        index_name = config['ES_WEATHER_INDEX']
        # Connect to FTP server
        ftp = FTP(ftp_host)
        ftp.login(user=ftp_user, passwd=ftp_passwd)
        ftp.encoding = 'latin-1'
        # Start from the directory
        ftp.cwd(f"/anon/gen/clim_data/IDCKWCDEA0/tables/{state}/")

        # List directory contents
        suburbs = ftp.nlst()
        
        subdirectories = []
        # Loop through files
        bulk_data = []
        flag = 0
        if source:
            for suburb in suburbs:
                if suburb == source:
                    flag = 1
                if flag:
                    subdirectories.append(suburb)
        else:
            for suburb in suburbs:
                subdirectories.append(suburb)
        for subdirectory in subdirectories:
            files = ftp.nlst(subdirectory)
            for file in files:
                data = []
                if file.endswith('.csv'):
                    tmpMonth = file.split('-')[-1].split('.')[0]
                    today = datetime.today()
                    # Format year and month as YYYYMM
                    curr_month = f"{today.year:04d}{today.month:02d}"
                    if curr_month == month:
                        continue
                    if month:
                        if int(float(tmpMonth)) < int(float(month)):
                            continue
                    # Process the file line by line and add data to bulk_data list
                    ftp.retrlines(f"RETR {file}", data.append)
                data = data[13:-1]
                for line in data:
                    row = line.split(",")
                    if tmpDate:
                        d1 = datetime.strptime(tmpDate, "%d/%m/%Y")
                        d2 = datetime.strptime(row[1], '%d/%m/%Y')
                        if d1 >= d2:
                            continue
                    document = {
                        "_index": index_name,
                        "Station Name": row[0],
                        "Date": row[1],
                        "Evapo-Rain": row[2] if not row[2] == ' ' else "-1.0",
                        "Rain": row[3] if not row[3] == ' ' else "-1.0",
                        "Pan-Rain": row[4] if not row[4] == ' ' else "-1.0",
                        "Max Temp": row[5] if not row[5] == ' ' else "-999.0",
                        "Min Temp": row[6] if not row[6] == ' ' else "-999.0",
                        "Max Humid": row[7] if not row[7] == ' ' else "-1",
                        "Min Humid": row[8] if not row[8] == ' ' else "-1",
                        "WindSpeed": row[9] if not row[9] == ' ' else "-1.0",
                        "UV": row[-1] if not row[-1] == ' ' else "-1.0",
                        "source": subdirectory
                    }
                    count += 1
                    bulk_data.append(document)
                    if len(bulk_data) == 1000:
                        producer.send(kafka_topic, value=bulk_data)
                        bulk_data = []
        if bulk_data:
            producer.send(kafka_topic, value=bulk_data)
        # Close FTP connection
        logging.info("finish job")
        ftp.quit()
        return {
            "statusCode": 200,
            "count": count
        }
    except Exception as e:
        logging.error(e)
        return {
            "statusCode": 500,
            "body": str(e) + "count: " + str(count),
            "count": count
        }
