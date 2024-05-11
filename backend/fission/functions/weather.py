from ftplib import FTP
from elasticsearch import Elasticsearch, ApiError, TransportError
from elasticsearch.helpers import bulk
import warnings
import logging
from datetime import datetime
warnings.filterwarnings("ignore")
# from flask import request


def main():
    url = 'https://elasticsearch.elastic.svc.cluster.local:9200'
    user = "elastic"
    password = "cloudcomp"
    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False, request_timeout=60)

    if not es.ping():
        raise ValueError("Connection failed")

    # Define the index name
    index_name = "weather_past_obs"

    # Create the index with a mapping (optional)
    # You can customize the mapping to define data types for each field
    # You can customize the mapping to define data types for each field
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
            "Source": {"type": "keyword"},
        }
    }
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, mappings=mappings,
                          settings={"index": {"default_pipeline": "add_timestamp"}})
        source = ''
        month = ''
        tmpDate = ''
    else:
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
        response = es.search(index=index_name, body=search_body)
        # Extract the last document
        last_document = response['hits']['hits'][0]
        resp = es.search(
            index=index_name,
            body={"query": {"term": {"_id": last_document['_id']}}},
        )
        resp = resp['hits']['hits'][0]['_source']
        date = resp['Date'].split('/')
        month = date[2] + date[1]
        tmpDate = resp['Date']
        source = resp['Source']
    # FTP server details
    try:
        ftp_host = "ftp.bom.gov.au"
        ftp_user = "anonymous"
        ftp_passwd = ""  # Your email address as the password

        # Connect to FTP server
        ftp = FTP(ftp_host)
        ftp.login(user=ftp_user, passwd=ftp_passwd)
        ftp.encoding = 'latin-1'
        # Start from the directory
        ftp.cwd("/anon/gen/clim_data/IDCKWCDEA0/tables/tas/")

        # List directory contents
        suburbs = ftp.nlst()

        subdirectories = []
        # Loop through files

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
                bulk_data = []
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
                    bulk_data.append(document)
                if bulk_data:
                    try:
                        bulk(es, bulk_data, index=index_name)
                    except ApiError as e:
                        logging.error("Api Error: ", {e})
                        continue
                    except TransportError as e:
                        logging.error("Transport Error: ", {e})
                        continue
        # Close FTP connection
        logging.info("finish job")
        ftp.quit()
        return {
            "statusCode": 200
        }
    except Exception as e:
        logging.error(str(e))
