from ftplib import FTP
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import warnings
warnings.filterwarnings("ignore")


def main():
    url = 'https://elasticsearch.elastic.svc.cluster.local:9200'
    user = "elastic"
    password = "cloudcomp"
    es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)

    if not es.ping():
        raise ValueError("Connection failed")

    # Define the index name
    index_name = "weather_data"

    # Create the index with a mapping (optional)
    # You can customize the mapping to define data types for each field
    mappings = {
        "properties": {
            "Station Name": {"type": "keyword"},
            "Date": {"type": "date"},
            "Evapo-Rain": {"type": "float"},
            "Rain": {"type": "float"},
            "Pan-Rain": {"type": "keyword"},  # Changed to keyword for potentially non-numeric values
            "Max Temp": {"type": "float"},
            "Min Temp": {"type": "float"},
            "Max Humid": {"type": "integer"},  # Changed to integer for whole numbers
            "Min Humid": {"type": "integer"},
            "WindSpeed": {"type": "float"},
            "UV": {"type": "float"},
        }
    }

    # Check if the index exists
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)


    # FTP server details
    ftp_host = "ftp.bom.gov.au"
    ftp_user = "anonymous"
    ftp_passwd = ""  # Your email address as the password

    # Connect to FTP server
    ftp = FTP(ftp_host)
    ftp.login(user=ftp_user, passwd=ftp_passwd)
    ftp.encoding = 'latin-1'
    # Start from the directory
    ftp.cwd("/anon/gen/clim_data/IDCKWCDEA0/tables/vic/")

    # List directory contents
    surburbs = ftp.nlst()

    subdirectories = []
    # Loop through files
    for suburb in surburbs:
        # print(f"Content of {surburb}:" + "\n")
        subdirectories.append(suburb)

    bulk_data = []
    # Loop through subdirectories
    for subdirectory in subdirectories:
        # Fission function to call multiple threads (not required for bulk API)
        files = ftp.nlst(subdirectory)
        for file in files:
            data = []
            if file.endswith('.csv'):
                # Process the file line by line and add data to bulk_data list
                ftp.retrlines(f"RETR {file}", data.append)
            data = data[13:-1]
            bulk_data = []
            for line in data:
                row = line.split(",")
                document = {
                    "_index": index_name,
                    "Station Name": row[0],
                    "Date": row[1],
                    "Evapo-Rain": row[2],
                    "Rain": row[3],
                    "Pan-Rain": row[4],
                    "Max Temp": row[5],
                    "Min Temp": row[6],
                    "Max Humid": row[7],
                    "Min Humid": row[8],
                    "WindSpeed": row[9],
                    "UV": row[-1],
                }
                bulk_data.append(document)

            result = bulk(es, bulk_data, index=index_name)

    # Close FTP connection
    ftp.quit()
