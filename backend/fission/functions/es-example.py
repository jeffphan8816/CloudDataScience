from elasticsearch import Elasticsearch
import warnings
warnings.filterwarnings("ignore")

url = 'https://elasticsearch:9200'
user = "elastic"
password = "cloudcomp"

es = Elasticsearch([url], basic_auth=(user, password), verify_certs=False)

def main():
    print(es.ping())
