#!/bin/sh

# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

curl -XPUT -k 'https://elasticsearch:31001/crimes'\
   --header 'Content-Type: application/json'\
   --data '{
    "settings": {
        "index": {
            "number_of_shards": 5,
            "number_of_replicas": 1
        }
    },
    "mappings": {
        "properties": {
            "reported_date": {
                "type": "date"
            },
            "suburb": {
                "type": "keyword"
            },            
            "postcode": {
                "type": "keyword"
            },
            "description_1": {
                "type": "keyword"
            },
            "description_2": {
                "type": "keyword"
            },
            "description_3": {
                "type": "text"
            }
        }
    }
}'\
   --user 'elastic:cloudcomp' | jq '.'

