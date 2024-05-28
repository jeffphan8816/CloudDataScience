# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

curl -XPUT -k 'https://elasticsearch:31001/crashes'\
   --header 'Content-Type: application/json'\
   --data '{
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 1
        }
    },
    "mappings": {
        "properties": {
            "crash_date": {
                "type": "date"
            },
            "location": {
                "type": "geo_point"
            },
            "light_condition": {
                "type": "keyword"
            },
            "severity": {
                "type": "byte"
            }
        }
    }
}'\
   --user 'elastic:cloudcomp' | jq '.'

