# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

curl -XPUT -k 'https://elasticsearch:31001/locations'\
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
            "location": {
                "type": "geo_point"
            },
            "suburb": {
                "type": "text"
            },
            "state": {
                "type": "keyword"
            }
        }
    }
}'\
   --user 'elastic:cloudcomp' | jq '.'

