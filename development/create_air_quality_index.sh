curl -XPUT -k 'https://0.0.0/airquality'\
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
            "name": {
                "type": "text"
            },
            "location": {
                "type": "geo_point"
            },
            "start": {
                "type": "date"
                ,
            "end": {
                "type": "date"
                ,
            "value": {
                "type": "float"
            }
        }
    }
}'\
   --user 'elastic:elastic' | jq '.'
