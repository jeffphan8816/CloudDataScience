curl -XPUT -k 'https://elasticsearch:31001/airquality'\
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
            "measure_name": {
                "type": "keyword"
            },
            "location": {
                "type": "geo_point"
            },
            "start": {
                "type": "date"
        	},
            "end": {
                "type": "date"
		},
            "value": {
                "type": "float"
            }
        }
    }
}'\
   --user 'elastic:cloudcomp' | jq '.'