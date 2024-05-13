fission fn create --name weather-station-api --code weather_from_station.py --env python-es
fission route create --name weather-station-rest --function weather-station-api --method GET --url '/weather/{Station}/{Start}/{End} '

fission fn create --name crash-api --code crash_api_connect.py --env python-es
fission route create --name crash-rest --function crash-api --method GET --url '/crashes/{Station}/{Size}/{Radius}'

fission fn create --name stream-api --code stream_api.py --env python-es
fission route create --name stream-rest --function stream-api --method GET --url '/stream/{Token}'