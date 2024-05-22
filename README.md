# Group 69

# cloudcomp

## elasticsearch access
1. If you are inside kubernetes cluster
```
curl "https://elasticsearch.elastic.svc.cluster.local:9200" -k -u "elastic:you_know_pass"
```
2. If you are inside bastion
```
curl "https://elasticsearch:31001" -k -u "elastic:you_know_pass"
```
3. If you are on your own computer and connected to vpn
```
curl "https://172.26.135.52:9200" -k -u "elastic:you_know_pass" -H "HOST:elasticsearch"
```

## API endpoints (from the bastion)

Get list of stations:
http://fission:31000/stations

Weather data:
http://fission:31000/weather/STATION_ID/START_YEAR/END_YEAR 

Crash data:
http://fission:31000/crashes/STATION_ID/SIZE/RADIUS_KM

Crime data:
http://fission:31000/crime/STATION_ID/SIZE/RADIUS_KM

Airquality data:
http://fission:31000/epa/STATION_ID/SIZE/RADIUS_KM

Stream api to continue pagination scroll (used for crime, epa, and crash data)
http://fission:31000/stream/TOKEN
