deploy:
	fission fn create --name weather-station-api --code weather_from_station.py --env python-es --executortype newdeploy --maxscale 3
	fission route create --name weather-station-rest --function weather-station-api --method GET --url '/weather/{Station}/{Start}/{End}'

	fission fn create --name crash-api --code crash_api_connect.py --env python-es --executortype newdeploy --maxscale 3
	fission route create --name crash-rest --function crash-api --method GET --url '/crashes/{Station}/{Size}/{Radius}'

	fission fn create --name epa-api --code epa_api_connect.py --env python-es --executortype newdeploy --maxscale 3
	fission route create --name epa-rest --function epa-api --method GET --url '/epa/{Station}/{Size}/{Radius}'

	fission fn create --name crime-api --code crime_api_connect.py --env python-es --executortype newdeploy --maxscale 3
	fission route create --name crime-rest --function crime-api --method GET --url '/crime/{Station}/{Size}/{Radius}'

	fission fn create --name stream-api --code stream_api.py --env python-es --executortype newdeploy --maxscale 3
	fission route create --name stream-rest --function stream-api --method GET --url '/stream/{Token}'

	fission fn create --name station-api --code station_api.py --env python-es --executortype newdeploy --maxscale 3
	fission route create --name station-rest --function station-api --method GET --url '/stations'

update:
	fission fn update --name weather-station-api --code weather_from_station.py
	fission fn update --name crash-api --code crash_api_connect.py
	fission fn update --name epa-api --code epa_api_connect.py
	fission fn update --name crime-api --code crime_api_connect.py
	fission fn update --name stream-api --code stream_api.py
	fission fn update --name station-api --code station_api.py

teardown:
	fission fn delete --name weather-station-api
	fission httptrigger delete --name weather-station-rest

	fission fn delete --name crash-api
	fission httptrigger delete --name crash-rest

	fission fn delete --name epa-api
	fission httptrigger delete --name epa-rest

	fission fn delete --name crime-api
	fission httptrigger delete --name crime-rest

	fission fn delete --name stream-api
	fission httptrigger delete --name stream-rest

	fission fn delete --name station-api
	fission httptrigger delete --name station-rest
