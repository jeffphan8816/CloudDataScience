deploy:
	fission fn create --name weather-station-api --code weather_from_station.py --env python-es --executortype newdeploy --maxscale 3 --secret es
	fission route create --name weather-station-rest --function weather-station-api --method GET --url '/weather/{Station}/{Start}/{End}'

	fission fn create --name crash-api --code crash_api_connect.py --env python-es --executortype newdeploy --maxscale 3 --secret es
	fission route create --name crash-rest --function crash-api --method GET --url '/crashes/{Station}/{Size}/{Radius}'

	fission fn create --name epa-api --code epa_api_connect.py --env python-es --executortype newdeploy --maxscale 3 --secret es
	fission route create --name epa-rest --function epa-api --method GET --url '/epa/{Station}/{Size}/{Radius}'

	fission fn create --name crime-api --code crime_api_connect.py --env python-es --executortype newdeploy --maxscale 3 --secret es
	fission route create --name crime-rest --function crime-api --method GET --url '/crime/{Station}/{Size}/{Radius}'

	fission fn create --name stream-api --code stream_api.py --env python-es --executortype newdeploy --maxscale 3 --secret es
	fission route create --name stream-rest --function stream-api --method GET --url '/stream/{Token}'

	fission fn create --name station-api --code station_api.py --env python-es --executortype newdeploy --maxscale 3 --secret es
	fission route create --name station-rest --function station-api --method GET --url '/stations'

	fission fn create --name station-coord-api --code coord_station_api.py --env python-es --executortype newdeploy --maxscale 3 --secret es
	fission route create --name station-coord-rest --function station-coord-api --method GET --url '/stations/{Lon}/{Lat}'

	zip -jr models_api.zip models_api
	fission --name models-api-pack package create --sourcearchive models_api.zip --env python-es --buildcmd "./build.sh"
	fission fn create --name models-api --env python-es --pkg models-api-pack --entrypoint "models_api.main" --executortype newdeploy --maxscale 3 --ft 120
	fission route create --name models-rest --function models-api --method GET --url '/models/{ModelName}'

update:
	fission fn update --name weather-station-api --code weather_from_station.py
	fission fn update --name crash-api --code crash_api_connect.py
	fission fn update --name epa-api --code epa_api_connect.py
	fission fn update --name crime-api --code crime_api_connect.py
	fission fn update --name stream-api --code stream_api.py
	fission fn update --name station-api --code station_api.py
	fission fn update --name station-coord-api --code coord_station_api.py

	fission fn delete --name models-api --ignorenotfound=true
	fission package delete --name models-api-pack
	fission httptrigger delete --name models-rest
	zip -jr models_api.zip models_api
	fission --name models-api-pack package create --sourcearchive models_api.zip --env python-es --buildcmd "./build.sh"
	fission fn create --name models-api --env python-es --pkg models-api-pack --entrypoint "models_api.main" --executortype newdeploy --maxscale 3 --ft 120
	fission route create --name models-rest --function models-api --method GET --url '/models/{ModelName}'


teardown:
	fission fn delete --name weather-station-api --ignorenotfound=true
	fission httptrigger delete --name weather-station-rest --ignorenotfound=true

	fission fn delete --name crash-api --ignorenotfound=true
	fission httptrigger delete --name crash-rest --ignorenotfound=true

	fission fn delete --name epa-api --ignorenotfound=true
	fission httptrigger delete --name epa-rest --ignorenotfound=true

	fission fn delete --name crime-api --ignorenotfound=true
	fission httptrigger delete --name crime-rest --ignorenotfound=true

	fission fn delete --name stream-api --ignorenotfound=true
	fission httptrigger delete --name stream-rest --ignorenotfound=true

	fission fn delete --name station-api --ignorenotfound=true
	fission httptrigger delete --name station-rest --ignorenotfound=true


	fission fn delete --name station-coord-api --ignorenotfound=true
	fission httptrigger delete --name station-coord-rest --ignorenotfound=true

	fission fn delete --name models-api --ignorenotfound=true
	fission package delete --name models-api-pack --ignorenotfound=true
	fission httptrigger delete --name models-rest --ignorenotfound=true
