NAMESPACE := default
SECRET_NAME := kafka
# Retrieve the secret values
KAFKA_URL := $(shell kubectl -n $(NAMESPACE) get secret $(SECRET_NAME) -o jsonpath="{.data.URL}" | base64 --decode)
KAFKA_TOPIC := $(shell kubectl -n $(NAMESPACE) get secret $(SECRET_NAME) -o jsonpath="{.data.KAFKA_TOPIC}" | base64 --decode)
# KAFKA_AIRQUAL_CONF_TOPIC := $(shell kubectl -n $(NAMESPACE) get secret $(SECRET_NAME) -o jsonpath="{.data.KAFKA_AIRQUAL_CONF_TOPIC}" | base64 --decode)

deploy:
	fission fn create --name harvest-weather-past-obs --code weather_kafka.py --env python-es --ft 3600
	fission function create --name weather-past-obs-consumer --code weather_consumer.py --env python-es --ft 300
	fission mqtrigger create --name weather-past-obs-consumer --function weather-past-obs-consumer --mqtype kafka --mqtkind keda --topic ${KAFKA_TOPIC} --maxretries 3 --metadata bootstrapServers=${KAFKA_URL} --metadata consumerGroup=kafka-elasticsearch-group --cooldownperiod=30 --pollinginterval=5

	fission fn create --name epa-to-kaf  --code epa_to_kafka.py --env python-es  --secret kafka --secret epa
	fission timer create --name daily-midnight --function epa-to-kaf --cron "0 0 * * *"
	fission fn create --name epakaf-to-es  --code epa_kafka_to_es.py --env python-es --secret es --secret kafka
	fission mqtrigger create --name kafka-confirm-epa --function epakaf-to-es --mqtype kafka --mqtkind keda --topic airquality-uploaded-kafka --maxretries 3 --metadata bootstrapServers=${KAFKA_URL} --metadata consumerGroup=kafka-elasticsearch-group --cooldownperiod=30 --pollinginterval=5

	zip -jr hourly-weather-obs.zip hourly-weather-obs
	fission package create --sourcearchive hourly-weather-obs.zip --env python-es --buildcmd "./build.sh" --name hourly-weather-obs
	fission fn create --pkg hourly-weather-obs --name fetch-weather-obs   --entrypoint "fetch-weather-obs.main" --executortype newdeploy --maxscale 3 --secret es
	fission fn create --pkg hourly-weather-obs --name process-weather-obs --entrypoint "process-weather-obs.main" --executortype newdeploy --maxscale 3 --secret es
	fission fn create --pkg hourly-weather-obs --name ingest-weather-obs  --entrypoint "ingest-weather-obs.main" --executortype newdeploy --maxscale 3 --secret es
	fission fn create --pkg hourly-weather-obs --name hourly-weather-obs  --entrypoint "hourly-weather-obs.main" --executortype newdeploy --maxscale 3 --secret es

	fission route create --method GET --url /fetch-weather-obs --function fetch-weather-obs --name fetch-weather-obs
	fission route create --method POST --url /process-weather-obs --function process-weather-obs --name process-weather-obs
	fission route create --method POST --url /ingest-weather-obs --function ingest-weather-obs --name ingest-weather-obs

	fission timer create --name get-hourly-weather --function hourly-weather-obs --cron "15 * * * *"

	zip -jr current-weather.zip current-weather
	fission package create --sourcearchive current-weather.zip --env python-es --buildcmd "./build.sh" --name current-weather
	fission fn create --name current-weather --pkg current-weather --entrypoint "current-weather.main" --executortype newdeploy --maxscale 3 --secret es
	fission route create --method GET --url /current-weather --function current-weather --name current-weather

	zip -jr ai-functions.zip ai-functions
	fission package create --sourcearchive ai-functions.zip --env python-es --buildcmd "./build.sh" --name ai-functions
	fission fn create --name gen-image --pkg ai-functions --entrypoint "gen-image.main" --executortype newdeploy --maxscale 3 --secret es
	fission route create --method POST --url /gen-image --function gen-image --name gen-image
	fission fn create --name gen-text --pkg ai-functions --entrypoint "gen-text.main" --executortype newdeploy --maxscale 3 --secret es
	fission route create --method POST --url /gen-text --function gen-text --name gen-text



# replace airquality-uploaded-kafka with KAFKA_AIRQUAL_CONF_TOPIC

update:
	fission fn update --name harvest-weather-past-obs --code weather_kafka.py
	fission fn update --name epa-to-kaf  --code epa_to_kafka.py
	fission fn update --name epakaf-to-es  --code epa_kafka_to_es.py

	zip -jr hourly-weather-obs.zip hourly-weather-obs
	fission package update --force --sourcearchive hourly-weather-obs.zip --env python-es --buildcmd "./build.sh" --name hourly-weather-obs

	zip -jr current-weather.zip current-weather
	fission package create --sourcearchive current-weather.zip --env python-es --buildcmd "./build.sh" --name current-weather
	fission fn create --name current-weather --pkg current-weather --entrypoint "current-weather.main" --executortype newdeploy --maxscale 3 --secret es
	fission route create --method GET --url /current-weather --function current-weather --name current-weather
	fission package update --force --sourcearchive current-weather.zip --env python-es --buildcmd "./build.sh" --name current-weather
	fission fn update --name current-weather --pkg current-weather --entrypoint "current-weather.main"

teardown:
	fission fn delete --name harvest-weather-past-obs --ignorenotfound=true
	fission fn delete --name weather-past-obs-consumer --ignorenotfound=true
	fission mqtrigger delete --name weather-past-obs-consumer --ignorenotfound=true

	fission fn delete --name epa-to-kaf --ignorenotfound=true
	fission timer delete --name daily-midnight --ignorenotfound=true
	fission fn delete --name epakaf-to-es --ignorenotfound=true
	fission mqtrigger delete --name kafka-confirm-epa --ignorenotfound=true

	fission fn delete --name fetch-weather-obs --ignorenotfound=true
	fission fn delete --name process-weather-obs --ignorenotfound=true
	fission fn delete --name ingest-weather-obs --ignorenotfound=true
	fission fn delete --name hourly-weather-obs --ignorenotfound=true

	fission route delete --name fetch-weather-obs --ignorenotfound=true
	fission route delete --name process-weather-obs --ignorenotfound=true
	fission route delete --name ingest-weather-obs --ignorenotfound=true

	fission timer delete --name get-hourly-weather --ignorenotfound=true

	fission fn delete --name current-weather --ignorenotfound=true
	fission route delete --name current-weather --ignorenotfound=true

	fission package delete --name hourly-weather-obs --ignorenotfound=true
	fission package delete --name current-weather --ignorenotfound=true

	fission fn delete --name gen-image --ignorenotfound=true
	fission route delete --name gen-image --ignorenotfound=true
	fission fn delete --name gen-text --ignorenotfound=true
	fission route delete --name gen-text --ignorenotfound=true
	fission package delete --name ai-functions --ignorenotfound=true
