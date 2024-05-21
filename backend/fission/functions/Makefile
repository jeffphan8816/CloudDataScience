NAMESPACE := default
SECRET_NAME := kafka
# Retrieve the secret values
KAFKA_URL := $(shell kubectl -n $(NAMESPACE) get secret $(SECRET_NAME) -o jsonpath="{.data.URL}" | base64 --decode)
KAFKA_TOPIC := $(shell kubectl -n $(NAMESPACE) get secret $(SECRET_NAME) -o jsonpath="{.data.KAFKA_TOPIC}" | base64 --decode)

deploy:
	fission fn create --name harvest-weather-past-obs --code weather-kafka.py --env python-es --ft 3600
	fission route create --name harvest-weather-past-obs --function harvest-weather-past-obs --method GET --url '/setup/weatherpastobs/{State}'
	fission route create --name weather-past-obs-consumer --code weather-consumer.py --env python-es --ft 300
	fission mqtrigger create --name weather-past-obs-consumer --function weather-past-obs-consumer --mqtype kafka --mqtkind keda --topic ${KAFKA_TOPIC} --maxretries 3 --metadata bootstrapServers=${KAFKA_URL} --metadata consumerGroup=kafka-elasticsearch-group --cooldownperiod=30 --pollinginterval=5
	fission fn create --name epa-to-kaf  --code epa_to_kafka.py --env python-es  --secret kafka --secret epa
	fission fn create --name epakaf-to-es  --code epa_kafka_to_es.py --env python-es --secret es --secret kafka

update:
	fission fn update --name harvest-weather-past-obs --code weather-kafka.py
	fission fn update --name epa-to-kaf  --code epa_to_kafka.py
	fission fn update --name epakaf-to-es  --code epa_kafka_to_es.py

teardown:
	fission fn delete --name harvest-weather-past-obs
	fission httptrigger delete --name harvest-weather-past-obs
	fission fn delete --name weather-past-obs-consumer
	fission mqtrigger delete --name weather-past-obs-consumer
	fission fn delete --name epa-to-kaf 
	fission fn delete --name epakaf-to-es
