deploy:
	fission fn create --name epa-to-kaf  --code epa_to_kafka.py --env python-es  --secret kafka --secret epa
	fission fn create --name epakaf-to-es  --code epa_kafka_to_es.py --env python-es --secret es --secret kafka

update:
	fission fn update --name epa-to-kaf  --code epa_to_kafka.py
	fission fn update --name epakaf-to-es  --code epa_kafka_to_es.py

teardown:
	fission fn delete --name epa-to-kaf 
	fission fn delete --name epakaf-to-es