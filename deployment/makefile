TIMEOUT := 1200s
DATADOG_VERSION := 3.61.0
ES_VERSION := 21.0.1
KEDA_VERSION := 2.14.2
KAFKA_VERSION := 0.40.0
KAFKA_UI_VERSION := 1.4.0
FISSION_VERSION := 1.20.1
INGRESS_CONTROLLER_VERSION := 4.10.0

datadog:
	helm upgrade --wait --timeout=$(TIMEOUT) --version ${DATADOG_VERSION} --namespace datadog --install --values datadog/values.yaml datadog-agent datadog/datadog

elasticsearch:
	helm upgrade --wait --timeout=$(TIMEOUT) --version ${ES_VERSION} --namespace elastic --render-subchart-notes --install --values elasticsearch/values.yaml elasticsearch bitnami/elasticsearch

keda:
	helm upgrade --wait --timeout=$(TIMEOUT) --version ${KEDA_VERSION} --namespace keda --install --values keda/values.yaml keda kedacore/keda

kafka:
	helm upgrade --wait --timeout=$(TIMEOUT) --version ${KAFKA_VERSION} --namespace kafka --install --values kafka/values.yaml kafka strimzi/strimzi-kafka-operator \
	&& kubectl apply -f kafka/kafka-cluster.yaml --namespace kafka

kafka-ui:
	helm upgrade --wait --timeout=$(TIMEOUT) --version ${KAFKA_UI_VERSION} --namespace kafka --install --values kafka-ui/values.yaml kafka-ui kafka-ui/kafka-ui

fission-all:
	helm upgrade --wait --timeout=$(TIMEOUT) --version v${FISSION_VERSION} --namespace fission --install --values fission-all/values.yaml fission-all fission-charts/fission-all

fission-env:
	fission spec apply --wait

ingress-controller:
	helm upgrade --wait --timeout=$(TIMEOUT) --version v${INGRESS_CONTROLLER_VERSION} --namespace ingress-nginx --install --values ingress/values.yaml ingress-nginx ingress-nginx/ingress-nginx \
	&& kubectl -n ingress-nginx patch deployment/ingress-nginx-controller --patch-file ingress/patch.yaml

ingress-elasticsearch:
	kubectl apply -f ingress/ingress-elasticsearch.yaml --namespace elastic

ingress-kibana:
	kubectl apply -f ingress/ingress-kibana.yaml --namespace elastic

ingress-kafka-ui:
	kubectl apply -f ingress/ingress-kafka-ui.yaml --namespace kafka

ingress-fission:
	kubectl apply -f ingress/ingress-fission.yaml --namespace fission

ingress-kafka:
	kubectl apply -f ingress/ingress-kafka.yaml --namespace kafka

.PHONY: datadog elasticsearch keda kafka kafka-ui fission-all fission-env ingress-controller ingress-elasticsearch ingress-kibana ingress-fission ingress-kafka-ui
