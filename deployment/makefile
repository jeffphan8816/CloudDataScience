TIMEOUT := 1200s
DATADOG_VERSION := 3.61.0
ES_VERSION := 21.0.1
FISSION_VERSION := 1.20.1
INGRESS_CONTROLLER_VERSION := 4.10.0

datadog:
	helm upgrade --wait --timeout=$(TIMEOUT) --version=${DATADOG_VERSION} --namespace datadog --install --values datadog/values.yaml datadog-agent datadog/datadog

elasticsearch:
	helm upgrade --wait --timeout=$(TIMEOUT) --version=${ES_VERSION} --namespace elastic --render-subchart-notes --install --values elasticsearch/values.yaml elasticsearch bitnami/elasticsearch

fission-all:
	helm upgrade --wait --timeout=$(TIMEOUT) --version v${FISSION_VERSION} --namespace fission --install --values fission-all/values.yaml fission-all fission-charts/fission-all

fission-env:
	fission spec apply --wait

ingress-controller:
	helm upgrade --wait --timeout=$(TIMEOUT) --version v${INGRESS_CONTROLLER_VERSION} --namespace ingress-nginx --install --values ingress/values.yaml ingress-nginx ingress-nginx/ingress-nginx \
	&& kubectl -n ingress-nginx patch deployment/ingress-nginx-controller --patch-file ingress/patch.yaml

ingress-elasticsearch:
	kubectl apply -f ingress/ingress-elasticsearch.yaml --namspace elastic

ingress-kibana:
	kubectl apply -f ingress/ingress-kibana.yaml --namspace elastic

ingress-fission:
	kubectl apply -f ingress/ingress-fission.yaml --namspace fission

.PHONY: datadog elasticsearch fission-all fission-env ingress-controller ingress-elasticsearch ingress-kibana ingress-fission
