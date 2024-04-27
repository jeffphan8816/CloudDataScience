TIMEOUT := 1200s
ES_VERSION := 21.0.1
FISSION_VERSION := 1.20.1

elasticsearch:
	helm upgrade --wait --timeout=$(TIMEOUT) --version=${ES_VERSION} --namespace elastic --render-subchart-notes --install --values elasticsearch/values.yaml elasticsearch bitnami/elasticsearch

fission-all:
	helm upgrade --wait --timeout=$(TIMEOUT) --version v${FISSION_VERSION} --namespace fission --install --values fission-all/values.yaml fission-all fission-charts/fission-all

.PHONY: elasticsearch kibana fission-all
