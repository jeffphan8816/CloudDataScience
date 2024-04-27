TIMEOUT := 1200s
ES_VERSION := 21.0.1
FISSION_VERSION := 1.20.1
INGRESS_CONTROLLER_VERSION := 4.10.0

elasticsearch:
	helm upgrade --wait --timeout=$(TIMEOUT) --version=${ES_VERSION} --namespace elastic --render-subchart-notes --install --values elasticsearch/values.yaml elasticsearch bitnami/elasticsearch

fission-all:
	helm upgrade --wait --timeout=$(TIMEOUT) --version v${FISSION_VERSION} --namespace fission --install --values fission-all/values.yaml fission-all fission-charts/fission-all

ingress-controller:
	helm upgrade --wait --timeout=$(TIMEOUT) --version v${INGRESS_CONTROLLER_VERSION} --namespace ingress-nginx --install --values ingress/values.yaml ingress-nginx ingress-nginx/ingress-nginx \
	&& kubectl -n ingress-nginx patch deployment/ingress-nginx-controller --patch-file ingress/patch.yaml

ingress-elasticsearch:
	kubectl apply -f ingress/ingress-elasticsearch.yaml --namspace elastic

ingress-kibana:
	kubectl apply -f ingress/ingress-kibana.yaml --namspace elastic

ingress-fission:
	kubectl apply -f ingress/ingress-fission.yaml --namspace fission

.PHONY: elasticsearch fission-all
