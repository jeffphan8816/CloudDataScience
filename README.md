# cloudcomp

## elasticsearch access
1. If you are inside kubernetes cluster
```
curl "https://elasticsearch.elastic.svc.cluster.local" -k -u "elastic:you_know_pass"
```
2. If you are inside bastion
```
curl "https://elasticsearch:31001" -k -u "elastic:you_know_pass"
```
3. If you are on your own computer and connected to vpn
```
curl "https://172.26.135.52:9200" -k -u "elastic:you_know_pass"
```