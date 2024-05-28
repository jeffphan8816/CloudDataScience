# Team 69
# Dillon Blake 1524907
# Andrea Delahaye 1424289
# Yue Peng 958289
# Jeff Phan 1577799
# Alistair Wilcox 212544

deploy:
	zip -jr content.zip content
	fission --name doc-api-pack package create --sourcearchive content.zip --env python-es --buildcmd "./build.sh"
	fission fn create --name doc-api --env python-es --pkg doc-api-pack --entrypoint "doc_api.main" --executortype newdeploy --maxscale 3
	fission route create --name doc-rest --function doc-api --method GET --url '/docs/{File}'

teardown:
	fission fn delete --name doc-api
	fission package delete --name doc-api-pack
	fission httptrigger delete --name doc-rest