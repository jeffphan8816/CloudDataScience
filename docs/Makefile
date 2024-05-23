deploy:
	zip -jr content.zip content
	fission --kube-context ~/.kube/config --name doc-api-pack package create --sourcearchive content.zip --env python-es --buildcmd "./build.sh"
	fission fn create --kube-context ~/.kube/config --name doc-api --env python-es --pkg doc-api-pack --entrypoint "doc_api.main" --executortype newdeploy --maxscale 3
	fission route create --kube-context ~/.kube/config --name doc-rest --function doc-api --method GET --url '/docs/{File}'

teardown:
	fission fn delete --kube-context ~/.kube/config --name doc-api
	fission package delete --kube-context ~/.kube/config --name doc-api-pack
	fission httptrigger delete --kube-context ~/.kube/config --name doc-rest