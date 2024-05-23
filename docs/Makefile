deploy:
	zip -jr content.zip content
	fission --kube-context ~/.kube/config --name doc-api-pack package create --sourcearchive content.zip --env python-es --buildcmd "./build.sh"
	fission --kube-context ~/.kube/config fn create --name doc-api --env python-es --pkg doc-api-pack --entrypoint "doc_api.main" --executortype newdeploy --maxscale 3
	fission --kube-context ~/.kube/config route create --name doc-rest --function doc-api --method GET --url '/docs/{File}'

teardown:
	fission --kube-context ~/.kube/config fn delete --name doc-api
	fission --kube-context ~/.kube/config package delete --name doc-api-pack
	fission --kube-context ~/.kube/config httptrigger delete --name doc-rest