cd ~/cloudcomp-aw/backend/fission/functions/

fission function delete --name multiweather
rm multiweather.zip
zip -jr multiweather.zip *.json multiweather.py
fission function create --name multiweather --env python-es --code multiweather.zip --entrypoint multiweather.main
fission package info --name multiweather-6bb3ed2b-f174-48f0-8e6a-70a9709e79ea

fission function test --name multiweather -v=2


fission package info --name multifile-6e3da105-97d7-4ffe-9531-341bb3c18de6 -v=2

fission package getdeploy --name multiweather-6bb3ed2b-f174-48f0-8e6a-70a9709e79ea
kubectl logs --selector='envName=python-es' -c builder

# Other tesing with hello.py below

# # fission function get --name multiweather
# fission function delete --name multiweather
# fission function update --name multiweather --code multiweather.zip

# zip -jr multiweather.zip hello.py
# fission function create --name multiweather --env python-es --code multiweather.zip --entrypoint hello.main


# fission fn log -f --name multiweather -v=2 -n kube-system .

# fission function test --name multiweather -v=2
# fission package info --name multiweather -v=2

# fission function delete --name multiweather
# fission package delete --name multiweather

# fission package create --sourcearchive multiweather.zip\
#   --env python-es\
#   --name multiweather\
#   --buildcmd './build.sh'

# fission fn create --name multiweather\
#   --pkg multiweather\
#   --env python-es\
#   --entrypoint "hello.main"

zip -jr multiweather.zip multiweather
fission package update --sourcearchive multiweather.zip --env python-multi --buildcmd "./build.sh" --name multiweather
fission fn update --name multiweather --pkg multiweather --entrypoint "multiweather.main"
fission fn test --name multiweather
fission fn logs --name multiweather


# from ypp ------------------------------------
zip -jr multiweather.zip multiweather
fission package create --sourcearchive multiweather.zip --env python-multi --buildcmd "./build.sh" --name multiweather
fission fn create --name multiweather --pkg multiweather --entrypoint "multiweather.main"
fission fn test --name multiweather
