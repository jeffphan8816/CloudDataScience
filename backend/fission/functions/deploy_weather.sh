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



# Ingest weather obs ------------------------------------
zip -jr ingest-weather-obs.zip ingest-weather-obs
fission package update --sourcearchive ingest-weather-obs.zip --env python-multi --buildcmd "./build.sh" --name ingest-weather-obs
fission fn update --name ingest-weather-obs --pkg ingest-weather-obs --entrypoint "ingest-weather-obs.main"
fission route delete --name ingest-weather-obs
fission route create --method POST --url /ingest-weather-obs --function ingest-weather-obs --name ingest-weather-obs

# fission fn test --name ingest-weather-obs
fission fn logs --name ingest-weather-obs

curl -X POST http://172.26.135.52:9090/ingest-weather-obs \
     -H "Content-Type: application/json" \
     -H "Host: fission" \
     -d '{"data": "sample"}'
fission fn logs --name ingest-weather-obs

# Fetch weather obs ------------------------------------

zip -jr fetch-weather-obs.zip fetch-weather-obs
fission package update --sourcearchive fetch-weather-obs.zip --env python-multi --buildcmd "./build.sh" --name fetch-weather-obs
fission fn update --name fetch-weather-obs --pkg fetch-weather-obs --entrypoint "fetch-weather-obs.main"

fission fn test --name fetch-weather-obs
fission fn logs --name fetch-weather-obs

fission timer create --name get-hourly-weather --function fetch-weather-obs --cron "15 * * * *"



# fission fn test --name fetch-weather-obs
# fission fn logs --name ingest-weather-obs

# from ypp ------------------------------------
zip -jr multiweather.zip multiweather
fission package create --sourcearchive multiweather.zip --env python-multi --buildcmd "./build.sh" --name multiweather
fission fn create --name multiweather --pkg multiweather --entrypoint "multiweather.main"
fission fn test --name multiweather
