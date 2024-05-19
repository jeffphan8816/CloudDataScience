cd ~/cloudcomp-aw/backend/fission/functions/

# Fetch weather obs ------------------------------------
# fission package create --sourcearchive fetch-weather-obs.zip --env python-es --buildcmd "./build.sh" --name fetch-weather-obs
# fission fn create --name fetch-weather-obs --pkg fetch-weather-obs --entrypoint "fetch-weather-obs.main"

zip -jr fetch-weather-obs.zip fetch-weather-obs
fission package update --sourcearchive fetch-weather-obs.zip --env python-es --buildcmd "./build.sh" --name fetch-weather-obs
fission fn update --name fetch-weather-obs --pkg fetch-weather-obs --entrypoint "fetch-weather-obs.main"

fission fn test --name fetch-weather-obs
fission fn logs --name fetch-weather-obs

fission route delete --name fetch-weather-obs
fission route create --method GET --url /fetch-weather-obs --function fetch-weather-obs --name fetch-weather-obs


# Process weather obs ------------------------------------
# fission package create --sourcearchive process-weather-obs.zip --env python-es --buildcmd "./build.sh" --name process-weather-obs
# fission fn create --name process-weather-obs --pkg process-weather-obs --entrypoint "process-weather-obs.main"

zip -jr process-weather-obs.zip process-weather-obs
fission package update --sourcearchive process-weather-obs.zip --env python-es --buildcmd "./build.sh" --name process-weather-obs
fission fn update --name process-weather-obs --pkg process-weather-obs --entrypoint "process-weather-obs.main"

fission route delete --name process-weather-obs
fission route create --method POST --url /process-weather-obs --function process-weather-obs --name process-weather-obs

fission fn test --name process-weather-obs

# Ingest weather obs ------------------------------------
# fission package create --sourcearchive ingest-weather-obs.zip --env python-es --buildcmd "./build.sh" --name ingest-weather-obs
# fission fn create --name ingest-weather-obs --pkg ingest-weather-obs --entrypoint "ingest-weather-obs.main"

zip -jr ingest-weather-obs.zip ingest-weather-obs
fission package update --sourcearchive ingest-weather-obs.zip --env python-es --buildcmd "./build.sh" --name ingest-weather-obs
fission fn update --name ingest-weather-obs --pkg ingest-weather-obs --entrypoint "ingest-weather-obs.main"

fission route delete --name ingest-weather-obs
fission route create --method POST --url /ingest-weather-obs --function ingest-weather-obs --name ingest-weather-obs

# Hourly weather obs ------------------------------------

# fission package create --sourcearchive hourly-weather-obs.zip --env python-es --buildcmd "./build.sh" --name hourly-weather-obs
# fission fn create --name hourly-weather-obs --pkg hourly-weather-obs --entrypoint "hourly-weather-obs.main"

zip -jr hourly-weather-obs.zip hourly-weather-obs
fission package update --sourcearchive hourly-weather-obs.zip --env python-es --buildcmd "./build.sh" --name hourly-weather-obs
fission fn update --name hourly-weather-obs --pkg hourly-weather-obs --entrypoint "hourly-weather-obs.main"

fission fn test --name hourly-weather-obs
fission fn logs --name hourly-weather-obs



# Todo: below

fission timer create --name get-hourly-weather --function hourly-weather-obs --cron "15 * * * *"
fission timer delete --name get-hourly-weather




# Testing ------------------------------------

# Default
curl -X GET http://172.26.135.52:9090/fetch-weather-obs -H "Host: fission"
fission fn logs --name fetch-weather-obs

# These should pass
curl -X GET "http://172.26.135.52:9090/fetch-weather-obs?state=VIC&region=MALLEE" -H "Host: fission"
fission fn logs --name fetch-weather-obs

curl -X GET "http://172.26.135.52:9090/fetch-weather-obs?state=WA&region=KIMBERLEY" -H "Host: fission"
fission fn logs --name fetch-weather-obs

# Check with long region name with spaces
curl -X GET 'http://172.26.135.52:9090/fetch-weather-obs?state=WA&region=CENTRAL%20WEST' -H "Host: fission"
fission fn logs --name fetch-weather-obs

# These should fail
curl -X GET "http://172.26.135.52:9090/fetch-weather-obs?state=NSW&region=MALLEE" -H "Host: fission"
curl -X GET "http://172.26.135.52:9090/fetch-weather-obs?state=NSW" -H "Host: fission"




# Ingest
fission fn test --name ingest-weather-obs
fission fn logs --name ingest-weather-obs

curl -X POST http://172.26.135.52:9090/ingest-weather-obs \
     -H "Content-Type: application/json" \
     -H "Host: fission" \
     -d '{"data": "sample"}'
fission fn logs --name ingest-weather-obs


# Test sequence ------------------------------------
curl -X GET "http://172.26.135.52:9090/fetch-weather-obs?state=WA&region=ISLANDS" -H "Host: fission"
fission fn logs --name fetch-weather-obs
fission fn logs --name process-weather-obs
fission fn logs --name ingest-weather-obs


# fission fn test --name fetch-weather-obs
# fission fn logs --name ingest-weather-obs

