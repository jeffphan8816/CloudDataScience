# Fission functions

This folder contains code for fission packages and functions


## Files and Directories

- **fetch-weather-obs/**: Fetch weather observations from BOM json endpoints.
- **hourly-weather-obs/**: Batch process to start the ingest hourly weather observations.
- **ingest-weather-obs/**: Ingest weather observation data into ES.
- **process-weather-obs/**: Process and clean weather data before ingesting into ES.
- **deploy_weather.sh**: For deploying the weather functions.
- **epa_collect.py**: Collecting EPA data.
- **lastweather-obs.py**: Retrieving the last weather observations.
- **weather-consumer.py**: Consuming weather data.
- **weather-kafka.py**: Interfacing with Kafka for weather data.
- **weather.py**: A general-purpose weather data handling.
