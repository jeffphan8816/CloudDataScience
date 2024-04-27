import requests
import json
from datetime import datetime


URL = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites/parameters?environmentalSegment=air'

def fetch_epa() -> list[dict]:
    headers = {
        'Cache-Control': 'no-cache',
        'X-API-Key': '96ff8ef9e03048e2bd2fa342a5d80587',
        'User-agent': 'CloudCluster'
    }
    resp = requests.get(URL, headers=headers)
    data = json.loads(resp.text)
    out = []
    if 'records' not in data.keys():
        return out
    for record in data['records']:
        location = record['geometry']['coordinates']
        if 'parameters' not in record.keys():
            continue
        parameters = record['parameters']
        for parameter in parameters:
            if 'name' not in parameter.keys():
                continue
            name = parameter['name']
            if 'timeSeriesReadings' not in parameter.keys():
                continue
            for series in parameter['timeSeriesReadings']:
                if 'readings' not in series.keys():
                    continue
                for reading in series['readings']:
                    cont = True
                    for key in ['since', 'until', 'averageValue']:
                        if key not in reading.keys():
                             cont = False
                    if not cont:
                        continue
                    toAdd = {}
                    toAdd['name'] = name
                    toAdd['location'] = location
                    toAdd['start'] = datetime.strptime(
                        reading['since'], '%Y-%m-%dT%H:%M:%SZ')
                    toAdd['end'] = datetime.strptime(
                        reading['since'], '%Y-%m-%dT%H:%M:%SZ')
                    toAdd['value'] = reading['averageValue']
                    out.append(toAdd)
    return out


print(fetch_epa())
