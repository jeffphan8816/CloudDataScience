import json
from elasticsearch import Elasticsearch
import datetime

from typing import List

ELASTIC_URL = 'https://172.26.135.52:9200'
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = 'cloudcomp'
ES_HEADERS = {'HOST': 'elasticsearch'}
BAD_PARAMS = json.dumps({'Status': 400, 'Message': 'Invalid Parameters'})
ERROR = json.dumps({'Status': 500, 'Message': 'Internal Server Error'})
EMPTY = json.dumps({'Status': 200, 'Data': []})
SCROLL = '5m'
es = Elasticsearch([ELASTIC_URL], basic_auth=(
    ELASTIC_USER, ELASTIC_PASSWORD), verify_certs=False, headers=ES_HEADERS, timeout=60)

def tempMSearch(
        max_hits: int,
        stations: List[str],
        index: str,
        radius: float
    ):
    msearch_body = []

    for station_id in stations:
        msearch_body.append({"index": "station_locations"})
        msearch_body.append({
            "query": {
                "match": {
                    "Station ID": station_id
                }
            }
        })
    # Perform msearch to retrieve station locations
    station_locations = {}
    res = es.msearch(body=msearch_body, max_concurrent_searches = 5)
    for idx, hit in enumerate(res['responses']):
        if hit['hits']['total']['value'] > 0:
            station = hit['hits']['hits'][0]['_source']
            station_locations[stations[idx]] = {
                "lat": station['location'][0],
                "lon": station['location'][1]
            }
        else:
            station_locations[stations[idx]] = None
            
    # Search for crashes near stations
    crashes_near_stations = {}
    msearch_body = []
    for station_id, location in station_locations.items():
        if location is not None:
            msearch_body.append({"index": "crashes"})
            query = {
                "query": {
                    "bool": {
                        "filter": {
                            "geo_distance": {
                                "distance": radius,
                                "location": {
                                    "lat": location['lat'],
                                    "lon": location['lon']
                                }
                            }
                        }
                    }
                }
                # 'query': {
                #     'bool': {
                #         'must': [
                #             {'match_all': {}}
                #         ],
                #         'filter': {
                #             'geo_distance': {
                #                 'distance': str(radius) + 'km',
                #                 'location': {
                #                     'lon': location['lat'],
                #                     'lat': location['lon']
                #                 }
                #             }
                #         }
                #     },
                # }
            }
            msearch_body.append(query)

    # Perform msearch to search for crashes near stations
    res = es.msearch(body=msearch_body)
    idx = 0
    for station_id, location in station_locations.items():
        if location is not None:
            print(res['responses'][idx])
            crashes_near_stations[station_id] = res['responses'][idx]['hits']['hits']
            idx += 1
        else:
            crashes_near_stations[station_id] = []

    return crashes_near_stations


def tempFissionCrashesFromStation(station, radius, size):
    
    # First get coordinates for station
    try:
        station_results = es.search(index='station_locations', body={
            'query': {
                'match': {
                    'Station ID': station
                }
            }
        })
        station_result_list = [station_results['hits']['hits'][i]['_source']
                               for i in range(len(station_results['hits']['hits']))]
        if len(station_result_list) == 0:
            return EMPTY
        lon = station_result_list[0]['location'][0]
        lat = station_result_list[0]['location'][1]
    except:
        return ERROR

    # Then get crashes in the radius
    try:
        results = es.search(index='crashes', body={
            'size': size,
            'query': {
                'bool': {
                    'must': [
                        {'match_all': {}}
                    ],
                    'filter': {
                        'geo_distance': {
                            'distance': str(radius) + 'km',
                            'location': {
                                'lon': lon,
                                'lat': lat
                            }
                        }
                    }
                },
            },
        }, scroll=SCROLL)

        out = {}
        out['Status'] = 200
        out['Token'] = results['_scroll_id']
        out['Data'] = results['hits']['hits']
        if len(out['Data']) == 0:
            out['Token'] = 'END'
        return json.dumps(out)
    except:
        return ERROR


def tempFissionStream(token):
    # Try to get next batch
    try:
        results = es.scroll(scroll_id=token, scroll=SCROLL)
        out = {}
        out['Status'] = 200
        out['Token'] = results['_scroll_id']
        out['Data'] = results['hits']['hits']
        if len(results['hits']['hits']) <= 0:
            es.clear_scroll(scroll_id=out['Token'])
            out['Token'] = 'END'
        return json.dumps(out)
    except:
        return ERROR
    
def tempFissionWeather(station, start, end):
    start_date = datetime.datetime(start, 1,  1)
    end_date = datetime.datetime(end, 1, 1)

    # First get station lat and long
    try:
        station_results = es.search(index='station_locations', body={
            'size': 1,
            'query': {
                'match': {
                    'Station ID': station
                }
            }
        })

        if len(station_results) == 0:
            return EMPTY
        station_results_list = [station_results['hits']['hits'][i]['_source']
                                for i in range(len(station_results['hits']['hits']))]
        if len(station_results_list) == 0:
            return EMPTY

        station_name = station_results_list[0]['Station Name']
    except:
        return ERROR

    # Then get weather results
    try:
        weather_results = es.search(index='weather_past_obs_kafka', body={
            'size': 10000,
            'query': {
                'bool': {
                    'must': [
                        {
                            'match': {
                                'Station Name': station_name
                            }
                        }
                    ],
                    'filter': {
                        'range': {
                            'Date': {
                                'gte': start_date.strftime('%d/%m/%Y'),
                                'lte': end_date.strftime('%d/%m/%Y')
                            }
                        }
                    }
                }
            }
        })

        out = {}
        out['Status'] = 200
        out['Data'] = [weather_results['hits']['hits'][i]['_source']
                       for i in range(len(weather_results['hits']['hits']))]
        return json.dumps(out)
    except:
        return ERROR


def tempFissionCrashes():
    # Then get crashes in the radius
    size = 10000
    try:
        results = es.search(index='crashes', body={
            'size': size,
            'query': {
                'match_all': {}
            }
        }, scroll=SCROLL)

        out = {}
        out['Status'] = 200
        out['Token'] = results['_scroll_id']
        out['Data'] = results['hits']['hits']
        if len(out['Data']) == 0:
            out['Token'] = 'END'
        return json.dumps(out)
    except:
        return ERROR

def tempFissionStations():
    # Then get crashes in the radius
    # size = 10000
    try:
        results = es.search(index='station_locations', body={
            "query": {
                "match_all": {}
            }
        })
        out = {}
        out['Status'] = 200
        # out['Token'] = results['_scroll_id']
        out['Data'] = results['hits']['hits']
        if len(out['Data']) == 0:
            out['Token'] = 'END'
        return json.dumps(out)
    except:
        return ERROR

