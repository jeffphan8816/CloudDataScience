"""
Compare stations from BOM and ES

This script compares the stations from the BOM and ES to see if they match up.

Spoiler: they don't.

Likely due to Roaming stations and changes over time
"""
import json
import pandas as pd

with open("./data/all_stations_from_es.json", "r") as f:
    all_stations_from_es = json.load(f)

with open("./data/bom_stations.json", "r") as f:
    bom_stations = json.load(f)

es_stations_source = [station['_source'] for station in all_stations_from_es]

df_es_stations = pd.DataFrame(es_stations_source)
df_bom_stations = pd.DataFrame(bom_stations)

df_es_stations[['longitude', 'latitude']] = pd.DataFrame(df_es_stations['location'].tolist(), index=df_es_stations.index)
df_es_stations = df_es_stations.drop(columns=['location'])

df_es_stations = df_es_stations.drop_duplicates()
df_bom_stations = df_bom_stations.drop_duplicates()

len(df_bom_stations)
len(df_es_stations)

df_bom_stations['ID'] = df_bom_stations['ID'].astype(int)

df_bom_stations.iloc[0]
df_es_stations.iloc[0]

# Station ID seems to match some but not others...
diff1 = df_bom_stations[~df_bom_stations['ID'].isin(df_es_stations['Station ID'])]
diff2 = df_es_stations[~df_es_stations['Station ID'].isin(df_bom_stations['ID'])]
diff1
diff2

same1 = df_bom_stations[df_bom_stations['ID'].isin(df_es_stations['Station ID'])]
same2 = df_es_stations[df_es_stations['Station ID'].isin(df_bom_stations['ID'])]
same1
same2

# Eg Geelong/Geelong Racecourse
mask = df_bom_stations.apply(lambda row: row.astype(str).str.contains('geelong', case=False).any(), axis=1)
df_bom_stations[mask]

mask = df_es_stations.apply(lambda row: row.astype(str).str.contains('geelong', case=False).any(), axis=1)
df_es_stations[mask]

