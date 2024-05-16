import json
import time

states = ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']


for state in states:

    print(state)
    with open(f'backend/fission/functions/fetch-weather-obs/bom_lookup_{state.lower()}.json') as f:
        data = json.load(f)
        print(data)
    print(f'{state} done.')
    time.sleep(1)


# read in the bom lookup json file
with open('./backend/fission/functions/fetch-weather-obs/bom_lookup_vic.json') as f:
    bom_lookup = json.load(f)

bom_lookup

# Function to recursively print the structure
def print_structure(d, indent=0):
    for key, value in d.items():
        print('  ' * indent + str(key))
        if isinstance(value, dict):
            print_structure(value, indent + 1)
        elif isinstance(value, list):
            print('  ' * (indent + 1) + '[List]')
            if len(value) > 0 and isinstance(value[0], dict):
                print_structure(value[0], indent + 2)

# Print the structure
print_structure(bom_lookup)
from pprint import pprint
pprint(bom_lookup)


import json
import time

states = ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']
lookup = {}

def extract_top_level_groupings(state, data):
    top_level_keys = list(data.keys())
    return {state: top_level_keys}

for state in states:
    print(state)
    with open(f'backend/fission/functions/fetch-weather-obs/bom_lookup_{state.lower()}.json') as f:
        data = json.load(f)
        lookup[state] = extract_top_level_groupings(state, data)
    print(f'{state} done.')
    time.sleep(1)

# Save the lookup to a JSON file
with open('top_level_groupings.json', 'w') as f:
    json.dump(lookup, f, indent=4)

print("Lookup JSON file created successfully!")
