### from 2010 to 2024

````python
import requests
import json
import os

# Define the landing zone path
Landing_path = '/dbfs/mnt/dldatabricks/00-landing'

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data from {url}: Status code {response.status_code}")
        return {}

def fetch_full_data(series, season, data_key):
    url = f"https://ergast.com/api/{series}/{season}/{data_key}.json"
    data = fetch_data(url)
    if 'MRData' in data and data_key in data['MRData']:
        return data['MRData'][data_key]
    else:
        print(f"Key 'MRData' or '{data_key}' not found in the response.")
        return {}

# Define the series and seasons to fetch
series = "f1"
seasons = list(range(2010, 2024))

# Define data keys for different endpoints
key_mapping = {
    "lap_times": "LapTimeTable",
    "qualifying": "QualifyingTable",
    "circuits": "CircuitTable",
    "constructors": "ConstructorTable",
    "drivers": "DriverTable",
    "pit_stops": "PitStopTable",
    "races": "RaceTable",
    "results": "ResultTable"
}

# Ensure the landing path exists
os.makedirs(Landing_path, exist_ok=True)

# Extract and write full data to the landing zone
for season in seasons:
    for name, data_key in key_mapping.items():
        full_data = fetch_full_data(series, season, name)
        if full_data:
            file_path = os.path.join(Landing_path, f"{name}_{season}.json")
            with open(file_path, 'w') as f:
                json.dump(full_data, f, indent=2)
            print(f"{name.capitalize()} data for {season} saved to {file_path}")
        else:
            print(f"No data returned for {name} in {season}")

        if full_data:
            print(json.dumps(full_data, indent=2))


### All Data
````python
import requests
import json
import os

# Define the landing zone path for full load
Landing_path_full = '/dbfs/mnt/dldatabricks/00-landing'

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data from {url}: Status code {response.status_code}")
        return {}

def fetch_full_data(url, data_key):
    data = fetch_data(url)
    if 'MRData' in data and data_key in data['MRData']:
        full_data = data['MRData'][data_key]
        return full_data
    else:
        print(f"Key 'MRData' or '{data_key}' not found in the response.")
        return {}

# Define API endpoints for different data
endpoints = {
    "lap_times": "https://ergast.com/api/f1/current/lapTimes.json",
    "qualifying": "https://ergast.com/api/f1/current/qualifying.json",
    "circuits": "https://ergast.com/api/f1/circuits.json",
    "constructors": "https://ergast.com/api/f1/constructors.json",
    "drivers": "https://ergast.com/api/f1/drivers.json",
    "pit_stops": "https://ergast.com/api/f1/current/pitStops.json",
    "races": "https://ergast.com/api/f1/current/races.json",
    "results": "https://ergast.com/api/f1/current/results.json"
}

key_mapping = {
    "lap_times": "Laps",
    "qualifying": "QualifyingTable",
    "circuits": "CircuitTable",
    "constructors": "ConstructorTable",
    "drivers": "DriverTable",
    "pit_stops": "PitStopTable",
    "races": "RaceTable",
    "results": "ResultTable"
}

# Ensure the landing path exists
os.makedirs(Landing_path_full, exist_ok=True)

# Extract and write data to the landing zone
for name, url in endpoints.items():
    data_key = key_mapping.get(name)
    full_data = fetch_full_data(url, data_key)
    if full_data:
        file_path = os.path.join(Landing_path_full, f"{name}.json")
        with open(file_path, 'w') as f:
            json.dump(full_data, f, indent=2)
        print(f"{name.capitalize()} full data saved to {file_path}")
    else:
        print(f"No data returned for {name}")

    if full_data:
        print(json.dumps(full_data, indent=2))
