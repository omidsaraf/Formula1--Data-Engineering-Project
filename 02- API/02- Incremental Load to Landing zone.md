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
        # Handle non-200 responses or log an error message
        print(f"Error fetching data from {url}: Status code {response.status_code}")
        return {}

def fetch_latest_data(url, data_key, item_key=None):
    data = fetch_data(url)
    if 'MRData' in data and data_key in data['MRData']:
        if item_key and item_key in data['MRData'][data_key]:
            latest_data = data['MRData'][data_key][item_key][-1]
        else:
            latest_data = data['MRData'][data_key]
        return latest_data
    else:
        # Handle the case where 'MRData' or the data_key is not present
        print(f"Key 'MRData' or '{data_key}' not found in the response.")
        return {}

# Define API endpoints for different data
endpoints = {
    "lap_times": "http://ergast.com/api/f1/current/lapTimes.json",
    "qualifying": "http://ergast.com/api/f1/current/qualifying.json",
    "circuits": "http://ergast.com/api/f1/circuits.json",
    "constructors": "http://ergast.com/api/f1/constructors.json",
    "drivers": "http://ergast.com/api/f1/drivers.json",
    "pit_stops": "http://ergast.com/api/f1/current/pitStops.json",
    "races": "http://ergast.com/api/f1/current/races.json",
    "results": "http://ergast.com/api/f1/current/results.json"
}

key_mapping = {
    "lap_times": ("LapTimeTable", "Laps"),
    "qualifying": ("QualifyingTable", "Qualifying"),
    "circuits": ("CircuitTable", "Circuits"),
    "constructors": ("ConstructorTable", "Constructors"),
    "drivers": ("DriverTable", "Drivers"),
    "pit_stops": ("PitStopTable", "PitStops"),
    "races": ("RaceTable", "Races"),
    "results": ("ResultTable", "Results")
}

# Ensure the landing path exists
os.makedirs(Landing_path, exist_ok=True)

# Extract and write data to landing zone
for name, url in endpoints.items():
    data_key, item_key = key_mapping.get(name, (None, None))
    latest_data = fetch_latest_data(url, data_key, item_key)
    file_path = os.path.join(Landing_path, f"{name}.json")
    with open(file_path, 'w') as f:
        json.dump(latest_data, f, indent=2)
    print(f"{name.capitalize()} latest data saved to {file_path}")

    # Display the data
    print(json.dumps(latest_data, indent=2))
````

![image](https://github.com/user-attachments/assets/e263e8de-d475-488f-be56-7ed3e2bfcd8e)

