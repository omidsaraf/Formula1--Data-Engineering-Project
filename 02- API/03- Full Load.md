### Full Load

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

def flatten_data(data, data_key, item_key=None):
    flattened = []
    if 'MRData' in data and data_key in data['MRData']:
        items = data['MRData'][data_key]
        if item_key:
            for item in items:
                if isinstance(item, dict) and item_key in item:
                    if isinstance(item[item_key], list):
                        flattened.extend(item[item_key])
                    else:
                        flattened.append(item[item_key])
                else:
                    flattened.append(item)
        else:
            flattened = items
    else:
        print(f"Key 'MRData' or '{data_key}' not found in the response.")
    return flattened

def process_multiline_json(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return data

# Define API endpoints for different data
endpoints = {
    "qualifying": "http://ergast.com/api/f1/current/qualifying.json",
    "circuits": "http://ergast.com/api/f1/circuits.json",
    "constructors": "http://ergast.com/api/f1/constructors.json",
    "drivers": "http://ergast.com/api/f1/drivers.json",
    "races": "http://ergast.com/api/f1/current/races.json",
    "results": "https://ergast.com/api/f1/current/results.json"
}

key_mapping = {
    "qualifying": ("QualifyingTable", "Qualifying"),
    "circuits": ("CircuitTable", "Circuits"),
    "constructors": ("ConstructorTable", "Constructors"),
    "drivers": ("DriverTable", "Drivers"),
    "races": ("RaceTable", "Races"),
    "results": ("ResultTable", "Results")
}

# Ensure the landing path exists
os.makedirs(Landing_path, exist_ok=True)

# Extract and write data to landing zone
for name, url in endpoints.items():
    data = fetch_data(url)
    file_path = os.path.join(Landing_path, f"{name}.json")
    
    # Flatten data if needed
    data_key, item_key = key_mapping.get(name, (None, None))
    if data_key:
        flattened_data = flatten_data(data, data_key, item_key)
    else:
        flattened_data = data

    # Save and process multi-line JSON data
    save_json(flattened_data, file_path)
    if name in ["results", "qualifying"]:
        processed_data = process_multiline_json(file_path)
        save_json(processed_data, file_path)
        print(f"{name.capitalize()} data read from multi-line JSON and processed.")
    
    # Display the data
    with open(file_path, 'r') as f:
        saved_data = json.load(f)
        print(f"{name.capitalize()} latest data saved to {file_path}")
        print(json.dumps(saved_data, indent=2))



`````
Specific Years (2020 - 2025)
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

def flatten_data(data, data_key, item_key=None):
    flattened = []
    if 'MRData' in data and data_key in data['MRData']:
        items = data['MRData'][data_key]
        if item_key:
            for item in items:
                if isinstance(item, dict) and item_key in item:
                    if isinstance(item[item_key], list):
                        flattened.extend(item[item_key])
                    else:
                        flattened.append(item[item_key])
                else:
                    flattened.append(item)
        else:
            flattened = items
    else:
        print(f"Key 'MRData' or '{data_key}' not found in the response.")
    return flattened

def process_multiline_json(file_path):
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    return data

def save_json(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)

# Define API endpoints for different data and add years to fetch from 2020 to current year
years = list(range(2020, 2025))
endpoints = {f"{year}_{name}": f"http://ergast.com/api/f1/{year}/{name}.json" for year in years for name in ["qualifying", "circuits", "constructors", "drivers", "races", "results"]}

key_mapping = {
    "qualifying": ("QualifyingTable", "Qualifying"),
    "circuits": ("CircuitTable", "Circuits"),
    "constructors": ("ConstructorTable", "Constructors"),
    "drivers": ("DriverTable", "Drivers"),
    "races": ("RaceTable", "Races"),
    "results": ("RaceTable", "Results")
}

# Ensure the landing path exists
os.makedirs(Landing_path, exist_ok=True)

# Extract and write data to landing zone
for endpoint_key, url in endpoints.items():
    data = fetch_data(url)
    file_path = os.path.join(Landing_path, f"{endpoint_key}.json")
    
    # Flatten data if needed
    data_key, item_key = key_mapping.get(endpoint_key.split('_')[-1], (None, None))
    if data_key:
        flattened_data = flatten_data(data, data_key, item_key)
    else:
        flattened_data = data

    # Save and process multi-line JSON data
    save_json(flattened_data, file_path)
    if endpoint_key.split('_')[-1] in ["results", "qualifying"]:
        processed_data = process_multiline_json(file_path)
        save_json(processed_data, file_path)
        print(f"{endpoint_key.split('_')[-1].capitalize()} data read from multi-line JSON and processed.")
    
    # Display the data
    with open(file_path, 'r') as f:
        saved_data = json.load(f)
        print(f"{endpoint_key.split('_')[-1].capitalize()} latest data saved to {file_path}")
        print(json.dumps(saved_data, indent=2))

