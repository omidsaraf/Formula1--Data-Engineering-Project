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

def save_json(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)

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
    save_json(data, file_path)

    # Display the saved data for debugging
    with open(file_path, 'r') as f:
        saved_data = json.load(f)
        print(f"{name.capitalize()} latest data saved to {file_path}")
        print(json.dumps(saved_data, indent=2))
