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
    "circuits": "http://ergast.com/api/f1/current/circuits.json",
    "constructors": "http://ergast.com/api/f1/current/constructors.json",
    "drivers": "http://ergast.com/api/f1/current/drivers.json",
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

````
````json
Qualifying latest data saved to /dbfs/mnt/dldatabricks/00-landing/qualifying.json
{
  "MRData": {
    "xmlns": "http://ergast.com/mrd/1.5",
    "series": "f1",
    "url": "http://ergast.com/api/f1/current/qualifying.json",
    "limit": "30",
    "offset": "0",
    "total": "359",
    "RaceTable": {
      "season": "2024",
      "Races": [
        {
          "season": "2024",
          "round": "1",
          "url": "https://en.wikipedia.org/wiki/2024_Bahrain_Grand_Prix",
          "raceName": "Bahrain Grand Prix",
          "Circuit": {
            "circuitId": "bahrain",
            "url": "http://en.wikipedia.org/wiki/Bahrain_International_Circuit",
            "circuitName": "Bahrain International Circuit",
            "Location": {
              "lat": "26.0325",
              "long": "50.5106",
              "locality": "Sakhir",
              "country": "Bahrain"
            }
          },
          "date": "2024-03-02",
          "time": "15:00:00Z",
          "QualifyingResults": [
            {
              "number": "1",
              "position": "1",
              "Driver": {
                "driverId": "max_verstappen",
                "permanentNumber": "33",
                "code": "VER",
                "url": "http://en.wikipedia.org/wiki/Max_Verstappen",
                "givenName": "Max",
                "familyName": "Verstappen",
                "dateOfBirth": "1997-09-30",
                "nationality": "Dutch"
              },
              "Constructor": {
                "constructorId": "red_bull",
                "url": "http://en.wikipedia.org/wiki/Red_Bull_Racing",
                "name": "Red Bull",
                "nationality": "Austrian"
              },
              "Q1": "1:30.031",
              "Q2": "1:29.374",
              "Q3": "1:29.179"
            },
            {
              "number": "16",
              "position": "2",
              "Driver": {
                "driverId": "leclerc",
                "permanentNumber": "16",
                "code": "LEC",
                "url": "http://en.wikipedia.org/wiki/Charles_Leclerc",
                "givenName": "Charles",
                "familyName": "Leclerc",
                "dateOfBirth": "1997-10-16",
                "nationality": "Monegasque"
              },
              "Constructor": {
                "constructorId": "ferrari",
                "url": "http://en.wikipedia.org/wiki/Scuderia_Ferrari",
                "name": "Ferrari",
                "nationality": "Italian"
              },
              "Q1": "1:30.243",
              "Q2": "1:29.165",
              "Q3": "1:29.407"
            },
            {
              "number": "63",
              "position": "3",
              "Driver": {
                "driverId": "russell",
                "permanentNumber": "63",
                "code": "RUS",
                "url": "http://en.wikipedia.org/wiki/George_Russell_(racing_driver)",
                "givenName": "George",
                "familyName": "Russell",
                "dateOfBirth": "1998-02-15",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mercedes",
                "url": "http://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One",
                "name": "Mercedes",
                "nationality": "German"
              },
              "Q1": "1:30.350",
              "Q2": "1:29.922",
              "Q3": "1:29.485"
            },
            {
              "number": "55",
              "position": "4",
              "Driver": {
                "driverId": "sainz",
                "permanentNumber": "55",
                "code": "SAI",
                "url": "http://en.wikipedia.org/wiki/Carlos_Sainz_Jr.",
                "givenName": "Carlos",
                "familyName": "Sainz",
                "dateOfBirth": "1994-09-01",
                "nationality": "Spanish"
              },
              "Constructor": {
                "constructorId": "ferrari",
                "url": "http://en.wikipedia.org/wiki/Scuderia_Ferrari",
                "name": "Ferrari",
                "nationality": "Italian"
              },
              "Q1": "1:29.909",
              "Q2": "1:29.573",
              "Q3": "1:29.507"
            },
            {
              "number": "11",
              "position": "5",
              "Driver": {
                "driverId": "perez",
                "permanentNumber": "11",
                "code": "PER",
                "url": "http://en.wikipedia.org/wiki/Sergio_P%C3%A9rez",
                "givenName": "Sergio",
                "familyName": "P\u00e9rez",
                "dateOfBirth": "1990-01-26",
                "nationality": "Mexican"
              },
              "Constructor": {
                "constructorId": "red_bull",
                "url": "http://en.wikipedia.org/wiki/Red_Bull_Racing",
                "name": "Red Bull",
                "nationality": "Austrian"
              },
              "Q1": "1:30.221",
              "Q2": "1:29.932",
              "Q3": "1:29.537"
            },
            {
              "number": "14",
              "position": "6",
              "Driver": {
                "driverId": "alonso",
                "permanentNumber": "14",
                "code": "ALO",
                "url": "http://en.wikipedia.org/wiki/Fernando_Alonso",
                "givenName": "Fernando",
                "familyName": "Alonso",
                "dateOfBirth": "1981-07-29",
                "nationality": "Spanish"
              },
              "Constructor": {
                "constructorId": "aston_martin",
                "url": "http://en.wikipedia.org/wiki/Aston_Martin_in_Formula_One",
                "name": "Aston Martin",
                "nationality": "British"
              },
              "Q1": "1:30.179",
              "Q2": "1:29.801",
              "Q3": "1:29.542"
            },
            {
              "number": "4",
              "position": "7",
              "Driver": {
                "driverId": "norris",
                "permanentNumber": "4",
                "code": "NOR",
                "url": "http://en.wikipedia.org/wiki/Lando_Norris",
                "givenName": "Lando",
                "familyName": "Norris",
                "dateOfBirth": "1999-11-13",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mclaren",
                "url": "http://en.wikipedia.org/wiki/McLaren",
                "name": "McLaren",
                "nationality": "British"
              },
              "Q1": "1:30.143",
              "Q2": "1:29.941",
              "Q3": "1:29.614"
            },
            {
              "number": "81",
              "position": "8",
              "Driver": {
                "driverId": "piastri",
                "permanentNumber": "81",
                "code": "PIA",
                "url": "http://en.wikipedia.org/wiki/Oscar_Piastri",
                "givenName": "Oscar",
                "familyName": "Piastri",
                "dateOfBirth": "2001-04-06",
                "nationality": "Australian"
              },
              "Constructor": {
                "constructorId": "mclaren",
                "url": "http://en.wikipedia.org/wiki/McLaren",
                "name": "McLaren",
                "nationality": "British"
              },
              "Q1": "1:30.531",
              "Q2": "1:30.122",
              "Q3": "1:29.683"
            },
            {
              "number": "44",
              "position": "9",
              "Driver": {
                "driverId": "hamilton",
                "permanentNumber": "44",
                "code": "HAM",
                "url": "http://en.wikipedia.org/wiki/Lewis_Hamilton",
                "givenName": "Lewis",
                "familyName": "Hamilton",
                "dateOfBirth": "1985-01-07",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mercedes",
                "url": "http://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One",
                "name": "Mercedes",
                "nationality": "German"
              },
              "Q1": "1:30.451",
              "Q2": "1:29.718",
              "Q3": "1:29.710"
            },
            {
              "number": "27",
              "position": "10",
              "Driver": {
                "driverId": "hulkenberg",
                "permanentNumber": "27",
                "code": "HUL",
                "url": "http://en.wikipedia.org/wiki/Nico_H%C3%BClkenberg",
                "givenName": "Nico",
                "familyName": "H\u00fclkenberg",
                "dateOfBirth": "1987-08-19",
                "nationality": "German"
              },
              "Constructor": {
                "constructorId": "haas",
                "url": "http://en.wikipedia.org/wiki/Haas_F1_Team",
                "name": "Haas F1 Team",
                "nationality": "American"
              },
              "Q1": "1:30.566",
              "Q2": "1:29.851",
              "Q3": "1:30.502"
            },
            {
              "number": "22",
              "position": "11",
              "Driver": {
                "driverId": "tsunoda",
                "permanentNumber": "22",
                "code": "TSU",
                "url": "http://en.wikipedia.org/wiki/Yuki_Tsunoda",
                "givenName": "Yuki",
                "familyName": "Tsunoda",
                "dateOfBirth": "2000-05-11",
                "nationality": "Japanese"
              },
              "Constructor": {
                "constructorId": "rb",
                "url": "http://en.wikipedia.org/wiki/RB_Formula_One_Team",
                "name": "RB F1 Team",
                "nationality": "Italian"
              },
              "Q1": "1:30.481",
              "Q2": "1:30.129"
            },
            {
              "number": "18",
              "position": "12",
              "Driver": {
                "driverId": "stroll",
                "permanentNumber": "18",
                "code": "STR",
                "url": "http://en.wikipedia.org/wiki/Lance_Stroll",
                "givenName": "Lance",
                "familyName": "Stroll",
                "dateOfBirth": "1998-10-29",
                "nationality": "Canadian"
              },
              "Constructor": {
                "constructorId": "aston_martin",
                "url": "http://en.wikipedia.org/wiki/Aston_Martin_in_Formula_One",
                "name": "Aston Martin",
                "nationality": "British"
              },
              "Q1": "1:29.965",
              "Q2": "1:30.200"
            },
            {
              "number": "23",
              "position": "13",
              "Driver": {
                "driverId": "albon",
                "permanentNumber": "23",
                "code": "ALB",
                "url": "http://en.wikipedia.org/wiki/Alexander_Albon",
                "givenName": "Alexander",
                "familyName": "Albon",
                "dateOfBirth": "1996-03-23",
                "nationality": "Thai"
              },
              "Constructor": {
                "constructorId": "williams",
                "url": "http://en.wikipedia.org/wiki/Williams_Grand_Prix_Engineering",
                "name": "Williams",
                "nationality": "British"
              },
              "Q1": "1:30.397",
              "Q2": "1:30.221"
            },
            {
              "number": "3",
              "position": "14",
              "Driver": {
                "driverId": "ricciardo",
                "permanentNumber": "3",
                "code": "RIC",
                "url": "http://en.wikipedia.org/wiki/Daniel_Ricciardo",
                "givenName": "Daniel",
                "familyName": "Ricciardo",
                "dateOfBirth": "1989-07-01",
                "nationality": "Australian"
              },
              "Constructor": {
                "constructorId": "rb",
                "url": "http://en.wikipedia.org/wiki/RB_Formula_One_Team",
                "name": "RB F1 Team",
                "nationality": "Italian"
              },
              "Q1": "1:30.562",
              "Q2": "1:30.278"
            },
            {
              "number": "20",
              "position": "15",
              "Driver": {
                "driverId": "kevin_magnussen",
                "permanentNumber": "20",
                "code": "MAG",
                "url": "http://en.wikipedia.org/wiki/Kevin_Magnussen",
                "givenName": "Kevin",
                "familyName": "Magnussen",
                "dateOfBirth": "1992-10-05",
                "nationality": "Danish"
              },
              "Constructor": {
                "constructorId": "haas",
                "url": "http://en.wikipedia.org/wiki/Haas_F1_Team",
                "name": "Haas F1 Team",
                "nationality": "American"
              },
              "Q1": "1:30.646",
              "Q2": "1:30.529"
            },
            {
              "number": "77",
              "position": "16",
              "Driver": {
                "driverId": "bottas",
                "permanentNumber": "77",
                "code": "BOT",
                "url": "http://en.wikipedia.org/wiki/Valtteri_Bottas",
                "givenName": "Valtteri",
                "familyName": "Bottas",
                "dateOfBirth": "1989-08-28",
                "nationality": "Finnish"
              },
              "Constructor": {
                "constructorId": "sauber",
                "url": "http://en.wikipedia.org/wiki/Sauber_Motorsport",
                "name": "Sauber",
                "nationality": "Swiss"
              },
              "Q1": "1:30.756"
            },
            {
              "number": "24",
              "position": "17",
              "Driver": {
                "driverId": "zhou",
                "permanentNumber": "24",
                "code": "ZHO",
                "url": "http://en.wikipedia.org/wiki/Zhou_Guanyu",
                "givenName": "Guanyu",
                "familyName": "Zhou",
                "dateOfBirth": "1999-05-30",
                "nationality": "Chinese"
              },
              "Constructor": {
                "constructorId": "sauber",
                "url": "http://en.wikipedia.org/wiki/Sauber_Motorsport",
                "name": "Sauber",
                "nationality": "Swiss"
              },
              "Q1": "1:30.757"
            },
            {
              "number": "2",
              "position": "18",
              "Driver": {
                "driverId": "sargeant",
                "permanentNumber": "2",
                "code": "SAR",
                "url": "http://en.wikipedia.org/wiki/Logan_Sargeant",
                "givenName": "Logan",
                "familyName": "Sargeant",
                "dateOfBirth": "2000-12-31",
                "nationality": "American"
              },
              "Constructor": {
                "constructorId": "williams",
                "url": "http://en.wikipedia.org/wiki/Williams_Grand_Prix_Engineering",
                "name": "Williams",
                "nationality": "British"
              },
              "Q1": "1:30.770"
            },
            {
              "number": "31",
              "position": "19",
              "Driver": {
                "driverId": "ocon",
                "permanentNumber": "31",
                "code": "OCO",
                "url": "http://en.wikipedia.org/wiki/Esteban_Ocon",
                "givenName": "Esteban",
                "familyName": "Ocon",
                "dateOfBirth": "1996-09-17",
                "nationality": "French"
              },
              "Constructor": {
                "constructorId": "alpine",
                "url": "http://en.wikipedia.org/wiki/Alpine_F1_Team",
                "name": "Alpine F1 Team",
                "nationality": "French"
              },
              "Q1": "1:30.793"
            },
            {
              "number": "10",
              "position": "20",
              "Driver": {
                "driverId": "gasly",
                "permanentNumber": "10",
                "code": "GAS",
                "url": "http://en.wikipedia.org/wiki/Pierre_Gasly",
                "givenName": "Pierre",
                "familyName": "Gasly",
                "dateOfBirth": "1996-02-07",
                "nationality": "French"
              },
              "Constructor": {
                "constructorId": "alpine",
                "url": "http://en.wikipedia.org/wiki/Alpine_F1_Team",
                "name": "Alpine F1 Team",
                "nationality": "French"
              },
              "Q1": "1:30.948"
            }
          ]
        },
        {
          "season": "2024",
          "round": "2",
          "url": "https://en.wikipedia.org/wiki/2024_Saudi_Arabian_Grand_Prix",
          "raceName": "Saudi Arabian Grand Prix",
          "Circuit": {
            "circuitId": "jeddah",
            "url": "http://en.wikipedia.org/wiki/Jeddah_Street_Circuit",
            "circuitName": "Jeddah Corniche Circuit",
            "Location": {
              "lat": "21.6319",
              "long": "39.1044",
              "locality": "Jeddah",
              "country": "Saudi Arabia"
            }
          },
          "date": "2024-03-09",
          "time": "17:00:00Z",
          "QualifyingResults": [
            {
              "number": "1",
              "position": "1",
              "Driver": {
                "driverId": "max_verstappen",
                "permanentNumber": "33",
                "code": "VER",
                "url": "http://en.wikipedia.org/wiki/Max_Verstappen",
                "givenName": "Max",
                "familyName": "Verstappen",
                "dateOfBirth": "1997-09-30",
                "nationality": "Dutch"
              },
              "Constructor": {
                "constructorId": "red_bull",
                "url": "http://en.wikipedia.org/wiki/Red_Bull_Racing",
                "name": "Red Bull",
                "nationality": "Austrian"
              },
              "Q1": "1:28.171",
              "Q2": "1:28.033",
              "Q3": "1:27.472"
            },
            {
              "number": "16",
              "position": "2",
              "Driver": {
                "driverId": "leclerc",
                "permanentNumber": "16",
                "code": "LEC",
                "url": "http://en.wikipedia.org/wiki/Charles_Leclerc",
                "givenName": "Charles",
                "familyName": "Leclerc",
                "dateOfBirth": "1997-10-16",
                "nationality": "Monegasque"
              },
              "Constructor": {
                "constructorId": "ferrari",
                "url": "http://en.wikipedia.org/wiki/Scuderia_Ferrari",
                "name": "Ferrari",
                "nationality": "Italian"
              },
              "Q1": "1:28.318",
              "Q2": "1:28.112",
              "Q3": "1:27.791"
            },
            {
              "number": "11",
              "position": "3",
              "Driver": {
                "driverId": "perez",
                "permanentNumber": "11",
                "code": "PER",
                "url": "http://en.wikipedia.org/wiki/Sergio_P%C3%A9rez",
                "givenName": "Sergio",
                "familyName": "P\u00e9rez",
                "dateOfBirth": "1990-01-26",
                "nationality": "Mexican"
              },
              "Constructor": {
                "constructorId": "red_bull",
                "url": "http://en.wikipedia.org/wiki/Red_Bull_Racing",
                "name": "Red Bull",
                "nationality": "Austrian"
              },
              "Q1": "1:28.638",
              "Q2": "1:28.467",
              "Q3": "1:27.807"
            },
            {
              "number": "14",
              "position": "4",
              "Driver": {
                "driverId": "alonso",
                "permanentNumber": "14",
                "code": "ALO",
                "url": "http://en.wikipedia.org/wiki/Fernando_Alonso",
                "givenName": "Fernando",
                "familyName": "Alonso",
                "dateOfBirth": "1981-07-29",
                "nationality": "Spanish"
              },
              "Constructor": {
                "constructorId": "aston_martin",
                "url": "http://en.wikipedia.org/wiki/Aston_Martin_in_Formula_One",
                "name": "Aston Martin",
                "nationality": "British"
              },
              "Q1": "1:28.706",
              "Q2": "1:28.122",
              "Q3": "1:27.846"
            },
            {
              "number": "81",
              "position": "5",
              "Driver": {
                "driverId": "piastri",
                "permanentNumber": "81",
                "code": "PIA",
                "url": "http://en.wikipedia.org/wiki/Oscar_Piastri",
                "givenName": "Oscar",
                "familyName": "Piastri",
                "dateOfBirth": "2001-04-06",
                "nationality": "Australian"
              },
              "Constructor": {
                "constructorId": "mclaren",
                "url": "http://en.wikipedia.org/wiki/McLaren",
                "name": "McLaren",
                "nationality": "British"
              },
              "Q1": "1:28.755",
              "Q2": "1:28.343",
              "Q3": "1:28.089"
            },
            {
              "number": "4",
              "position": "6",
              "Driver": {
                "driverId": "norris",
                "permanentNumber": "4",
                "code": "NOR",
                "url": "http://en.wikipedia.org/wiki/Lando_Norris",
                "givenName": "Lando",
                "familyName": "Norris",
                "dateOfBirth": "1999-11-13",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mclaren",
                "url": "http://en.wikipedia.org/wiki/McLaren",
                "name": "McLaren",
                "nationality": "British"
              },
              "Q1": "1:28.805",
              "Q2": "1:28.479",
              "Q3": "1:28.132"
            },
            {
              "number": "63",
              "position": "7",
              "Driver": {
                "driverId": "russell",
                "permanentNumber": "63",
                "code": "RUS",
                "url": "http://en.wikipedia.org/wiki/George_Russell_(racing_driver)",
                "givenName": "George",
                "familyName": "Russell",
                "dateOfBirth": "1998-02-15",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mercedes",
                "url": "http://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One",
                "name": "Mercedes",
                "nationality": "German"
              },
              "Q1": "1:28.749",
              "Q2": "1:28.448",
              "Q3": "1:28.316"
            },
            {
              "number": "44",
              "position": "8",
              "Driver": {
                "driverId": "hamilton",
                "permanentNumber": "44",
                "code": "HAM",
                "url": "http://en.wikipedia.org/wiki/Lewis_Hamilton",
                "givenName": "Lewis",
                "familyName": "Hamilton",
                "dateOfBirth": "1985-01-07",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mercedes",
                "url": "http://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One",
                "name": "Mercedes",
                "nationality": "German"
              },
              "Q1": "1:28.994",
              "Q2": "1:28.606",
              "Q3": "1:28.460"
            },
            {
              "number": "22",
              "position": "9",
              "Driver": {
                "driverId": "tsunoda",
                "permanentNumber": "22",
                "code": "TSU",
                "url": "http://en.wikipedia.org/wiki/Yuki_Tsunoda",
                "givenName": "Yuki",
                "familyName": "Tsunoda",
                "dateOfBirth": "2000-05-11",
                "nationality": "Japanese"
              },
              "Constructor": {
                "constructorId": "rb",
                "url": "http://en.wikipedia.org/wiki/RB_Formula_One_Team",
                "name": "RB F1 Team",
                "nationality": "Italian"
              },
              "Q1": "1:28.988",
              "Q2": "1:28.564",
              "Q3": "

*** WARNING: max output size exceeded, skipping output. ***

port",
                "name": "Sauber",
                "nationality": "Swiss"
              },
              "grid": "17",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "14",
                "lap": "30",
                "Time": {
                  "time": "1:35.458"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "204.102"
                }
              }
            },
            {
              "number": "20",
              "position": "12",
              "positionText": "12",
              "points": "0",
              "Driver": {
                "driverId": "kevin_magnussen",
                "permanentNumber": "20",
                "code": "MAG",
                "url": "http://en.wikipedia.org/wiki/Kevin_Magnussen",
                "givenName": "Kevin",
                "familyName": "Magnussen",
                "dateOfBirth": "1992-10-05",
                "nationality": "Danish"
              },
              "Constructor": {
                "constructorId": "haas",
                "url": "http://en.wikipedia.org/wiki/Haas_F1_Team",
                "name": "Haas F1 Team",
                "nationality": "American"
              },
              "grid": "15",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "15",
                "lap": "34",
                "Time": {
                  "time": "1:35.570"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "203.863"
                }
              }
            },
            {
              "number": "3",
              "position": "13",
              "positionText": "13",
              "points": "0",
              "Driver": {
                "driverId": "ricciardo",
                "permanentNumber": "3",
                "code": "RIC",
                "url": "http://en.wikipedia.org/wiki/Daniel_Ricciardo",
                "givenName": "Daniel",
                "familyName": "Ricciardo",
                "dateOfBirth": "1989-07-01",
                "nationality": "Australian"
              },
              "Constructor": {
                "constructorId": "rb",
                "url": "http://en.wikipedia.org/wiki/RB_Formula_One_Team",
                "name": "RB F1 Team",
                "nationality": "Italian"
              },
              "grid": "14",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "13",
                "lap": "37",
                "Time": {
                  "time": "1:35.163"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "204.735"
                }
              }
            },
            {
              "number": "22",
              "position": "14",
              "positionText": "14",
              "points": "0",
              "Driver": {
                "driverId": "tsunoda",
                "permanentNumber": "22",
                "code": "TSU",
                "url": "http://en.wikipedia.org/wiki/Yuki_Tsunoda",
                "givenName": "Yuki",
                "familyName": "Tsunoda",
                "dateOfBirth": "2000-05-11",
                "nationality": "Japanese"
              },
              "Constructor": {
                "constructorId": "rb",
                "url": "http://en.wikipedia.org/wiki/RB_Formula_One_Team",
                "name": "RB F1 Team",
                "nationality": "Italian"
              },
              "grid": "11",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "18",
                "lap": "37",
                "Time": {
                  "time": "1:35.833"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "203.303"
                }
              }
            },
            {
              "number": "23",
              "position": "15",
              "positionText": "15",
              "points": "0",
              "Driver": {
                "driverId": "albon",
                "permanentNumber": "23",
                "code": "ALB",
                "url": "http://en.wikipedia.org/wiki/Alexander_Albon",
                "givenName": "Alexander",
                "familyName": "Albon",
                "dateOfBirth": "1996-03-23",
                "nationality": "Thai"
              },
              "Constructor": {
                "constructorId": "williams",
                "url": "http://en.wikipedia.org/wiki/Williams_Grand_Prix_Engineering",
                "name": "Williams",
                "nationality": "British"
              },
              "grid": "13",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "17",
                "lap": "40",
                "Time": {
                  "time": "1:35.723"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "203.537"
                }
              }
            },
            {
              "number": "27",
              "position": "16",
              "positionText": "16",
              "points": "0",
              "Driver": {
                "driverId": "hulkenberg",
                "permanentNumber": "27",
                "code": "HUL",
                "url": "http://en.wikipedia.org/wiki/Nico_H%C3%BClkenberg",
                "givenName": "Nico",
                "familyName": "H\u00fclkenberg",
                "dateOfBirth": "1987-08-19",
                "nationality": "German"
              },
              "Constructor": {
                "constructorId": "haas",
                "url": "http://en.wikipedia.org/wiki/Haas_F1_Team",
                "name": "Haas F1 Team",
                "nationality": "American"
              },
              "grid": "10",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "10",
                "lap": "46",
                "Time": {
                  "time": "1:34.834"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "205.445"
                }
              }
            },
            {
              "number": "31",
              "position": "17",
              "positionText": "17",
              "points": "0",
              "Driver": {
                "driverId": "ocon",
                "permanentNumber": "31",
                "code": "OCO",
                "url": "http://en.wikipedia.org/wiki/Esteban_Ocon",
                "givenName": "Esteban",
                "familyName": "Ocon",
                "dateOfBirth": "1996-09-17",
                "nationality": "French"
              },
              "Constructor": {
                "constructorId": "alpine",
                "url": "http://en.wikipedia.org/wiki/Alpine_F1_Team",
                "name": "Alpine F1 Team",
                "nationality": "French"
              },
              "grid": "19",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "20",
                "lap": "34",
                "Time": {
                  "time": "1:36.226"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "202.473"
                }
              }
            },
            {
              "number": "10",
              "position": "18",
              "positionText": "18",
              "points": "0",
              "Driver": {
                "driverId": "gasly",
                "permanentNumber": "10",
                "code": "GAS",
                "url": "http://en.wikipedia.org/wiki/Pierre_Gasly",
                "givenName": "Pierre",
                "familyName": "Gasly",
                "dateOfBirth": "1996-02-07",
                "nationality": "French"
              },
              "Constructor": {
                "constructorId": "alpine",
                "url": "http://en.wikipedia.org/wiki/Alpine_F1_Team",
                "name": "Alpine F1 Team",
                "nationality": "French"
              },
              "grid": "20",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "9",
                "lap": "45",
                "Time": {
                  "time": "1:34.805"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "205.508"
                }
              }
            },
            {
              "number": "77",
              "position": "19",
              "positionText": "19",
              "points": "0",
              "Driver": {
                "driverId": "bottas",
                "permanentNumber": "77",
                "code": "BOT",
                "url": "http://en.wikipedia.org/wiki/Valtteri_Bottas",
                "givenName": "Valtteri",
                "familyName": "Bottas",
                "dateOfBirth": "1989-08-28",
                "nationality": "Finnish"
              },
              "Constructor": {
                "constructorId": "sauber",
                "url": "http://en.wikipedia.org/wiki/Sauber_Motorsport",
                "name": "Sauber",
                "nationality": "Swiss"
              },
              "grid": "16",
              "laps": "56",
              "status": "+1 Lap",
              "FastestLap": {
                "rank": "19",
                "lap": "33",
                "Time": {
                  "time": "1:36.202"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "202.523"
                }
              }
            },
            {
              "number": "2",
              "position": "20",
              "positionText": "20",
              "points": "0",
              "Driver": {
                "driverId": "sargeant",
                "permanentNumber": "2",
                "code": "SAR",
                "url": "http://en.wikipedia.org/wiki/Logan_Sargeant",
                "givenName": "Logan",
                "familyName": "Sargeant",
                "dateOfBirth": "2000-12-31",
                "nationality": "American"
              },
              "Constructor": {
                "constructorId": "williams",
                "url": "http://en.wikipedia.org/wiki/Williams_Grand_Prix_Engineering",
                "name": "Williams",
                "nationality": "British"
              },
              "grid": "18",
              "laps": "55",
              "status": "+2 Laps",
              "FastestLap": {
                "rank": "8",
                "lap": "42",
                "Time": {
                  "time": "1:34.735"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "205.659"
                }
              }
            }
          ]
        },
        {
          "season": "2024",
          "round": "2",
          "url": "https://en.wikipedia.org/wiki/2024_Saudi_Arabian_Grand_Prix",
          "raceName": "Saudi Arabian Grand Prix",
          "Circuit": {
            "circuitId": "jeddah",
            "url": "http://en.wikipedia.org/wiki/Jeddah_Street_Circuit",
            "circuitName": "Jeddah Corniche Circuit",
            "Location": {
              "lat": "21.6319",
              "long": "39.1044",
              "locality": "Jeddah",
              "country": "Saudi Arabia"
            }
          },
          "date": "2024-03-09",
          "time": "17:00:00Z",
          "Results": [
            {
              "number": "1",
              "position": "1",
              "positionText": "1",
              "points": "25",
              "Driver": {
                "driverId": "max_verstappen",
                "permanentNumber": "33",
                "code": "VER",
                "url": "http://en.wikipedia.org/wiki/Max_Verstappen",
                "givenName": "Max",
                "familyName": "Verstappen",
                "dateOfBirth": "1997-09-30",
                "nationality": "Dutch"
              },
              "Constructor": {
                "constructorId": "red_bull",
                "url": "http://en.wikipedia.org/wiki/Red_Bull_Racing",
                "name": "Red Bull",
                "nationality": "Austrian"
              },
              "grid": "1",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4843273",
                "time": "1:20:43.273"
              },
              "FastestLap": {
                "rank": "3",
                "lap": "50",
                "Time": {
                  "time": "1:31.773"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "242.188"
                }
              }
            },
            {
              "number": "11",
              "position": "2",
              "positionText": "2",
              "points": "18",
              "Driver": {
                "driverId": "perez",
                "permanentNumber": "11",
                "code": "PER",
                "url": "http://en.wikipedia.org/wiki/Sergio_P%C3%A9rez",
                "givenName": "Sergio",
                "familyName": "P\u00e9rez",
                "dateOfBirth": "1990-01-26",
                "nationality": "Mexican"
              },
              "Constructor": {
                "constructorId": "red_bull",
                "url": "http://en.wikipedia.org/wiki/Red_Bull_Racing",
                "name": "Red Bull",
                "nationality": "Austrian"
              },
              "grid": "3",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4862704",
                "time": "+13.6431"
              },
              "FastestLap": {
                "rank": "8",
                "lap": "37",
                "Time": {
                  "time": "1:32.273"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "240.876"
                }
              }
            },
            {
              "number": "16",
              "position": "3",
              "positionText": "3",
              "points": "16",
              "Driver": {
                "driverId": "leclerc",
                "permanentNumber": "16",
                "code": "LEC",
                "url": "http://en.wikipedia.org/wiki/Charles_Leclerc",
                "givenName": "Charles",
                "familyName": "Leclerc",
                "dateOfBirth": "1997-10-16",
                "nationality": "Monegasque"
              },
              "Constructor": {
                "constructorId": "ferrari",
                "url": "http://en.wikipedia.org/wiki/Scuderia_Ferrari",
                "name": "Ferrari",
                "nationality": "Italian"
              },
              "grid": "2",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4861912",
                "time": "+18.639"
              },
              "FastestLap": {
                "rank": "1",
                "lap": "50",
                "Time": {
                  "time": "1:31.632"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "242.561"
                }
              }
            },
            {
              "number": "81",
              "position": "4",
              "positionText": "4",
              "points": "12",
              "Driver": {
                "driverId": "piastri",
                "permanentNumber": "81",
                "code": "PIA",
                "url": "http://en.wikipedia.org/wiki/Oscar_Piastri",
                "givenName": "Oscar",
                "familyName": "Piastri",
                "dateOfBirth": "2001-04-06",
                "nationality": "Australian"
              },
              "Constructor": {
                "constructorId": "mclaren",
                "url": "http://en.wikipedia.org/wiki/McLaren",
                "name": "McLaren",
                "nationality": "British"
              },
              "grid": "5",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4875280",
                "time": "+32.007"
              },
              "FastestLap": {
                "rank": "10",
                "lap": "1",
                "Time": {
                  "time": "1:32.310"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "240.779"
                }
              }
            },
            {
              "number": "14",
              "position": "5",
              "positionText": "5",
              "points": "10",
              "Driver": {
                "driverId": "alonso",
                "permanentNumber": "14",
                "code": "ALO",
                "url": "http://en.wikipedia.org/wiki/Fernando_Alonso",
                "givenName": "Fernando",
                "familyName": "Alonso",
                "dateOfBirth": "1981-07-29",
                "nationality": "Spanish"
              },
              "Constructor": {
                "constructorId": "aston_martin",
                "url": "http://en.wikipedia.org/wiki/Aston_Martin_in_Formula_One",
                "name": "Aston Martin",
                "nationality": "British"
              },
              "grid": "4",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4879032",
                "time": "+35.759"
              },
              "FastestLap": {
                "rank": "13",
                "lap": "43",
                "Time": {
                  "time": "1:32.387"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "240.579"
                }
              }
            },
            {
              "number": "63",
              "position": "6",
              "positionText": "6",
              "points": "8",
              "Driver": {
                "driverId": "russell",
                "permanentNumber": "63",
                "code": "RUS",
                "url": "http://en.wikipedia.org/wiki/George_Russell_(racing_driver)",
                "givenName": "George",
                "familyName": "Russell",
                "dateOfBirth": "1998-02-15",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mercedes",
                "url": "http://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One",
                "name": "Mercedes",
                "nationality": "German"
              },
              "grid": "7",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4883209",
                "time": "+39.936"
              },
              "FastestLap": {
                "rank": "7",
                "lap": "42",
                "Time": {
                  "time": "1:32.254"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "240.926"
                }
              }
            },
            {
              "number": "38",
              "position": "7",
              "positionText": "7",
              "points": "6",
              "Driver": {
                "driverId": "bearman",
                "permanentNumber": "38",
                "code": "BEA",
                "url": "http://en.wikipedia.org/wiki/Oliver_Bearman",
                "givenName": "Oliver",
                "familyName": "Bearman",
                "dateOfBirth": "2005-05-08",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "ferrari",
                "url": "http://en.wikipedia.org/wiki/Scuderia_Ferrari",
                "name": "Ferrari",
                "nationality": "Italian"
              },
              "grid": "11",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4885952",
                "time": "+42.679"
              },
              "FastestLap": {
                "rank": "5",
                "lap": "50",
                "Time": {
                  "time": "1:32.186"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "241.103"
                }
              }
            },
            {
              "number": "4",
              "position": "8",
              "positionText": "8",
              "points": "4",
              "Driver": {
                "driverId": "norris",
                "permanentNumber": "4",
                "code": "NOR",
                "url": "http://en.wikipedia.org/wiki/Lando_Norris",
                "givenName": "Lando",
                "familyName": "Norris",
                "dateOfBirth": "1999-11-13",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mclaren",
                "url": "http://en.wikipedia.org/wiki/McLaren",
                "name": "McLaren",
                "nationality": "British"
              },
              "grid": "6",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4888981",
                "time": "+45.708"
              },
              "FastestLap": {
                "rank": "4",
                "lap": "1",
                "Time": {
                  "time": "1:31.944"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "241.738"
                }
              }
            },
            {
              "number": "44",
              "position": "9",
              "positionText": "9",
              "points": "2",
              "Driver": {
                "driverId": "hamilton",
                "permanentNumber": "44",
                "code": "HAM",
                "url": "http://en.wikipedia.org/wiki/Lewis_Hamilton",
                "givenName": "Lewis",
                "familyName": "Hamilton",
                "dateOfBirth": "1985-01-07",
                "nationality": "British"
              },
              "Constructor": {
                "constructorId": "mercedes",
                "url": "http://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One",
                "name": "Mercedes",
                "nationality": "German"
              },
              "grid": "8",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4890664",
                "time": "+47.391"
              },
              "FastestLap": {
                "rank": "2",
                "lap": "38",
                "Time": {
                  "time": "1:31.746"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "242.260"
                }
              }
            },
            {
              "number": "27",
              "position": "10",
              "positionText": "10",
              "points": "1",
              "Driver": {
                "driverId": "hulkenberg",
                "permanentNumber": "27",
                "code": "HUL",
                "url": "http://en.wikipedia.org/wiki/Nico_H%C3%BClkenberg",
                "givenName": "Nico",
                "familyName": "H\u00fclkenberg",
                "dateOfBirth": "1987-08-19",
                "nationality": "German"
              },
              "Constructor": {
                "constructorId": "haas",
                "url": "http://en.wikipedia.org/wiki/Haas_F1_Team",
                "name": "Haas F1 Team",
                "nationality": "American"
              },
              "grid": "15",
              "laps": "50",
              "status": "Finished",
              "Time": {
                "millis": "4920269",
                "time": "+1:16.996"
              },
              "FastestLap": {
                "rank": "12",
                "lap": "49",
                "Time": {
                  "time": "1:32.366"
                },
                "AverageSpeed": {
                  "units": "kph",
                  "speed": "240.633"
                }
              }
            }
          ]
        }
      ]
    }
  }
}

