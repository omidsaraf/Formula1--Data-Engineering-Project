Logical Design:

![Untitled](https://github.com/user-attachments/assets/b724ae10-7e2a-44ad-8f7e-3821baf173fc)


## Dimension Tables:
---

### Circuits Dimension

- circuit_sk: Integer
- circuit_ID: string
- circuit_Name: string
- location: string
- country: string
- lat: double
- lng: double

### Constructors Dimension

- Constructor_sk: Integer
- Constructor_name: string
- nationality: string

### Drivers Dimension

- driver_sk: Integer
- full_name: string
- dob: string
- nationality: string


### Races Dimension

- race_id: integer
- race_year: integer
- name: string
- round: integer
- circuit_id: string
- race_timestamp: timestamp
- ingestion_date: timestamp

## Fact Tables:
---


### Qualifying Fact
- qualify_Id: integer
- constructor_Id: string
- number: integer
- position: integer
- q1: string
- q2: string
- q3: string
- DriverName: string
- race_Id: integer
- ingestion_date: timestamp

### Results Fact
- season: integer
- round: integer
- raceName: string
- raceDate: string
- raceTime: string
- driver_number: integer
- position: integer
- points: double
- driver_fullName: string
- dob: string
- nationality: string
- constructor_name: string
- grid_position: integer
- laps: integer
- status: string
- time: string
- fastest_LapRank: integer
- fastest_LapNumber: integer
- fastest_LapTime: string
- average_Speed: double
- ingestion_date: timestamp

## Instructions for Use
1. **Ensure your Spark session is properly configured and running**.
2. **Set the paths to your Silver Layer data**.
3. **Run the provided Python code snippets in a notebook or script to read and display the data**.
4. **Perform any additional transformations if needed before writing them to the Gold Layer**.

## Dimensions

### Circuits Dimension
```python

# Define the path to your Silver Layer data
Path_Circuits = "/mnt/dldatabricks/02-silver/circuits/"

# Read the Delta table into a DataFrame
circuits_df = spark.read.format("delta").load(Path_Circuits)

# Display the DataFrame
circuits_df.show(truncate=False)
```

### Constructors Dimension
````python

# Define the path to your Silver Layer data
Path_Constructors = "/mnt/dldatabricks/02-silver/constructors/"

# Read the Delta table into a DataFrame
constructors_df = spark.read.format("delta").load(Path_Constructors)

# Display the DataFrame
constructors_df.show(truncate=False)
````


### Drivers Dimension
````python

# Define the path to your Silver Layer data
Path_Drivers = "/mnt/dldatabricks/02-silver/drivers/"

# Read the Delta table into a DataFrame
drivers_df = spark.read.format("delta").load(Path_Drivers)

# Display the DataFrame
drivers_df.show(truncate=False)
````
## Fact Tables
### Races Fact

````python
# Define the path to your Silver Layer data
Path_Race = "/mnt/dldatabricks/02-silver/races/"

# Read the Delta table into a DataFrame
races_df = spark.read.format("delta").load(Path_Race)

# Display the DataFrame
races_df.show(truncate=False)
````


### Qualifying Fact
````python
# Define the path to your Silver Layer data
Path_Qualifying = "/mnt/dldatabricks/02-silver/qualifying/"

# Read the Delta table into a DataFrame
qualifying_df = spark.read.format("delta").load(Path_Qualifying)

# Display the DataFrame
qualifying_df.show(truncate=False)
````
### Results Fact
`````python
# Define the path to your Silver Layer data
Path_Results = "/mnt/dldatabricks/02-silver/results/"

# Read the Delta table into a DataFrame
results_df = spark.read.format("delta").load(Path_Results)

# Display the DataFrame
results_df.show(truncate=False)
