#### Initial Load
---
````python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define the path to your JSON file
results_path = '/mnt/dldatabricks/01-bronze/*/results.json'

# Read the JSON file into a DataFrame
df = spark.read.json(results_path, multiLine=True)

# Explode the nested Races array
races_df = df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Explode the nested Results array within each race
results_df = races_df.select(
    col("race.season").cast(IntegerType()).alias("season"),
    col("race.round").cast(IntegerType()).alias("round"),
    col("race.raceName").alias("raceName"),
    col("race.date").cast(StringType()).alias("raceDate"),
    col("race.time").alias("raceTime"),
    explode(col("race.Results")).alias("result")
)

# Extract required fields and create full name
results_bronze = results_df.select(
    col("season"),
    col("round"),
    col("raceName"),
    col("raceDate"),
    col("raceTime"),
    col("result.number").cast(IntegerType()).alias("driver_number"),
    col("result.position").cast(IntegerType()).alias("position"),
    col("result.points").cast(DoubleType()).alias("points"),
    concat_ws(" ", col("result.Driver.givenName"), col("result.Driver.familyName")).alias("driver_fullName"),  # Combine givenName and familyName
    col("result.Driver.dateOfBirth").alias("dob"),
    col("result.Driver.nationality").alias("nationality"),
    col("result.Constructor.name").alias("constructor_name"),
    col("result.grid").cast(IntegerType()).alias("grid_position"),
    col("result.laps").cast(IntegerType()).alias("laps"),
    col("result.status").alias("status"),
    col("result.Time.time").alias("time"),
    col("result.FastestLap.rank").cast(IntegerType()).alias("fastest_LapRank"),
    col("result.FastestLap.lap").cast(IntegerType()).alias("fastest_LapNumber"),
    col("result.FastestLap.Time.time").alias("fastest_LapTime"),
    col("result.FastestLap.AverageSpeed.speed").cast(DoubleType()).alias("average_Speed")
)
results_bronze = results_bronze.withColumn("ingestion_date", current_timestamp())

# write the DataFrame in Parquet format to the destination partition by season and round


results_bronze.write.partitionBy("season", "round").mode("append").saveAsTable("F1_Silver.results")

#display
results_silver=spark.read.format("delta").load("/mnt/dldatabricks/02-silver/F1_Silver/results")
results_silver.display()

````
![image](https://github.com/user-attachments/assets/e2c9772b-fb9f-4c9b-ba70-8907889630dd)


![image](https://github.com/user-attachments/assets/cbd5b100-acb1-4777-a454-aa95e1669f38)

````
season	round	raceName	raceDate	raceTime	driver_number	position	points	driver_fullName	dob	nationality	constructor_name	grid_position	laps	status	time	fastest_LapRank	fastest_LapNumber	fastest_LapTime	average_Speed	ingestion_date
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	44	1	25	Lewis Hamilton	1985-01-07	British	Mercedes	2	56	Finished	1:32:03.897	4	44	1:34.015	207.235	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	33	2	18	Max Verstappen	1997-09-30	Dutch	Red Bull	1	56	Finished	+0.745	2	41	1:33.228	208.984	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	77	3	16	Valtteri Bottas	1989-08-28	Finnish	Mercedes	3	56	Finished	+37.383	1	56	1:32.090	211.566	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	4	4	12	Lando Norris	1999-11-13	British	McLaren	7	56	Finished	+46.466	6	38	1:34.396	206.398	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	11	5	10	Sergio Pérez	1990-01-26	Mexican	Red Bull	0	56	Finished	+52.047	3	44	1:33.970	207.334	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	16	6	8	Charles Leclerc	1997-10-16	Monegasque	Ferrari	4	56	Finished	+59.090	11	39	1:34.988	205.112	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	3	7	6	Daniel Ricciardo	1989-07-01	Australian	McLaren	6	56	Finished	+66.004	10	36	1:34.932	205.233	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	55	8	4	Carlos Sainz	1994-09-01	Spanish	Ferrari	8	56	Finished	+67.100	7	48	1:34.509	206.151	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	22	9	2	Yuki Tsunoda	2000-05-11	Japanese	AlphaTauri	13	56	Finished	+85.692	8	38	1:34.761	205.603	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	18	10	1	Lance Stroll	1998-10-29	Canadian	Aston Martin	10	56	Finished	+86.713	9	31	1:34.865	205.378	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	7	11	0	Kimi Räikkönen	1979-10-17	Finnish	Alfa Romeo	14	56	Finished	+88.864	14	45	1:35.192	204.672	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	99	12	0	Antonio Giovinazzi	1993-12-14	Italian	Alfa Romeo	12	55	+1 Lap	null	13	32	1:35.122	204.823	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	31	13	0	Esteban Ocon	1996-09-17	French	Alpine F1 Team	16	55	+1 Lap	null	15	33	1:35.250	204.548	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	63	14	0	George Russell	1998-02-15	British	Williams	15	55	+1 Lap	null	12	40	1:35.036	205.008	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	5	15	0	Sebastian Vettel	1987-07-03	German	Aston Martin	20	55	+1 Lap	null	16	26	1:35.566	203.871	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	47	16	0	Mick Schumacher	1999-03-22	German	Haas F1 Team	18	55	+1 Lap	null	18	38	1:36.134	202.667	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	10	17	0	Pierre Gasly	1996-02-07	French	AlphaTauri	5	52	Retired	null	5	48	1:34.090	207.069	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	6	18	0	Nicholas Latifi	1995-06-29	Canadian	Williams	17	51	Retired	null	19	16	1:36.602	201.685	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	14	19	0	Fernando Alonso	1981-07-29	Spanish	Alpine F1 Team	9	32	Brakes	null	17	31	1:36.063	202.816	2024-10-14T03:26:06.188+00:00
2021	1	Bahrain Grand Prix	2021-03-28	15:00:00Z	9	20	0	Nikita Mazepin	1999-03-02	Russian	Haas F1 Team	19	0	Accident	null	null	null	null	null	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	33	1	25	Max Verstappen	1997-09-30	Dutch	Red Bull	3	63	Finished	2:02:34.598	2	60	1:17.524	227.96	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	44	2	19	Lewis Hamilton	1985-01-07	British	Mercedes	1	63	Finished	+22.000	1	60	1:16.702	230.403	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	4	3	15	Lando Norris	1999-11-13	British	McLaren	7	63	Finished	+23.702	3	63	1:18.259	225.819	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	16	4	12	Charles Leclerc	1997-10-16	Monegasque	Ferrari	4	63	Finished	+25.579	6	60	1:18.379	225.473	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	55	5	10	Carlos Sainz	1994-09-01	Spanish	Ferrari	11	63	Finished	+27.036	7	60	1:18.490	225.154	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	3	6	8	Daniel Ricciardo	1989-07-01	Australian	McLaren	6	63	Finished	+51.220	12	54	1:19.341	222.739	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	10	7	6	Pierre Gasly	1996-02-07	French	AlphaTauri	5	63	Finished	+52.818	9	52	1:18.994	223.718	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	18	8	4	Lance Stroll	1998-10-29	Canadian	Aston Martin	10	63	Finished	+56.909	8	59	1:18.782	224.32	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	31	9	2	Esteban Ocon	1996-09-17	French	Alpine F1 Team	9	63	Finished	+65.704	15	62	1:19.422	222.512	2024-10-14T03:26:06.188+00:00
2021	2	Emilia Romagna Grand Prix	2021-04-18	13:00:00Z	14	10	1	Fernando Alonso	1981-07-29	Spanish	Alpine F1 Team	15	63	Finished	+66.561	14	62	1:19.417	222.526	2024-10-14T03:26:06.188+00:00
````



#### Incremental Load
````python
# results
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, concat_ws
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Define paths
results_path = '/mnt/dldatabricks/01-bronze/*/results.json'

# Read the JSON file into a DataFrame
df = spark.read.json(results_path, multiLine=True)

# Explode the nested Races array
races_df = df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Explode the nested Results array within each race
results_df = races_df.select(
    col("race.season").cast(IntegerType()).alias("season"),
    col("race.round").cast(IntegerType()).alias("round"),
    col("race.raceName").alias("raceName"),
    col("race.date").cast(StringType()).alias("raceDate"),
    col("race.time").alias("raceTime"),
    explode(col("race.Results")).alias("result")
)

# Extract required fields and create full name
results_bronze = results_df.select(
    col("season"),
    col("round"),
    col("raceName"),
    col("raceDate"),
    col("raceTime"),
    col("result.number").cast(IntegerType()).alias("driver_number"),
    col("result.position").cast(IntegerType()).alias("position"),
    col("result.points").cast(DoubleType()).alias("points"),
    concat_ws(" ", col("result.Driver.givenName"), col("result.Driver.familyName")).alias("driver_fullName"),  # Combine givenName and familyName
    col("result.Driver.dateOfBirth").alias("dob"),
    col("result.Driver.nationality").alias("nationality"),
    col("result.Constructor.name").alias("constructor_name"),
    col("result.grid").cast(IntegerType()).alias("grid_position"),
    col("result.laps").cast(IntegerType()).alias("laps"),
    col("result.status").alias("status"),
    col("result.Time.time").alias("time"),
    col("result.FastestLap.rank").cast(IntegerType()).alias("fastest_LapRank"),
    col("result.FastestLap.lap").cast(IntegerType()).alias("fastest_LapNumber"),
    col("result.FastestLap.Time.time").alias("fastest_LapTime"),
    col("result.FastestLap.AverageSpeed.speed").cast(DoubleType()).alias("average_Speed")
)

# Deduplicate the source data
results_bronze_dedup = results_bronze.dropDuplicates(["season", "round", "driver_fullName", "constructor_name"])

# Process the new DataFrame
results_bronze_new_processed = results_bronze_dedup \
    .withColumn("ingestion_date", current_timestamp()) \
    .select("season", "round", "raceName", "raceDate", "raceTime", "driver_number", "position", "points", "driver_fullName", "dob", "nationality", "constructor_name", "grid_position", "laps", "status", "time", "fastest_LapRank", "fastest_LapNumber", "fastest_LapTime", "average_Speed", "ingestion_date")

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/F1_Silver/results")

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        results_bronze_new_processed.alias("new"),
        "existing.season = new.season AND existing.round = new.round AND existing.driver_fullName = new.driver_fullName AND existing.constructor_name = new.constructor_name"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Read and display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/F1_Silver/results")
merged_data.display()
````
![image](https://github.com/user-attachments/assets/74899bac-f85f-4cdc-9bb0-2ed50e729c2c)

