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
    col("race.season").alias("season"),
    col("race.round").alias("round"),
    col("race.raceName").alias("raceName"),
    col("race.date").alias("raceDate"),
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
    col("result.number").alias("driver_number"),
    col("result.position").alias("position"),
    col("result.points").cast(DoubleType()).alias("points"),
    concat_ws(" ", col("result.Driver.givenName"), col("result.Driver.familyName")).alias("driver_FullName"),  # Combine givenName and familyName
    col("result.Driver.dateOfBirth").alias("dob"),
    col("result.Driver.nationality").alias("nationality"),
    col("result.Constructor.name").alias("constructor_name"),
    col("result.grid").alias("grid_Position"),
    col("result.laps").alias("laps"),
    col("result.status").alias("status"),
    col("result.Time.time").alias("time"),
    col("result.FastestLap.rank").alias("fastestLapRank"),
    col("result.FastestLap.lap").alias("fastestLapNumber"),
    col("result.FastestLap.Time.time").alias("fastestLapTime"),
    col("result.FastestLap.AverageSpeed.speed").cast(DoubleType()).alias("averageSpeed")
)

display(results_bronze)
````
````pandas
season	round	raceName	raceDate	raceTime	driver_number	position	points	driver_FullName	dob	nationality	constructor_name	grid_Position	laps	status	time	fastestLapRank	fastestLapNumber	fastestLapTime	averageSpeed	
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	1	1	26	Max Verstappen	1997-09-30	Dutch	Red Bull	1	57	Finished	1:31:44.742	1	39	1:32.608	210.383	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	11	2	18	Sergio Pérez	1990-01-26	Mexican	Red Bull	5	57	Finished	+22.457	4	40	1:34.364	206.468	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	55	3	15	Carlos Sainz	1994-09-01	Spanish	Ferrari	4	57	Finished	+25.110	6	44	1:34.507	206.156	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	16	4	12	Charles Leclerc	1997-10-16	Monegasque	Ferrari	2	57	Finished	+39.669	2	36	1:34.090	207.069	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	63	5	10	George Russell	1998-02-15	British	Mercedes	3	57	Finished	+46.788	12	40	1:35.065	204.946	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	4	6	8	Lando Norris	1999-11-13	British	McLaren	7	57	Finished	+48.458	5	1	1:34.476	206.223	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	44	7	6	Lewis Hamilton	1985-01-07	British	Mercedes	9	57	Finished	+50.324	7	39	1:34.722	205.688	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	81	8	4	Oscar Piastri	2001-04-06	Australian	McLaren	8	57	Finished	+56.082	11	1	1:34.983	205.123	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	14	9	2	Fernando Alonso	1981-07-29	Spanish	Aston Martin	6	57	Finished	+1:14.887	3	48	1:34.199	206.83	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	18	10	1	Lance Stroll	1998-10-29	Canadian	Aston Martin	12	57	Finished	+1:33.216	16	30	1:35.632	203.73	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	24	11	0	Guanyu Zhou	1999-05-30	Chinese	Sauber	17	56	+1 Lap	null	14	30	1:35.458	204.102	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	20	12	0	Kevin Magnussen	1992-10-05	Danish	Haas F1 Team	15	56	+1 Lap	null	15	34	1:35.570	203.863	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	3	13	0	Daniel Ricciardo	1989-07-01	Australian	RB F1 Team	14	56	+1 Lap	null	13	37	1:35.163	204.735	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	22	14	0	Yuki Tsunoda	2000-05-11	Japanese	RB F1 Team	11	56	+1 Lap	null	18	37	1:35.833	203.303	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	23	15	0	Alexander Albon	1996-03-23	Thai	Williams	13	56	+1 Lap	null	17	40	1:35.723	203.537	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	27	16	0	Nico Hülkenberg	1987-08-19	German	Haas F1 Team	10	56	+1 Lap	null	10	46	1:34.834	205.445	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	31	17	0	Esteban Ocon	1996-09-17	French	Alpine F1 Team	19	56	+1 Lap	null	20	34	1:36.226	202.473	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	10	18	0	Pierre Gasly	1996-02-07	French	Alpine F1 Team	20	56	+1 Lap	null	9	45	1:34.805	205.508	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	77	19	0	Valtteri Bottas	1989-08-28	Finnish	Sauber	16	56	+1 Lap	null	19	33	1:36.202	202.523	2024-10-11T01:53:00.716+00:00
2024	1	Bahrain Grand Prix	2024-03-02	15:00:00Z	2	20	0	Logan Sargeant	2000-12-31	American	Williams	18	55	+2 Laps	null	8	42	1:34.735	205.659	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	1	1	25	Max Verstappen	1997-09-30	Dutch	Red Bull	1	50	Finished	1:20:43.273	3	50	1:31.773	242.188	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	11	2	18	Sergio Pérez	1990-01-26	Mexican	Red Bull	3	50	Finished	+13.6431	8	37	1:32.273	240.876	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	16	3	16	Charles Leclerc	1997-10-16	Monegasque	Ferrari	2	50	Finished	+18.639	1	50	1:31.632	242.561	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	81	4	12	Oscar Piastri	2001-04-06	Australian	McLaren	5	50	Finished	+32.007	10	1	1:32.310	240.779	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	14	5	10	Fernando Alonso	1981-07-29	Spanish	Aston Martin	4	50	Finished	+35.759	13	43	1:32.387	240.579	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	63	6	8	George Russell	1998-02-15	British	Mercedes	7	50	Finished	+39.936	7	42	1:32.254	240.926	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	38	7	6	Oliver Bearman	2005-05-08	British	Ferrari	11	50	Finished	+42.679	5	50	1:32.186	241.103	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	4	8	4	Lando Norris	1999-11-13	British	McLaren	6	50	Finished	+45.708	4	1	1:31.944	241.738	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	44	9	2	Lewis Hamilton	1985-01-07	British	Mercedes	8	50	Finished	+47.391	2	38	1:31.746	242.26	2024-10-11T01:53:00.716+00:00
2024	2	Saudi Arabian Grand Prix	2024-03-09	17:00:00Z	27	10	1	Nico Hülkenberg	1987-08-19	German	Haas F1 Team	15	50	Finished	+1:16.996	12	49	1:32.366	240.633	2024-10-11T01:53:00.716+00:00
