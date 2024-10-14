#### Initial Load
````python

### Write Data as Delta Table (Initial Load)
````python
from pyspark.sql.functions import concat, col, lit, to_timestamp, current_timestamp

# races table
races_path='/mnt/dldatabricks/01-bronze/*/races.json'

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

# Read the JSON file into a DataFrame
df = spark.read.json(races_path, multiLine=True)

# Explode the nested Races array
races_df = df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Extract required fields and add raceId
races_bronze = races_df.select(
    monotonically_increasing_id().cast(IntegerType()).alias("raceId"),  # Generate unique raceId
    col("race.season").cast(IntegerType()).alias("year"),
    col("race.round").cast(IntegerType()).alias("round"),
    col("race.Circuit.circuitId").alias("circuitId"),
    col("race.raceName").alias("name"),
    col("race.date").cast(DateType()).alias("date"),
    regexp_replace(col("race.time"), "Z$", "").alias("time"),  # Remove 'Z' at the end of time
    col("race.url").alias("url")
)

# Display the transformed DataFrame
#races_bronze.display()
# Process the DataFrame
races_silver = races_bronze \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .drop('url', 'date', 'time') \
    .withColumn("ingestion_date", current_timestamp()) \
    .select('race_id', 'race_year', 'name', 'round', 'circuit_id', 'race_timestamp', 'ingestion_date')

# Write the DataFrame in Delta format to the destination

races_silver.write.format("delta").mode("append").saveAsTable("F1_Silver.Races")

`````
![image](https://github.com/user-attachments/assets/7e609ee6-7a93-4c9f-96b6-43b107e57fc6)

![image](https://github.com/user-attachments/assets/37495593-cfe4-41e3-bb3c-a3155800cd0c)

![image](https://github.com/user-attachments/assets/3ce7ff87-08b6-4402-a688-986c52c12a2e)

![image](https://github.com/user-attachments/assets/0803271b-ecd9-48de-bf70-38d36feba5d1)

### Incremental load (**seperated notebook for ADF Pipeline**)- Incremental Load
````python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Define paths
races_path = '/mnt/dldatabricks/01-bronze/*/races.json'

# Read the JSON file into a DataFrame
df = spark.read.json(races_path, multiLine=True)

# Explode the nested Races array
races_df = df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Extract required fields and add raceId
races_bronze = races_df.select(
    monotonically_increasing_id().cast(IntegerType()).alias("raceId"),  # Generate unique raceId
    col("race.season").cast(IntegerType()).alias("year"),
    col("race.round").cast(IntegerType()).alias("round"),
    col("race.Circuit.circuitId").alias("circuitId"),
    col("race.raceName").alias("name"),
    col("race.date").cast(DateType()).alias("date"),
    regexp_replace(col("race.time"), "Z$", "").alias("time"),  # Remove 'Z' at the end of time
    col("race.url").alias("url")
)

# Deduplicate the source data
races_bronze_dedup = races_bronze.dropDuplicates(["raceId"])

# Process the new DataFrame
races_bronze_new_processed = races_bronze_dedup \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .drop('url', 'date', 'time') \
    .withColumn("ingestion_date", current_timestamp()) \
    .select('race_id', 'race_year', 'name', 'round', 'circuit_id', 'race_timestamp', 'ingestion_date')

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/F1_Silver/races")

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        races_bronze_new_processed.alias("new"),
        "existing.race_year = new.race_year AND existing.round = new.round AND existing.circuit_id = new.circuit_id AND existing.name = new.name"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Read and display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/F1_Silver/races")
merged_data.display()

````
![image](https://github.com/user-attachments/assets/9708696b-d026-4a59-b2a5-0aeb1a3ba2f3)

````
race_id	race_year	name	round	circuit_id	race_timestamp	ingestion_date
0	2022	Bahrain Grand Prix	1	bahrain	2022-03-20T15:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
1	2022	Saudi Arabian Grand Prix	2	jeddah	2022-03-27T17:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
2	2022	Australian Grand Prix	3	albert_park	2022-04-10T05:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
3	2022	Emilia Romagna Grand Prix	4	imola	2022-04-24T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
4	2022	Miami Grand Prix	5	miami	2022-05-08T19:30:00.000+00:00	2024-10-14T05:37:04.627+00:00
5	2022	Spanish Grand Prix	6	catalunya	2022-05-22T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
6	2022	Monaco Grand Prix	7	monaco	2022-05-29T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
7	2022	Azerbaijan Grand Prix	8	baku	2022-06-12T11:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
8	2022	Canadian Grand Prix	9	villeneuve	2022-06-19T18:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
9	2022	British Grand Prix	10	silverstone	2022-07-03T14:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
10	2022	Austrian Grand Prix	11	red_bull_ring	2022-07-10T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
11	2022	French Grand Prix	12	ricard	2022-07-24T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
12	2022	Hungarian Grand Prix	13	hungaroring	2022-07-31T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
13	2022	Belgian Grand Prix	14	spa	2022-08-28T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
14	2022	Dutch Grand Prix	15	zandvoort	2022-09-04T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
15	2022	Italian Grand Prix	16	monza	2022-09-11T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
16	2022	Singapore Grand Prix	17	marina_bay	2022-10-02T12:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
17	2022	Japanese Grand Prix	18	suzuka	2022-10-09T05:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
18	2022	United States Grand Prix	19	americas	2022-10-23T19:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
19	2022	Mexico City Grand Prix	20	rodriguez	2022-10-30T20:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
20	2022	São Paulo Grand Prix	21	interlagos	2022-11-13T18:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
21	2022	Abu Dhabi Grand Prix	22	yas_marina	2022-11-20T13:00:00.000+00:00	2024-10-14T05:37:04.627+00:00
0	2021	Bahrain Grand Prix	1	bahrain	2021-03-28T15:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
1	2021	Emilia Romagna Grand Prix	2	imola	2021-04-18T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
2	2021	Portuguese Grand Prix	3	portimao	2021-05-02T14:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
3	2021	Spanish Grand Prix	4	catalunya	2021-05-09T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
4	2021	Monaco Grand Prix	5	monaco	2021-05-23T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
5	2021	Azerbaijan Grand Prix	6	baku	2021-06-06T12:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
6	2021	French Grand Prix	7	ricard	2021-06-20T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
7	2021	Styrian Grand Prix	8	red_bull_ring	2021-06-27T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
8	2021	Austrian Grand Prix	9	red_bull_ring	2021-07-04T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
9	2021	British Grand Prix	10	silverstone	2021-07-18T14:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
10	2021	Hungarian Grand Prix	11	hungaroring	2021-08-01T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
11	2021	Belgian Grand Prix	12	spa	2021-08-29T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
12	2021	Dutch Grand Prix	13	zandvoort	2021-09-05T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
13	2021	Italian Grand Prix	14	monza	2021-09-12T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
14	2021	Russian Grand Prix	15	sochi	2021-09-26T12:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
15	2021	Turkish Grand Prix	16	istanbul	2021-10-10T12:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
16	2021	United States Grand Prix	17	americas	2021-10-24T19:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
17	2021	Mexico City Grand Prix	18	rodriguez	2021-11-07T19:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
18	2021	São Paulo Grand Prix	19	interlagos	2021-11-14T17:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
19	2021	Qatar Grand Prix	20	losail	2021-11-21T14:00:00.000+00:00	2024-10-14T05:15:06.568+00:00
20	2021	Saudi Arabian Grand Prix	21	jeddah	2021-12-05T17:30:00.000+00:00	2024-10-14T05:15:06.568+00:00
21	2021	Abu Dhabi Grand Prix	22	yas_marina	2021-12-12T13:00:00.000+00:00	2024-10-14T05:15:06.568+00:00

