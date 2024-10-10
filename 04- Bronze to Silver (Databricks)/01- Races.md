### Transformations
````python
races = races_bronze\
        .withColumnRenamed('year', 'race_year') \
        .withColumnRenamed('circuitId', 'circuit_id') \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
        .drop('url', 'date', 'time') \
        .withColumn("ingestion_date", current_timestamp()) \
        .select('race_id', 'race_year', 'name', 'round', 'circuit_id','race_timestamp', 'ingestion_date')
display(races)
````
![image](https://github.com/user-attachments/assets/dc72043e-45e0-4978-bf08-e04e30ac1502)

### Write Data as Delta Table (Initial Load)
````python
from pyspark.sql.functions import concat, col, lit, to_timestamp, current_timestamp

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
races_silver.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/02-silver/races")
`````
![image](https://github.com/user-attachments/assets/f534704b-c14b-46d0-8c3e-1a5b73b23632)


### Incremental load (**seperated notebook for ADF Pipeline**)- Incremental Load

#### 1- Read new upcoming file from Bronze Layer
````python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable


# Load the new data from the bronze layer
races_bronze_new = spark.read.json("/mnt/dldatabricks/01-bronze/*/races.json", multiLine=True)

# Explode the nested Races array
races_bronze_new = races_bronze_new.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Extract required fields and add raceId
races_bronze_new = races_bronze_new.select(
    monotonically_increasing_id().cast(IntegerType()).alias("raceId"),  # Generate unique raceId
    col("race.season").cast(IntegerType()).alias("year"),
    col("race.round").cast(IntegerType()).alias("round"),
    col("race.Circuit.circuitId").alias("circuitId"),
    col("race.raceName").alias("name"),
    col("race.date").cast(DateType()).alias("date"),
    regexp_replace(col("race.time"), "Z$", "").alias("time"),  # Remove 'Z' at the end of time
    col("race.url").alias("url"))

# Process the new DataFrame
races_bronze_new_processed = races_bronze_new \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .drop('url', 'date', 'time') \
    .withColumn("ingestion_date", current_timestamp()) \
    .select('race_id', 'race_year', 'name', 'round', 'circuit_id', 'race_timestamp', 'ingestion_date')
#races_bronze_new_processed.display()
`````
#### 2- Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/races")
````python
# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        races_bronze_new_processed.alias("new"),
        "existing.race_id = new.race_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Read and display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/races")
merged_data.display()
````
![image](https://github.com/user-attachments/assets/74458df3-9481-4c7f-a813-73fb16432ad5)

![image](https://github.com/user-attachments/assets/7674625f-6209-4892-823b-a350858a569a)


