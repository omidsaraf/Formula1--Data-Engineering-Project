### Transformations
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

races_silver.write.format("delta").mode("overwrite").saveAsTable("F1_Silver.Races")

`````
![image](https://github.com/user-attachments/assets/7e609ee6-7a93-4c9f-96b6-43b107e57fc6)

![image](https://github.com/user-attachments/assets/37495593-cfe4-41e3-bb3c-a3155800cd0c)

![image](https://github.com/user-attachments/assets/3ce7ff87-08b6-4402-a688-986c52c12a2e)

![image](https://github.com/user-attachments/assets/0803271b-ecd9-48de-bf70-38d36feba5d1)

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


