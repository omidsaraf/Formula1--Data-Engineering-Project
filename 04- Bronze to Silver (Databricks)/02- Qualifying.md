### Full Load

````python
from pyspark.sql.functions import col, explode, lit, monotonically_increasing_id, concat_ws

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read Qualifying JSON") \
    .getOrCreate()

# Define the path to your JSON file
qualifying_path = '/mnt/dldatabricks/01-bronze/*/qualifying.json'

# Read the JSON file into a DataFrame
df = spark.read.json(qualifying_path, multiLine=True)

# Explode the nested Races array
races_df = df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Explode the nested QualifyingResults array within each race
qualifying_df = races_df.select(
    col("race.Circuit.circuitId").alias("circuitId"),
    col("race.round").alias("round"),
    explode(col("race.QualifyingResults")).alias("qualifying")
)

# Extract required fields and generate unique IDs for qualifyId and raceId
qualifying_bronze = qualifying_df.select(
    monotonically_increasing_id().cast(IntegerType()).alias("qualify_Id"),  # Generate unique qualifyId
    col("qualifying.Constructor.constructorId").alias("constructor_Id"),
    col("qualifying.number").cast(IntegerType()).alias("number"),
    col("qualifying.position").cast(IntegerType()).alias("position"),
    col("qualifying.Q1").alias("q1"),
    col("qualifying.Q2").alias("q2"),
    col("qualifying.Q3").alias("q3"),
    concat_ws(" ", col("qualifying.Driver.givenName"), col("qualifying.Driver.familyName")).alias("DriverName"),  # Combine names as DriverName
    monotonically_increasing_id().cast(IntegerType()).alias("race_Id")  # Generate unique raceId
)

qualifying_bronze=qualifying_bronze.withColumn("ingestion_date", current_timestamp())

# Write the DataFrame in Delta format to the destination
qualifying_bronze.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/02-silver/qualifying")

qualifying_silver=spark.read.format("delta").load("/mnt/dldatabricks/02-silver/qualifying")
display(qualifying_silver)

````
![image](https://github.com/user-attachments/assets/4a1b9dcb-7f44-47d9-81b3-35f7359d8a0a)




### Incremantal Load
````python
#qualifying

from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/qualifying")

# Define the path to new incremental JSON file
incremental_path = '/mnt/dldatabricks/01-bronze/*/qualifying.json'

# Read the new data from the JSON file into a DataFrame
incremental_df = spark.read.json(incremental_path, multiLine=True)

# Explode the nested Races array
races_df_new = incremental_df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Explode the nested QualifyingResults array within each race
qualifying_df_new = races_df_new.select(
    col("race.Circuit.circuitId").alias("circuitId"),
    col("race.round").alias("round"),
    explode(col("race.QualifyingResults")).alias("qualifying")
)

# Extract required fields and generate unique IDs for qualifyId and raceId
qualifying_incremental = qualifying_df_new.select(
    monotonically_increasing_id().cast(IntegerType()).alias("qualify_Id"),  # Generate unique qualifyId
    col("qualifying.Constructor.constructorId").alias("constructor_Id"),
    col("qualifying.number").cast(IntegerType()).alias("number"),
    col("qualifying.position").cast(IntegerType()).alias("position"),
    col("qualifying.Q1").alias("q1"),
    col("qualifying.Q2").alias("q2"),
    col("qualifying.Q3").alias("q3"),
    concat_ws(" ", col("qualifying.Driver.givenName"), col("qualifying.Driver.familyName")).alias("DriverName"),  # Combine names as DriverName
    monotonically_increasing_id().cast(IntegerType()).alias("race_Id")  # Generate unique raceId
)

qualifying_incremental=qualifying_incremental.withColumn("ingestion_date", current_timestamp())

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        qualifying_incremental.alias("new"),
        "existing.qualify_Id = new.qualify_Id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/qualifying")
merged_data.display()

````

![image](https://github.com/user-attachments/assets/dfc7e5cf-528b-49ce-9f83-88eed9be4dcc)


![image](https://github.com/user-attachments/assets/7ee0e263-f463-4d0a-a737-96fae5c363df)


