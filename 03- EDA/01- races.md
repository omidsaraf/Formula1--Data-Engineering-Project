````python
from pyspark.sql.functions import col, explode, lit, monotonically_increasing_id, regexp_replace
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

# Read the JSON file into a DataFrame
df = spark.read.json(races_path, multiLine=True)

# Explode the nested Races array
races_df = df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Extract required fields and add raceId
races_flat_df = races_df.select(
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
races_flat_df.show(truncate=False)
````
![image](https://github.com/user-attachments/assets/e6664192-2161-49b1-9d6d-9aa6b4d6a75b)

#### Schema
````python
races_schema=StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])
