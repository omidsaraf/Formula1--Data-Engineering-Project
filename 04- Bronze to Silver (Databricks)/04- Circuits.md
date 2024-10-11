### Full Load
````python
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the path to your JSON file
circuits_path = '/mnt/dldatabricks/01-bronze/*/circuits.json'

# Read the JSON file into a DataFrame
df = spark.read.json(circuits_path, multiLine=True)

# Explode the nested Circuits array
circuits_df = df.select(explode(col("MRData.CircuitTable.Circuits")).alias("circuit"))

# Extract required fields
circuits_bronze = circuits_df.select(
    col("circuit.circuitId").alias("circuitID"),
    col("circuit.circuitName").alias("circuitName"),
    col("circuit.Location.locality").alias("location"),
    col("circuit.Location.country").alias("country"),
    col("circuit.Location.lat").cast(DoubleType()).alias("lat"),
    col("circuit.Location.long").cast(DoubleType()).alias("lng"),
    col("circuit.url").alias("url")
)
circuits_bronze = circuits_bronze.withColumn("ingestion_date", current_timestamp()).drop("url")

# Add a row number to ensure uniqueness
window_spec = Window.partitionBy("circuitID").orderBy(col("ingestion_date").desc())
circuits_bronze = circuits_bronze.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the latest row for each circuitID
circuits_bronze = circuits_bronze.filter(col("row_num") == 1).drop("row_num")

# Write the DataFrame in Delta format to the destination
circuits_bronze.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/02-silver/circuits")

# Display the transformed DataFrame
circuits_silver=spark.read.format("delta").load("/mnt/dldatabricks/02-silver/circuits")
circuits_silver.display()
`````
![image](https://github.com/user-attachments/assets/eaf92c0e-be9f-4869-9656-fbf955cbd861)

![image](https://github.com/user-attachments/assets/30103555-86ae-4f82-bccf-289541b1a883)

### Incremental Load

````python
# circuits

from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode, current_timestamp, row_number
from pyspark.sql.window import Window

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/circuits")

# Define the path to new incremental JSON file
incremental_path = '/mnt/dldatabricks/01-bronze/*/circuits.json'

# Read the new data from the JSON file into a DataFrame
incremental_df = spark.read.json(incremental_path, multiLine=True)

# Explode the nested Circuits array
circuits_df_new = incremental_df.select(explode(col("MRData.CircuitTable.Circuits")).alias("circuit"))

# Extract required fields
circuits_incremental = circuits_df_new.select(
    col("circuit.circuitId").alias("circuitID"),
    col("circuit.circuitName").alias("circuitName"),
    col("circuit.Location.locality").alias("location"),
    col("circuit.Location.country").alias("country"),
    col("circuit.Location.lat").cast(DoubleType()).alias("lat"),
    col("circuit.Location.long").cast(DoubleType()).alias("lng"),
    lit(None).cast(IntegerType()).alias("alt"),  # Altitude placeholder
    col("circuit.url").alias("url")
)
circuits_incremental = circuits_incremental.withColumn("ingestion_date", current_timestamp())

# Add a row number to each row to ensure uniqueness
window_spec = Window.partitionBy("circuitID").orderBy(col("ingestion_date").desc())
circuits_incremental = circuits_incremental.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the latest row for each circuitID
circuits_incremental = circuits_incremental.filter(col("row_num") == 1).drop("row_num")

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        circuits_incremental.alias("new"),
        "existing.circuitID = new.circuitID"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/circuits")
merged_data.display()
````
![image](https://github.com/user-attachments/assets/c0a45ed4-33f7-41a0-9e92-e5a99657c771)

![image](https://github.com/user-attachments/assets/1fe98648-9cf1-414b-aa76-cdb632836c45)
