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
    col("circuit.circuitId").alias("circuit_ID"),
    col("circuit.circuitName").alias("circuit_Name"),
    col("circuit.Location.locality").alias("location"),
    col("circuit.Location.country").alias("country"),
    col("circuit.Location.lat").cast(DoubleType()).alias("lat"),
    col("circuit.Location.long").cast(DoubleType()).alias("lng"),
    col("circuit.url").alias("url")
)

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
circuits_silver = circuits_bronze.filter(col("row_num") == 1).drop("row_num")

# Write the DataFrame in Delta format to the destination
circuits_silver.write.format("delta").mode("append").saveAsTable("F1_Silver.circuits")
`````
![image](https://github.com/user-attachments/assets/96ad5b23-3712-4623-a8d3-ce87d274df07)

![image](https://github.com/user-attachments/assets/5af0df56-1277-4d5b-9c07-a53c37da5981)

![image](https://github.com/user-attachments/assets/6ee53504-221a-4245-8bbf-6b0bfdc70bf5)


### Incremental Load

````python
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Define paths
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

# Deduplicate the source data
circuits_bronze_dedup = circuits_bronze.dropDuplicates(["circuitID"])

# Process the new DataFrame
circuits_bronze_new_processed = circuits_bronze_dedup \
    .withColumn("ingestion_date", current_timestamp()) \
    .drop("url") \
    .select("circuitID", "circuitName", "location", "country", "lat", "lng", "ingestion_date")

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/F1_Silver/circuits")

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        circuits_bronze_new_processed.alias("new"),
        "existing.circuitID = new.circuitID AND existing.country = new.country"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Read and display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/F1_Silver/circuits")
merged_data.display()
````
![image](https://github.com/user-attachments/assets/2ab5af82-e25a-4212-b121-2ddfb4931e46)

![image](https://github.com/user-attachments/assets/8804e5e7-9e8e-4ff9-a78b-7eba4ba92e9c)

