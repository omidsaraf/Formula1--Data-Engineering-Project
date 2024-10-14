
### Full Load
```python
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the path to your JSON file
constructors_path = '/mnt/dldatabricks/01-bronze/*/constructors.json'

# Read the JSON file into a DataFrame
df = spark.read.json(constructors_path, multiLine=True)

# Explode the nested Constructors array
constructors_df = df.select(explode(col("MRData.ConstructorTable.Constructors")).alias("constructor"))

# Extract required fields
constructors_bronze = constructors_df.select(
    col("constructor.name").alias("name"),
    col("constructor.nationality").alias("nationality"),
    col("constructor.url").alias("url")
)
constructors_bronze= constructors_bronze.withColumn("ingestion_date", current_timestamp())


# Add a row number to ensure uniqueness
window_spec = Window.partitionBy("url").orderBy(col("ingestion_date").desc())
constructors_bronze = constructors_bronze.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the latest row for each url
constructors_bronze = constructors_bronze.filter(col("row_num") == 1).drop("row_num")


# Display the transformed DataFrame
constructors_bronze.display()

# Write the DataFrame in Delta format to the destination
constructors_bronze.write.format("delta").mode("append").saveAsTable("F1_Silver.constructors")

constructors_silver=spark.read.format("delta").load("/mnt/dldatabricks/02-silver/F1_Silver/constructors")
constructors_silver.display()

````
![image](https://github.com/user-attachments/assets/f0f08796-923f-433c-a929-f36f2277e758)

![image](https://github.com/user-attachments/assets/8a5f32d0-9207-40fd-be45-e48ec4d9a7df)

![image](https://github.com/user-attachments/assets/1cfca5d3-dd7a-4fb5-bace-59dd809a5c8f)


### Incremental Load
````python
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Define paths
constructors_path = '/mnt/dldatabricks/01-bronze/*/constructors.json'

# Read the JSON file into a DataFrame
df = spark.read.json(constructors_path, multiLine=True)

# Explode the nested Constructors array
constructors_df = df.select(explode(col("MRData.ConstructorTable.Constructors")).alias("constructor"))

# Extract required fields
constructors_bronze = constructors_df.select(
    col("constructor.name").alias("name"),
    col("constructor.nationality").alias("nationality"),
    col("constructor.url").alias("url")
)

# Deduplicate the source data
constructors_bronze_dedup = constructors_bronze.dropDuplicates(["url"])

# Process the new DataFrame
constructors_bronze_new_processed = constructors_bronze_dedup \
    .withColumn("ingestion_date", current_timestamp())

# Display the processed DataFrame
constructors_bronze_new_processed.show(truncate=False)

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/F1_Silver/constructors")

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        constructors_bronze_new_processed.alias("new"),
        "existing.url = new.url"
    ) \
    .whenMatchedUpdate(set={
        "name": "new.name",
        "nationality": "new.nationality",
        "ingestion_date": "new.ingestion_date"
    }) \
    .whenNotMatchedInsert(values={
        "name": "new.name",
        "nationality": "new.nationality",
        "url": "new.url",
        "ingestion_date": "new.ingestion_date"
    }) \
    .execute()

# Read and display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/F1_Silver/constructors")

merged_data.display()
````
![image](https://github.com/user-attachments/assets/a7fb82bd-b47e-420d-96bc-e70883672ab0)
