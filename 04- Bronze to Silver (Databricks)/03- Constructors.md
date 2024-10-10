
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
#constructors_bronze.display()

# Write the DataFrame in Delta format to the destination
constructors_bronze.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/02-silver/constructors")
````
![image](https://github.com/user-attachments/assets/a62915e0-5959-4942-a465-dc244ed8faba)

### Incremental Load
````python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode, current_timestamp, row_number
from pyspark.sql.window import Window

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/constructors")

# Define the path to new incremental JSON file
incremental_path = '/mnt/dldatabricks/01-bronze/*/constructors.json'

# Read the new data from the JSON file into a DataFrame
incremental_df = spark.read.json(incremental_path, multiLine=True)

# Explode the nested Constructors array
constructors_df_new = incremental_df.select(explode(col("MRData.ConstructorTable.Constructors")).alias("constructor"))

# Extract required fields
constructors_incremental = constructors_df_new.select(
    col("constructor.name").alias("name"),
    col("constructor.nationality").alias("nationality"),
    col("constructor.url").alias("url")
)
constructors_incremental = constructors_incremental.withColumn("ingestion_date", current_timestamp())

# Add a row number to each row based on url (or any other column if available)
window_spec = Window.partitionBy("url").orderBy(col("ingestion_date").desc())
constructors_incremental = constructors_incremental.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the latest row for each url
constructors_incremental = constructors_incremental.filter(col("row_num") == 1).drop("row_num")

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        constructors_incremental.alias("new"),
        "existing.url = new.url"  # Adjust this condition as needed
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/constructors")
merged_data.display()
````
![image](https://github.com/user-attachments/assets/a7fb82bd-b47e-420d-96bc-e70883672ab0)
