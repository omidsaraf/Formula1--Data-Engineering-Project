
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

# Display the transformed DataFrame
#constructors_bronze.display()

# Write the DataFrame in Delta format to the destination
constructors_bronze.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/02-silver/constructors")
````

### Incremental Load
````python
