
````python
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
constructors_bronze.display()
````
![image](https://github.com/user-attachments/assets/1eb9d52c-abd7-4ec4-b216-c1beac420757)


`````python
constructors_schema= StructType([
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
    ])
