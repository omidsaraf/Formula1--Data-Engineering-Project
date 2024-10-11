````PYTHON
from pyspark.sql.functions import explode, col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

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
# Display the transformed DataFrame
circuits_bronze.display()

`````
![image](https://github.com/user-attachments/assets/70f1fae6-4804-4214-a25d-53f91b521886)
