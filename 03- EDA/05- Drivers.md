````python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read Drivers JSON") \
    .getOrCreate()

# Define the path to your JSON file
drivers_path = '/mnt/dldatabricks/01-bronze/*/drivers.json'

# Read the JSON file into a DataFrame
df = spark.read.json(drivers_path, multiLine=True)

# Explode the nested Drivers array
drivers_df = df.select(explode(col("MRData.DriverTable.Drivers")).alias("driver"))

# Extract required fields and create full name
drivers_bronze = drivers_df.select(
    col("driver.driverId").alias("driver_Id"),
    col("driver.givenName").alias("givenName"),
    col("driver.familyName").alias("familyName"),
    col("driver.dateOfBirth").alias("dob"),
    col("driver.nationality").alias("nationality"),
    col("driver.url").alias("url")
)

display(drivers_bronze)
`````
![image](https://github.com/user-attachments/assets/a34f971c-9adf-44c8-8e7e-dbb5e186c35b)

````python
drivers_schema=StructType([
    StructField("driver_Id", StringType(), True),
    StructField("givenName", StringType(), True),
    StructField("familyName", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
    ])
