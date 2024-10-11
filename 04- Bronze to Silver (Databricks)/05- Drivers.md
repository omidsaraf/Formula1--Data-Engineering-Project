### Full Load
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
    concat_ws(" ", col("driver.givenName"), col("driver.familyName")).alias("full_name"),  # Combine givenName and familyName  
    col("driver.driverId").alias("driver_Id"),
    col("driver.givenName").alias("givenName"),
    col("driver.familyName").alias("familyName"),
    col("driver.dateOfBirth").alias("dob"),
    col("driver.nationality").alias("nationality"),
    col("driver.url").alias("url"),
)

drivers_bronze= drivers_bronze.withColumn("ingestion_date", current_timestamp())\
    .drop("driver_Id","givenName","familyName")\
    .drop("url")


# Write the DataFrame in Delta format to the destination
drivers_bronze.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/02-silver/drivers")

# Display the transformed DataFrame
drivers_silver= spark.read.format("delta").load("/mnt/dldatabricks/02-silver/drivers")
display(drivers_silver)
````
![image](https://github.com/user-attachments/assets/092b3a06-7261-412f-91b8-17587db7f43a)
![image](https://github.com/user-attachments/assets/3f0b7c12-a1fa-4a7e-8050-4d871f4bbd1c)

### Full Load
````python


