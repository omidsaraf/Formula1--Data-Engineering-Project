### Full Load
````python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


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
drivers_bronze.write.format("delta").mode("overwrite").saveAsTable("F1_Silver.drivers")

# Display
drivers_silver=spark.read.format("delta").load("/mnt/dldatabricks/02-silver/F1_Silver/drivers")
drivers_silver.display()
````
![image](https://github.com/user-attachments/assets/a23c9a73-394a-4f34-81e7-8ba40b3d4f62)

![image](https://github.com/user-attachments/assets/59702c4f-3484-426e-afd2-17c07e5cf3cd)

![image](https://github.com/user-attachments/assets/7a4b1d8d-72cd-433f-9257-6cf95f4c2514)


### Incremental Load
````python

# drivers

from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode, concat_ws, current_timestamp, row_number
from pyspark.sql.window import Window

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/02-silver/drivers")

# Define the path to new incremental JSON file
incremental_path = '/mnt/dldatabricks/01-bronze/*/drivers.json'

# Read the new data from the JSON file into a DataFrame
incremental_df = spark.read.json(incremental_path, multiLine=True)

# Explode the nested Drivers array
drivers_df_new = incremental_df.select(explode(col("MRData.DriverTable.Drivers")).alias("driver"))

# Extract required fields and create full name
drivers_incremental = drivers_df_new.select(
    concat_ws(" ", col("driver.givenName"), col("driver.familyName")).alias("full_name"),  # Combine givenName and familyName
    col("driver.dateOfBirth").alias("dob"),
    col("driver.nationality").alias("nationality")
)

drivers_incremental = drivers_incremental.withColumn("ingestion_date", current_timestamp())

# Add a row number to ensure uniqueness
window_spec = Window.orderBy(col("ingestion_date").desc())
drivers_incremental = drivers_incremental.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the latest row for each driver
drivers_incremental = drivers_incremental.filter(col("row_num") == 1).drop("row_num")

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        drivers_incremental.alias("new"),
        "existing.full_name = new.full_name AND existing.dob = new.dob AND existing.nationality = new.nationality"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/02-silver/drivers")
merged_data.display()
````
![image](https://github.com/user-attachments/assets/705b6ac7-d5af-4559-b8cd-b58d4d6b0523)
![image](https://github.com/user-attachments/assets/c9817061-fddc-46b2-ae61-001e630ca094)


