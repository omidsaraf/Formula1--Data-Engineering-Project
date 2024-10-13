## Circuits
````python
# Circuits

# Define the path to your Silver Layer data
Path_Circuits = "/mnt/dldatabricks/02-silver/circuits/"

# Read the Delta table into a DataFrame
circuits_df = spark.read.format("delta").load(Path_Circuits)
circuits_df = circuits_df.drop('ingestion_date')


# shows count of duplications
duplicates = circuits_df.count() - circuits_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handilging
circuits_df=circuits_df.dropDuplicates()

# shows count of nulls
nulls = circuits_df.select([count(when(col(c).isNull(), c)).alias(c) for c in circuits_df.columns]).toPandas()
print(f"nulls:{nulls}")


#Null Handling
nullif_df = circuits_df.withColumn("lat", nullif(col("lat"), lit(0)))
nullif_df = nullif_df.withColumn("lng", nullif(col("lng"), lit(0)))
nullif_df = nullif_df.withColumn("location", nullif(col("location"), lit("")))
nullif_df = nullif_df.withColumn("circuitName", nullif(col("circuitName"), lit("")))
nullif_df = nullif_df.withColumn("country", nullif(col("country"), lit(""))) 
Modified_df = nullif_df.withColumn("circuitID", nullif(("circuitID"), lit(0)))


#Rename Columns
Dim_Circuits = Modified_df \
    .withColumnRenamed("circuitID", "circuit_id") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("circuitName", "circuit_name")\
    .select("circuit_id", "circuit_name", "location","country","latitude", "longitude")


display(Dim_Circuits)
`````
![image](https://github.com/user-attachments/assets/e25a94a7-d25e-417e-8248-afc618f01f7b)

## Constructors
````python

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define the path to your Silver Layer data
Path_Constructors = "/mnt/dldatabricks/02-silver/constructors/"

# Read the Delta table into a DataFrame
Constructors_df = spark.read.format("delta").load(Path_Constructors)
Constructors_df = Constructors_df.drop('ingestion_date')


# shows count of duplications
duplicates = Constructors_df.count() - Constructors_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handilging
Constructors_df=Constructors_df.dropDuplicates()

# shows count of nulls
nulls = Constructors_df.select([count(when(col(c).isNull(), c)).alias(c) for c in Constructors_df.columns]).toPandas()
print(f"nulls:{nulls}")

#Null Handling
nullif_df = Constructors_df.withColumn("name", nullif(col("name"), lit("")))
Modified_df = nullif_df.withColumn("nationality", nullif(col("nationality"), lit("")))

#Rename Columns
Dim_Constructors = Modified_df \
    .withColumn("constructor_name", lower(col("name"))) \
    .select("constructor_name", "nationality").drop("name")

display(Dim_Constructors )
````
![image](https://github.com/user-attachments/assets/fd3c6892-1c28-4b0c-ac46-03dfa688f163)

## Drivers
````python
# Define the path to your Silver Layer data
Path_Drivers = "/mnt/dldatabricks/02-silver/drivers/"

# Read the Delta table into a DataFrame
Drivers_df = spark.read.format("delta").load(Path_Drivers)
Drivers_df = Drivers_df.drop('ingestion_date')


# shows count of duplications
duplicates = Drivers_df.count() - Drivers_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handilging
Drivers_df=Drivers_df.dropDuplicates()

# shows count of nulls
nulls = Drivers_df.select([count(when(col(c).isNull(), c)).alias(c) for c in Drivers_df.columns]).toPandas()
print(f"nulls:{nulls}")

#Null Handling
nullif_df = Drivers_df.withColumn("full_name", nullif(col("full_name"), lit("")))
nullif_df = Drivers_df.withColumn("dob", nullif(col("dob"), lit("")))
Modified_df = nullif_df.withColumn("nationality", nullif(col("nationality"), lit("")))

#Rename Columns
Dim_Drivers = Modified_df\
    .select("full_name", "dob", "nationality")

display(Dim_Drivers)
`````
![image](https://github.com/user-attachments/assets/e6b76641-bfe5-4a69-9cba-f7702f69f3f7)

