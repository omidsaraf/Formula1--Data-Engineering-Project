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
# Define the path
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

#Create surrogate key
# window_spec = Window.orderBy("dob")
#Modified_df = Modified_df.withColumn("driver_sk", row_number().over(window_spec))

#Rename Columns
Drivers = Modified_df.withColumnRenamed("full_name", "driver_name")\
    .withColumn("Age", (year(current_date()) - year(col("dob"))))\
    .select("driver_name", "Age", "nationality","dob")\
    .orderBy("driver_name")

display(Drivers)
`````
![image](https://github.com/user-attachments/assets/2d6f695f-e09f-44b9-881b-4b44542d2375)

## Races
````python
(Snowfalke with Dim_Circuit)

# Define paths
Path_Races = "/mnt/dldatabricks/02-silver/races/"

# Read the Delta table into a DataFrame
races_df = spark.read.format("delta").load(Path_Races)
races_df = races_df.drop('ingestion_date')

# Show count of duplications
duplicates = races_df.count() - races_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handling
races_df = races_df.dropDuplicates()

# shows count of nulls
nulls = races_df.select([count(when(col(c).isNull(), c)).alias(c) for c in races_df.columns]).toPandas()
print(f"nulls:{nulls}")


#Null Handling
nullif_df = races_df.withColumn("race_id", nullif(col("race_id"), lit("")))
nullif_df = nullif_df.withColumn("race_year", nullif(col("race_year"), lit("")))
nullif_df = nullif_df.withColumn("name", nullif(col("name"), lit("")))
nullif_df = nullif_df.withColumn("round", nullif(col("round"), lit("")))
nullif_df = nullif_df.withColumn("circuit_id", nullif(col("circuit_id"), lit("")))
nullif_df = nullif_df.withColumn("race_timestamp", nullif(col("race_timestamp"), lit("")))

# Rename Columns and correct the chaining of transformations
races_df = nullif_df.withColumn("race_date", to_date(col("race_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))\
                    .withColumn("race_time", to_timestamp(col("race_timestamp"), "HH:mm:ss.SSS'Z'"))\
                    .drop("race_timestamp")\
                    .withColumnRenamed("name", "race_name")\
                    .select("race_id", "race_year", "race_name", "round", "circuit_id", "race_date", "race_time")\
                      
races_df = races_df.orderBy("race_id")

  
# Display the final DataFrame
display(races_df)
````
![image](https://github.com/user-attachments/assets/dc8090b6-cf6a-438e-b237-0fa802b97778)

##
`````python

