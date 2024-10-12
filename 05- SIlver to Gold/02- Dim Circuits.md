

The circuits file has columns that are static and unchanging, like the circuit IDs and their geographical information, which means they fit into SCD Type 0 (Fixed).

Circuits Dimension:
CircuitID: Static, does not change.

CircuitName: Typically static unless there's a renaming which is rare.

Location: Static as it's geographical information.

Country: Static.

Lat: Static.

Lng: Static.

Ingestion_date: Metadata for when the data was loaded.

Given these characteristics, they fit best under SCD Type 0 as this data is not expected to change over time.

SCD Types Recap:
SCD Type 0 (Fixed): Data that does not change over time (e.g., circuit IDs and geographic data).

SCD Type 1 (Overwrite): Simple updates to data without maintaining history (e.g., updating circuit names).

SCD Type 2 (Versioning): Tracks historical changes using version numbers or effective dates (e.g., tracking changes in circuit names over time).

SCD Type 3 (Alternate Columns): Tracks historical changes using additional columns (e.g., keeping previous and current circuit names).


```python
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
Modified_df = nullif_df.withColumn("country", nullif(col("circuitID"), lit(""))) 

#Create surrogate key
window_spec = Window.orderBy("circuitID")
Modified_df = Modified_df.withColumn("circuit_sk", row_number().over(window_spec))

#Rename Columns, Reorder columns
Dim_Circuits = Modified_df \
    .withColumnRenamed("circuitID", "circuit_id") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("circuitName", "circuit_name")\
    .select("circuit_sk", "circuit_id", "circuit_name", "location","country","latitude", "longitude")


display(Dim_Circuits)
````
![image](https://github.com/user-attachments/assets/ab3852df-fde7-4046-b399-8a2986554f62)

````python
#Write to Gold Layer
Dim_Circuits.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/03-gold/Dim_Circuits")
````
![image](https://github.com/user-attachments/assets/92195396-b28b-4d28-8cca-56532a75815a)

