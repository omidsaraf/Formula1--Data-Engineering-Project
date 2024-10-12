## Circuits Dimension Analysis

### SCD Type: 0 (Fixed)

**Circuits Dimension** is categorized under **SCD Type 0 (Fixed)** because the data within this table is static and does not change over time. Below are the columns and the reasoning:

### Columns:
- **circuitID**: string (Static, unique identifier for the circuit)
- **circuitName**: string (Typically static unless renamed, which is rare)
- **location**: string (Geographical information, static)
- **country**: string (Static)
- **lat**: double (Latitude, static geographical information)
- **lng**: double (Longitude, static geographical information)

### Reasoning:
- **Static Nature**: The data represents fixed geographical information and identifiers that do not change over time.
- **No History Tracking Required**: There is no need to track changes or maintain versions for the circuits data.
- **Simple ETL**: Using SCD Type 0 simplifies the ETL process as we can overwrite the data without worrying about historical changes.

### ETL Process for Circuits Data - Only Full Load:

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

