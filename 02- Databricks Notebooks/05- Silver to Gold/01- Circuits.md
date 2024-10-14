Full Load only as all possible new records has been added into circuit silver layer and only required overwrite data here

````python

# Define the path to your Silver Layer data
Path_Circuits = "/mnt/dldatabricks/02-silver/F1_Silver/circuits"

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


#Create surrogate key
##window_spec = Window.orderBy("circuitID")
##Modified_df = Modified_df.withColumn("circuit_sk", row_number().over(window_spec))

#Rename Columns
Circuits = Modified_df \
    .withColumnRenamed("circuitID", "circuit_id") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("circuitName", "circuit_name")\
    .select("circuit_id", "circuit_name", "location","country","latitude", "longitude")


display(Circuits)


#Write to Gold Layer
Circuits.write.format("delta").mode("overwrite").saveAsTable("F1_Gold.circuits")

````

![image](https://github.com/user-attachments/assets/74937d4b-e995-49ea-80a4-c97a489eef52)
