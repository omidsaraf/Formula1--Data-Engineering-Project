

````python
# Races (Snowfalke with Dim_Circuit)

# Define paths
Path_Races = "/mnt/dldatabricks/02-silver/F1_Silver/races/"

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
                      
Races = races_df.orderBy("race_id")

  
# Display the final DataFrame
display(Races)

#Write to Gold Layer (Partitioning by race_year)
Races.write.format("delta").partitionBy("race_year").mode("append").saveAsTable("F1_Gold.races")
`````
![image](https://github.com/user-attachments/assets/066a295b-89aa-4b85-b977-2ac2665ab25d)
