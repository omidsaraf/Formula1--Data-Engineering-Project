Full Load only as all possible new records has been added into circuit silver layer and only required overwrite data here

`````python
# Define the path
Path_Drivers = "/mnt/dldatabricks/02-silver/F1_Silver/drivers/"

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
Drivers = Modified_df.withColumnRenamed("full_name", "DriverName")\
    .withColumn("Age", (year(current_date()) - year(col("dob"))))\
    .select("DriverName", "Age", "nationality")\
    .orderBy("DriverName")

display(Drivers)


#Write to Gold Layer
Drivers.write.format("delta").mode("overwrite").saveAsTable("F1_Gold.drivers")
`````
![image](https://github.com/user-attachments/assets/bd20bdd3-6689-4800-b07a-83886e786065)

