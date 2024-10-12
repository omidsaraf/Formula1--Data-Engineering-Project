## Drivers Dimension Analysis

### SCD Type: 1 (Overwrite)

**Drivers Dimension** is best categorized under **SCD Type 1 (Overwrite)** since the data may occasionally require updates, such as changes in the driver's details like name or nationality. However, historical changes do not need to be tracked over time.

### Columns:
- **full_name**: string (Driver's full name, subject to occasional updates)
- **dob**: string (Driver's date of birth, generally static)
- **nationality**: string (Driver's nationality, subject to occasional updates)

### Reasoning:
- **Occasional Updates**: Driver's name and nationality may need updates without maintaining a full history of changes.
- **Simplified ETL**: Using SCD Type 1 simplifies the ETL process as it overwrites the existing data with the latest updates.

### ETL Process for Drivers Data - Full Load

````python
# Drivers

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

#Create surrogate key
window_spec = Window.orderBy("dob")
Modified_df = Modified_df.withColumn("driver_sk", row_number().over(window_spec))

#Rename Columns
Dim_Drivers = Modified_df\
    .select("driver_sk", "full_name", "dob", "nationality")

display(Dim_Drivers)


#Write to Gold Layer
Dim_Drivers.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/03-gold/Dim_Drivers")
![image](https://github.com/user-attachments/assets/11ee96e6-84a0-4421-bf2d-d7b4bb373796)

![image](https://github.com/user-attachments/assets/e9c6b3e9-d8cf-45dd-92f2-2ec41a1163f4)

