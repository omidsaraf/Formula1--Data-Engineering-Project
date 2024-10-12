## Constructors Dimension Analysis

### SCD Type: 1 (Overwrite)

**Constructors Dimension** is best categorized under **SCD Type 1 (Overwrite)** since the data may occasionally require updates, such as changes in the constructor's name or nationality. However, historical changes do not need to be tracked over time.

### Columns:
- **name**: string (Constructor name, subject to occasional updates)
- **nationality**: string (Nationality of the constructor, subject to occasional updates)
- **url**: string (Reference URL, subject to occasional updates)
- **ingestion_date**: timestamp (Metadata for when the data was loaded)

### Reasoning:
- **Occasional Updates**: Constructor names, nationality, and URLs may need updates without maintaining a full history of changes.
- **Simplified ETL**: Using SCD Type 1 simplifies the ETL process as it overwrites the existing data with the latest updates.

### ETL Process for Constructors Data:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count, row_number
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read Constructors Data") \
    .getOrCreate()

# Define the path to your Silver Layer data
Path_Constructors = "/mnt/dldatabricks/02-silver/constructors/"

# Read the Delta table into a DataFrame
Constructors_df = spark.read.format("delta").load(Path_Constructors)
Constructors_df = Constructors_df.drop('ingestion_date')

# Shows count of duplications
duplicates = Constructors_df.count() - Constructors_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handling
Constructors_df = Constructors_df.dropDuplicates()

# Shows count of nulls
nulls = Constructors_df.select([count(when(col(c).isNull(), c)).alias(c) for c in Constructors_df.columns]).toPandas()
print(f"Nulls: {nulls}")

# Null Handling
nullif_df = Constructors_df.withColumn("name", when(col("name") != "", col("name")).otherwise(None))
nullif_df = nullif_df.withColumn("nationality", when(col("nationality") != "", col("nationality")).otherwise(None))

# Create surrogate key with identity key and order by name
window_spec = Window.orderBy("name")
Modified_df = nullif_df.withColumn("constructor_sk", row_number().over(window_spec))

# Rename Columns
Dim_Constructors = Modified_df \
    .withColumnRenamed("name", "constructor_name") \
    .select("constructor_sk", "constructor_name", "nationality")

# Display the final DataFrame
Dim_Constructors.show(truncate=False)

# Write to Gold Layer
Dim_Constructors.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/03-gold/Dim_Constructors")


````python
# Constructors

# Define the path to your Silver Layer data
Path_Constructors = "/mnt/dldatabricks/02-silver/constructors/"

# Read the Delta table into a DataFrame
Constructors_df = spark.read.format("delta").load(Path_Constructors)
Constructors_df = Constructors_df.drop('ingestion_date')


# shows count of duplications
duplicates = Constructors_df.count() - Constructors_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handilging
Constructors_df_df=Constructors_df_df.dropDuplicates()

# shows count of nulls
nulls = Constructors_df.select([count(when(col(c).isNull(), c)).alias(c) for c in Constructors_df.columns]).toPandas()
print(f"nulls:{nulls}")

#Null Handling
nullif_df = Constructors_df.withColumn("name", nullif(col("name"), lit("")))
Modified_df = nullif_df.withColumn("nationality", nullif(col("nationality"), lit("")))

#Create surrogate key
window_spec = Window.orderBy("name")
Modified_df = Modified_df.withColumn("constructor_sk", row_number().over(window_spec))

#Rename Columns
Dim_Constructors = Modified_df \
    .withColumnRenamed("name", "constructor_name")\
    .select("constructor_sk", "constructor_name", "nationality")


display(Dim_Constructors )


#Write to Gold Layer
Dim_Constructors .write.format("delta").mode("overwrite").save("/mnt/dldatabricks/03-gold/Dim_Constructors")
`````
![image](https://github.com/user-attachments/assets/6d641ad6-a8ff-42eb-955d-c119429abb3a)
![image](https://github.com/user-attachments/assets/25d3e3d7-47fc-41a4-8014-27ab239e3a4c)

