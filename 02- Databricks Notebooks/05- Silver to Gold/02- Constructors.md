Full Load only as all possible new records has been added into circuit silver layer and only required overwrite data here
````python
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define the path to your Silver Layer data
Path_Constructors = "/mnt/dldatabricks/02-silver/F1_Silver/constructors/"

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

#Create surrogate key
#window_spec = Window.orderBy("name")
#Modified_df = Modified_df.withColumn("constructor_sk", row_number().over(window_spec))

#Rename Columns
Constructors = Modified_df \
    .withColumn("constructor_id", lower(col("name"))) \
    .select("constructor_id", "nationality").drop("name")

display(Constructors )


#Write to Gold Layer
Constructors .write.format("delta").mode("overwrite").saveAsTable("F1_Gold.constructors")
`````
![image](https://github.com/user-attachments/assets/5b0257ba-13a7-46a7-bf6f-bcdcece72502)
