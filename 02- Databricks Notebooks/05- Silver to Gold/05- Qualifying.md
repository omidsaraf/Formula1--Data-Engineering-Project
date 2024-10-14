Requires Incremental Load with Append mode Write
````python
#path
Path_Qualifying = "/mnt/dldatabricks/02-silver/F1_Silver/qualifying/"

# Read the Delta table into a DataFrame
qualifying_df = spark.read.format("delta").load(Path_Qualifying)
qualifying_df = qualifying_df.drop('ingestion_date')

# Show count of duplications
duplicates = qualifying_df.count() - qualifying_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handling
qualifying_df = qualifying_df.dropDuplicates()

# Show count of nulls
nulls = qualifying_df.select([count(when(col(c).isNull(), c)).alias(c) for c in qualifying_df.columns]).toPandas()
print(f"Nulls: {nulls}", "(**null handling is required data scientist's approval**)")

# Null values must be handled by data scientist's request

# Rename Columns
qualifying_df = qualifying_df.withColumnRenamed("name", "race_name")\
  .orderBy("race_id")

display(qualifying_df)

#Write to Gold Layer
qualifying_df.write.format("delta").mode("append").saveAsTable("F1_Gold.qualifying")
`````
![image](https://github.com/user-attachments/assets/90b7a2a5-a104-4bc5-ad51-08ba73e0f8b4)
