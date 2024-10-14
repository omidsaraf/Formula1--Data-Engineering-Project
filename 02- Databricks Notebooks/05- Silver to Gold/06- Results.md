Incremental, Partition by Season and Appened Mode
`````python
#path
Path_Results = "/mnt/dldatabricks/02-silver/F1_Silver/results/"

# Read the Delta table into a DataFrame
Results = spark.read.format("delta").load(Path_Results)
Results = Results.drop('ingestion_date')

# Show count of duplications
duplicates = Results.count() - Results.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handling
Results= Results.dropDuplicates()

# Show count of nulls
nulls = Results.select([count(when(col(c).isNull(), c)).alias(c) for c in Results.columns]).toPandas()
print(f"Nulls: {nulls}", "(**null handling is required data scientist's approval**)")

# Null values must be handled by data scientist's request

# Rename Columns
Results.display()

#Write to Gold Layer
Results.write.format("delta").partitionBy("season").mode("append").saveAsTable("F1_Gold.results")
```

![image](https://github.com/user-attachments/assets/930c2fad-5da7-4c45-8b17-819644a7c717)
