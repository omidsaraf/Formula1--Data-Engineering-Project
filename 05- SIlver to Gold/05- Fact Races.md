
## Fact_Races Table Analysis

- **Explanation:**
Duplicates Handling: Removes any duplicate records.

-Null Handling: Fills null values with default values for demonstration purposes (data scientists should determine appropriate values).

-Key Lookup: Reads the Dim_circuits table from the Gold Layer and performs a left join to add the circuit_sk foreign key.

-Column Renaming: Renames name to race_name for clarity.

-Removing Redundant Columns: Removes the business key circuit_id from the fact table.

-Saving Data: Writes the transformed data into the Gold Layer as Delta format, partitioned by race_year.

-**Incremental Load:**
For the incremental load, the best approach would be to use the merge (upsert) operation. This ensures that the existing records are updated, and new records are inserted without data loss.

### Columns:
- **race_id**: integer
- **race_year**: integer
- **race_name**: string
- **round**: integer
- **circuit_sk**: integer
- **race_timestamp**: timestamp

### Reasoning:
- **Updates Without History**: Race details might change over time, but we don't need to track these changes historically.
- **Simplified ETL**: Using SCD Type 1 simplifies the ETL process by overwriting the existing data with the latest updates.

### ETL Process for Fact_Races Data - Full Load (Initially)

`````python
#Fact_Races

# Define paths
Path_Races = "/mnt/dldatabricks/02-silver/races/"
Path_Circuits = "/mnt/dldatabricks/03-gold/Dim_Circuits/"

# Read the Delta table into a DataFrame
races_df = spark.read.format("delta").load(Path_Races)
races_df = races_df.drop('ingestion_date')

# Show count of duplications
duplicates = races_df.count() - races_df.dropDuplicates().count()
print(f"Duplicates: {duplicates}")

# Duplicate Handling
races_df = races_df.dropDuplicates()

# Show count of nulls
nulls = races_df.select([count(when(col(c).isNull(), c)).alias(c) for c in races_df.columns]).toPandas()
print(f"Nulls: {nulls}", "(**null handling is required data scientist's approval**)")

# Null values must be handled by data scientist's request
# For demonstration, filling nulls with default values (should be decided by data scientists)
races_df = races_df.fillna({
    "race_id": -1,
    "race_year": 1900,
    "name": "Unknown",
    "round": -1,
    "circuit_id": "Unknown",
    "race_timestamp": "1970-01-01T00:00:00.000Z"
})

# Key look up for circuit_sk (read from Dim_circuit of gold layer and insert it as foreign key of Dim_circuit into Fact_Races)
dim_circuits_df = spark.read.format("delta").load(Path_Circuits).select("circuit_id", "circuit_sk")

fact_races = races_df.join(dim_circuits_df, races_df.circuit_id == dim_circuits_df.circuit_id, "left_outer") \
    .select(races_df["*"], dim_circuits_df["circuit_sk"])

# Remove circuit business key (circuit_id) from Fact
fact_races = fact_races.drop("circuit_id").orderBy("race_id")\
  .withColumnRenamed("name", "race_name")\
  .select("race_id", "race_name","race_year","round", "circuit_sk", "race_timestamp")

# Display the final DataFrame
display(fact_races)
#Write to Gold Layer (Partitioning by race_year)
fact_races.write.format("delta").partitionBy("race_year").mode("overwrite").save("/mnt/dldatabricks/03-gold/Fact_races")

![image](https://github.com/user-attachments/assets/88c43369-8817-44d1-9465-609a266a1815)
![image](https://github.com/user-attachments/assets/1295719e-985b-4919-b867-637b75d80515)
````
### ETL Process for Fact_Races Data - Incremental Load
````python
from delta.tables import DeltaTable

# Read the existing Delta table in the Gold Layer
delta_table = DeltaTable.forPath(spark, "/mnt/dldatabricks/03-gold/Fact_races")

# Define the DataFrame with new data
new_data = fact_races  # Your transformed DataFrame

# Perform the merge (upsert) operation
delta_table.alias("existing") \
    .merge(
        new_data.alias("new"),
        "existing.race_id = new.race_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Display the merged data
merged_data = spark.read.format("delta").load("/mnt/dldatabricks/03-gold/Fact_races")
merged_data.show(truncate=False)

