````python
from pyspark.sql.functions import col, explode, lit, monotonically_increasing_id, concat_ws

# Create a Spark session
spark = SparkSession.builder \
    .appName("Read Qualifying JSON") \
    .getOrCreate()

# Define the path to your JSON file
qualifying_path = '/mnt/dldatabricks/01-bronze/*/qualifying.json'

# Read the JSON file into a DataFrame
df = spark.read.json(qualifying_path, multiLine=True)

# Explode the nested Races array
races_df = df.select(explode(col("MRData.RaceTable.Races")).alias("race"))

# Explode the nested QualifyingResults array within each race
qualifying_df = races_df.select(
    col("race.Circuit.circuitId").alias("circuitId"),
    col("race.round").alias("round"),
    explode(col("race.QualifyingResults")).alias("qualifying")
)

# Extract required fields and generate unique IDs for qualifyId and raceId
qualifying_bronze = qualifying_df.select(
    monotonically_increasing_id().cast(IntegerType()).alias("qualify_Id"),  # Generate unique qualifyId
    concat_ws(" ", col("qualifying.Driver.givenName"), col("qualifying.Driver.familyName")).alias("DriverName"),  # Combine names as DriverName
    col("qualifying.Constructor.constructorId").alias("constructor_Id"),
    col("qualifying.number").cast(IntegerType()).alias("number"),
    col("qualifying.position").cast(IntegerType()).alias("position"),
    col("qualifying.Q1").alias("q1"),
    col("qualifying.Q2").alias("q2"),
    col("qualifying.Q3").alias("q3"),
    monotonically_increasing_id().cast(IntegerType()).alias("race_Id")  # Generate unique raceId
)

# Display the transformed DataFrame
qualifying_bronze.display()
`````
![image](https://github.com/user-attachments/assets/12e6de7e-fbfd-4dc7-8b5d-cbcb8c8def89)

