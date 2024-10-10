### Transformations
````python
races = races_bronze\
        .withColumnRenamed('year', 'race_year') \
        .withColumnRenamed('circuitId', 'circuit_id') \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
        .drop('url', 'date', 'time') \
        .withColumn("ingestion_date", current_timestamp()) \
        .select('race_id', 'race_year', 'name', 'round', 'circuit_id','race_timestamp', 'ingestion_date')
display(races)
````
![image](https://github.com/user-attachments/assets/dc72043e-45e0-4978-bf08-e04e30ac1502)

### Write Data as Delta Table (Initial Load)
````python
from pyspark.sql.functions import concat, col, lit, to_timestamp, current_timestamp

# Process the DataFrame
races_silver = races_bronze \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .drop('url', 'date', 'time') \
    .withColumn("ingestion_date", current_timestamp()) \
    .select('race_id', 'race_year', 'name', 'round', 'circuit_id', 'race_timestamp', 'ingestion_date')

# Write the DataFrame in Delta format to the destination
races_silver.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/02-silver/races")
`````
### Incremental load (seperated notebook for ADF Pipeline)- Incremental Load

