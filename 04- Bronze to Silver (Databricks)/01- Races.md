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
