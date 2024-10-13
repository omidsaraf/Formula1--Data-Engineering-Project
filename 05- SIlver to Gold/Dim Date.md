`````
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


# Generate a range of dates with id casted to INT
date_range = spark.range(0, 2000).selectExpr("date_add('2020-01-01', cast(id as int)) as date")

# Add additional date columns
dim_date = date_range \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("day_of_week", dayofweek("date")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("is_weekend", (dayofweek("date").isin(1, 7)).cast("boolean")) \
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))

# Reorder Columns
dim_date = dim_date.select("date_key", "date", "year", "month", "day", "day_of_week", "quarter", "is_weekend")



# Display the DataFrame
dim_date.display()

# Write to Gold Layer
dim_date.write.format("delta").mode("overwrite").save("/mnt/dldatabricks/03-gold/Dim_date")
`````
![image](https://github.com/user-attachments/assets/d50914d8-0a30-4bb4-9fae-b7b4428130b8)
![image](https://github.com/user-attachments/assets/c7a34d6f-8e7e-4e61-9f49-b6bc47eea4cc)
