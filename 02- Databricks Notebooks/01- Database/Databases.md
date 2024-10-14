

````python

def create_database(layer, database_name):
    # Use the CREATE DATABASE IF NOT EXISTS command to ensure the database is created if it does not exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION '/mnt/dldatabricks/{layer}/{database_name}'")
    print(f"Database {database_name} created or already exists")
    spark.sql(f"DESCRIBE DATABASE EXTENDED {database_name}").display()

````
````
# Now, call the function to create your databases

create_database("02-silver","F1_Silver")
create_database("03-gold","F1_Gold")
````

![image](https://github.com/user-attachments/assets/b73ab9d3-2696-4a30-bef2-ee4daf44999f)
![image](https://github.com/user-attachments/assets/ac7c5ece-3b8e-4620-b7f2-f287530a91c3)

![image](https://github.com/user-attachments/assets/dfc26731-639f-41dd-93bb-5006964c9351)

