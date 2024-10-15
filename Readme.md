# Formula1--Data Engineering Project
![image](https://github.com/user-attachments/assets/c0c07ab1-fd2c-4711-a414-5f56004a3c2c)
![image](https://github.com/user-attachments/assets/b6a3e93e-0923-44f6-808a-1092f3a6ec18)
## Project Purpose

The Ergast Formula 1 Project is designed to provide a robust and scalable data pipeline for ingesting, transforming, and analyzing Formula 1 racing data. This project leverages modern data engineering practices, including incremental data ingestion, ETL (Extract, Transform, Load) pipelines, and Delta Lake for efficient data storage and querying. The primary goal is to enable data analysts and data scientists to derive actionable insights and perform advanced analytics on Formula 1 data using tools like Databricks and Power BI.

## Data Ingestion and Processing Workflow

![image](https://github.com/user-attachments/assets/2543dddf-cfef-4513-81cc-46d2fae5d507)

### 1. Initial Data Ingestion

![image](https://github.com/user-attachments/assets/233c98ad-11e7-403f-afb3-4d3893039d20)

#### API Data Ingestion with Python and Pandas
- **Objective**: Incrementally ingest data from the Ergast API.
- **Tools**: Python, Pandas.
- **Process**:
  - Use Python and Pandas to fetch data incrementally from the Ergast API.
  - Save the ingested files into the Landing Zone (staging area).

### 2. Pipeline Setup

#### Pipeline for Data Management
- **Objective**: Automate data management in the Landing Zone.
- **Tools**: Azure Data Factory (ADF), Pipeline parameters.
- **Process**:
  - Use a Pipeline parameter array to manage files within the Landing Zone.
  - Delete existing files from the last ingestion using conditional logic.
  - Copy new data into the Bronze Zone using a Databricks notebook.
![image](https://github.com/user-attachments/assets/810247f3-e416-4be3-b47f-0d4fa2dee611)

### 3. Data Transformation and Loading

#### Pipeline for Data Transfer from Bronze to Silver Layer
- **Objective**: Transfer data from the Bronze Zone to the Silver Layer.
- **Tools**: Azure Data Factory, For Each loop container, Pipeline parameters.
- **Process**:
  - Iterate through Pipeline parameters within a For Each loop.
  - Copy files one by one under a folder named with the ingestion date.
 ![image](https://github.com/user-attachments/assets/4328c271-37fa-4898-a552-2b565dff2b9d)

### 4. Data Cleansing and Transformation

#### Incremental Transformation and Loading in Silver Layer
- **Objective**: Clean and transform data incrementally in the Silver Layer.
- **Tools**: Databricks, PySpark.
- **Process**:
  - Use a Databricks notebook to transform data incrementally.
  - Modify data types and schemas.
  - Write data as Delta tables in managed databases (Silver and Gold layers) for analysis.

![image](https://github.com/user-attachments/assets/131f00db-e733-4aad-b125-1cd7579bb647)


### 5. Final Data Cleansing and Loading

#### Final Data Cleansing in Gold Layer
- **Objective**: Perform final data cleansing and load in the Gold Layer.
- **Tools**: Databricks, PySpark.
- **Process**:
  - Handle null values for dimensions, count them, and handle duplications.
  - Write cleaned dataframes in Delta format.
  - Use overwrite mode for dimensions and append mode for facts, partitioning by last ingested data to save cost.

### 6. Azure Account and Security Setup

#### Azure Environment Setup
- **Objective**: Secure and configure the Azure environment for the project.
- **Tools**: Azure Service Principals, Azure Key Vault.
- **Process**:
  - Set up service principals and configure Azure Key Vault for secrets management.
  - Mount storage and configure access shares for the Databricks cluster.
  - Run and test pipelines using ADF with scheduled triggers.
![376177636-6dfc1021-181c-451a-ab21-aeb13bcc3f40](https://github.com/user-attachments/assets/70628d9e-cd98-4982-9909-feb7709168b4)
![image](https://github.com/user-attachments/assets/c7bdd929-6b85-4a00-a491-8eba0cc51b7f)

### 7. Data Visualization and Insights
![image](https://github.com/user-attachments/assets/6eede8ff-5ae6-4b89-b182-28cd33cec58b)

#### Connecting Databricks to Power BI
- **Objective**: Enable data visualization and business insights.
- **Tools**: Power BI, Databricks.
- **Process**:
  - Connect Databricks to Power BI to define relationships between tables in the data model.
  - Generate reports and dashboards to derive business insights as required.

## Best Practices

- **Modularity**: Ensure each component (ingestion, transformation, loading) is modular and reusable.
- **Scalability**: Design pipelines to handle large datasets efficiently.
- **Security**: Implement robust security measures, including role-based access control and encryption.
- **Monitoring**: Set up monitoring and alerting mechanisms for pipelines.
- **Documentation**: Maintain comprehensive documentation for each step of the process.

