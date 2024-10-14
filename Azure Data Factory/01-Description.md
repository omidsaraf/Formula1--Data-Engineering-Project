
# Azure Data Factory (ADF) Pipeline

The Azure Data Factory (ADF) pipeline orchestrates and automates the data movement and transformation processes. It ensures data is processed efficiently and incrementally, facilitating a seamless ETL (Extract, Transform, Load) workflow.

## Components

### 1. Data Ingestion
- **Objective**: Fetch data incrementally from various sources like APIs or files and load them into the Landing Zone.
- **Tools**: Python, Pandas.
- **Process**: Utilize Python and Pandas to fetch data from the Ergast API, saving the ingested files into the Landing Zone (staging area).

### 2. Data Management
- **Objective**: Automate data management in the Landing Zone.
- **Tools**: Azure Data Factory (ADF), Pipeline parameters.
- **Process**:
  - Use a Pipeline parameter array to manage files within the Landing Zone.
  - Employ conditional logic to delete existing files from the previous ingestion.
  - Copy new data into the Bronze Zone using a Databricks notebook.

### 3. Data Transfer from Bronze to Silver Layer
- **Objective**: Transfer data from the Bronze Zone to the Silver Layer.
- **Tools**: Azure Data Factory, For Each loop container, Pipeline parameters.
- **Process**:
  - Iterate through Pipeline parameters within a For Each loop container.
  - Copy files individually under a folder named with the ingestion date.

### 4. Data Cleansing and Transformation
- **Objective**: Clean and transform data incrementally in the Silver Layer.
- **Tools**: Databricks, PySpark.
- **Process**:
  - Use a Databricks notebook to transform data incrementally.
  - Modify data types and schemas.
  - Write data as Delta tables in managed databases for the Silver and Gold layers, facilitating further analysis.

### 5. Final Data Cleansing in Gold Layer
- **Objective**: Perform final data cleansing and load data into the Gold Layer.
- **Tools**: Databricks, PySpark.
- **Process**:
  - Handle null values for dimensions, count them, and address duplications.
  - Write cleaned dataframes in Delta format.
  - Use overwrite mode for dimensions and append mode for facts, partitioning by the last ingested data to optimize costs.

## Access Control and Security

### Service Principal Creation
1. **Create the Service Principal**:
    ```bash
    az ad sp create-for-rbac --name "myServicePrincipal"
    ```
2. **Capture Output**:
    Save the generated `appId`, `password`, and `tenant` values for later use.

3. **Assign Roles**:
    - **Storage Account Contributor**:
        ```bash
        az role assignment create --assignee <appId> --role "Contributor" --scope /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>
        ```
    - **Databricks Contributor**:
        ```bash
        az role assignment create --assignee <appId> --role "Contributor" --scope /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Databricks/workspaces/<databricks-workspace>
        ```

### Managed Identity for Access Control
1. **Enable Managed Identity for ADF**:
    - Navigate to ADF in the Azure portal.
    - Enable Managed Identity under the Identity settings.

2. **Assign Roles**:
    - **Storage Account Contributor**:
        ```bash
        az role assignment create --assignee <ADF-principal-id> --role "Contributor" --scope /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>
        ```
    - **Databricks Contributor**:
        ```bash
        az role assignment create --assignee <ADF-principal-id> --role "Contributor" --scope /subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Databricks/workspaces/<databricks-workspace>
        ```
