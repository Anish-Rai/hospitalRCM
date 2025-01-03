# Azure Data Engineering Project: ETL Pipeline with SCD Type 2 and CDM
### Overview
This project demonstrates the implementation of a scalable data engineering solution using Azure Databricks and Delta Lake for CDM (Common Data Model) and SCD (Slowly Changing Dimensions) Type 2 logic. The pipeline follows medallion architecture as it processes raw data from Bronze (raw layer) to Silver (cleansed layer), ensuring data integrity and compliance with historical data requirements.

### Data Pipeline Architecture
![data-pipeline-architecture](https://github.com/user-attachments/assets/e4c1092f-b82c-42f8-b3c6-5dd4c3aa9bb3)



### Key Features
* SCD Type 2 Implementation: Maintains historical records while tracking data changes for accurate reporting.
* CDM ETL Pipeline: Transforms raw data into a structured format following the Common Data Model standards.
* Delta Lake Integration: Ensures ACID compliance and supports efficient data storage and querying.
* Azure Databricks Notebooks: Contains modularized PySpark code for data extraction, transformation, and loading.
* Layered Architecture:
* Bronze: Raw, unprocessed data.
* Silver: Cleansed and enriched data.
* Gold (Future Scope): Aggregated and analytics-ready data.
* Tools and Technologies
* Cloud Platform: Microsoft Azure
* Data Processing: Azure Databricks (PySpark)
* Data Storage: Azure Data Lake Storage (ADLS) with Delta Lake
* ETL Orchestration: Azure Data Factory
* Monitoring: Azure Monitor
* Version Control: GitHub

### Tools and Technologies
* Cloud Platform: Microsoft Azure
* Data Processing: Azure Databricks (PySpark)
* Data Storage: Azure Data Lake Storage (ADLS) with Delta Lake
* ETL Orchestration: Azure Data Factory
* Monitoring: Azure Monitor
* Version Control: GitHub

### Pipeline Details
1. Bronze Layer
Contains raw, unprocessed data directly ingested from various source systems.
Stored in Delta format for scalability.
2. Silver Layer
Cleansed and enriched data processed by the ETL pipeline.
Implements SCD Type 2 logic:
Maintains historical records for tracking changes.
Uses is_current and timestamps for version control.
Converts raw data into CDM-compliant entities.
3. Gold Layer (Future Scope)
Aggregated and analytics-ready data for BI tools.
Optimized for reporting and visualization.

### 🔗 Read the full article on Medium:  
[What I Learned from Building My First ETL Pipeline in Azure for Healthcare Data](https://medium.com/@anish.rai3737/what-i-learned-from-building-my-first-etl-pipeline-in-azure-for-healthcare-data-476fff00c924)
