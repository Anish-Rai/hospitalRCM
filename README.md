Azure Data Engineering Project: ETL Pipeline with SCD Type 2 and CDM
Overview
This project demonstrates the implementation of a scalable data engineering solution using Azure Databricks and Delta Lake for CDM (Common Data Model) and SCD (Slowly Changing Dimensions) Type 2 logic. The pipeline processes raw data from Bronze (raw layer) to Silver (cleansed layer), ensuring data integrity and compliance with historical data requirements.

Key Features
SCD Type 2 Implementation: Maintains historical records while tracking data changes for accurate reporting.
CDM ETL Pipeline: Transforms raw data into a structured format following the Common Data Model standards.
Delta Lake Integration: Ensures ACID compliance and supports efficient data storage and querying.
Azure Databricks Notebooks: Contains modularized PySpark code for data extraction, transformation, and loading.
Layered Architecture:
Bronze: Raw, unprocessed data.
Silver: Cleansed and enriched data.
Gold (Future Scope): Aggregated and analytics-ready data.
Tools and Technologies
Cloud Platform: Microsoft Azure
Data Processing: Azure Databricks (PySpark)
Data Storage: Azure Data Lake Storage (ADLS) with Delta Lake
ETL Orchestration: Azure Data Factory
Monitoring: Azure Monitor
Version Control: GitHub
