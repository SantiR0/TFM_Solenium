# Quoía Data Ingestion and Medallion Architecture

This repository contains Python notebooks and utility scripts to implement a data ingestion pipeline for the Quoía project. The pipeline is based on the medallion architecture, following a Bronze, Silver, and Gold layered data structure in Databricks. The architecture leverages Delta Lake for efficient data storage and transformation, ensuring consistency, scalability, and optimal performance for analytics and machine learning models.

![image](https://github.com/user-attachments/assets/114a70c1-9ee0-4953-9392-e741e3a4e154)


# Project Structure
## Data Ingestion Notebooks:
nb_QuoiaDBtoBronze.ipynb: Handles the ingestion of data from a PostgreSQL source database into the Bronze layer in Delta Lake.
nb_BronzeToSilver.ipynb: Transforms and cleans the raw data from the Bronze layer and moves it into the Silver layer. This includes filtering, deduplication, and column transformations.
nb_SilverToGold.ipynb: Consolidates data from the Silver layer into the Gold layer, applying advanced transformations and creating aggregate views for analytics and machine learning.

## Utilities:
nb_utils.ipynb: Contains helper functions and configurations used throughout the notebooks. These include methods for setting up Spark sessions, loading configurations, and managing Delta Lake operations.
nb_utils_modified.py: A Python file which is adapted for testing purposes.
nb_utils_test.py: Unit tests for the ingestion process, ensuring that the pipeline works as expected.

## Workflow
The data ingestion process follows a medallion architecture:

Bronze Layer: Raw data is ingested into the Bronze layer with minimal transformations.
Silver Layer: The data in the Bronze layer is cleansed, and the necessary transformations (e.g., joining, deduplication) are applied before loading into the Silver layer.
Gold Layer: The data in the Silver layer is further transformed, creating a final cleaned dataset ready for analytics and machine learning models.

## Requirements
To run the notebooks in this repository, you will need the following:

Databricks environment: The notebooks are designed to run in Databricks, utilizing the Spark and Delta Lake ecosystem.
PostgreSQL: Source data for the ingestion process is stored in PostgreSQL, and you'll need access to the relevant databases.
Python Libraries:
  pyspark
  pandas
  delta.tables
  json


How to Use
Step 1: Bronze Ingestion
Use the nb_QuoiaDBtoBronze.ipynb notebook to ingest raw data from the PostgreSQL database into the Bronze layer. This step includes setting up connectors and reading data from the source.

Step 2: Silver Transformation
Run the nb_BronzeToSilver.ipynb notebook to transform the raw data and load it into the Silver layer. Here, transformations such as filtering and data cleansing occur.

Step 3: Gold Aggregation
Finally, use the nb_SilverToGold.ipynb notebook to aggregate and enrich the data in the Gold layer. This data is now ready for analysis and machine learning models.
