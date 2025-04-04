# Data Warehouse Project

## Data Architecture

This project demonstrates the building of a Data Warehouse based on the Medallion Architecture which conceptually comprises of the Bronze, Silver and Gold layers.

1. Bronze Layer: Stores raw data as it is from the source systems. The data is ingested from the raw csv files into the SQL server database.
   
2. Silver Layer: This layer is where data cleaning takes place as well as data transformation steps such as standardization and normalization.
   
3. Gold Layer: Contains business-ready data that is housed within a Star Schema in this case for the purpose of reporting and analysis.

Data loading into the Bronze and Silver layers is done using Full Load in this project. Truncating and Inserting steps are also put in place to prevent data duplication each time the batch processing is ran.

<img src="https://github.com/bayyangjie/Data-Warehouse-Project/blob/main/docs/Data_Architecture.png?raw=true" width="100%">

## Project Requirements

### Objective
Create a data warehouse using MS SQL server to consolidate the sales data for reporting and decision-making.

### Specifications
Data Sources: The source data is imported in the form of CSV files from the ERP and CRM systems.

Data quality: The imported data undergoes data cleaning to ensure accuracy of analysis

Integration: Combination of the data from all the sources into a data model to understand relationships between the source tables

## Tools used
Draw.io: Design data architecture, models, flows, and diagrams.

MS SQL: GUI for managing and interacting with databases.

Github: Code documentation and description of different phases of the Data Warehouse creation.


