# Data Warehouse Project

## 🏗️  Data Architecture

This project demonstrates the building of a Data Warehouse based on the Medallion Architecture.

1. Bronze Layer: Stores raw data as it is from the source systems. The data is ingested from the raw csv files into the SQL server database.
   
2. Silver Layer: This layer is where data cleaning takes place as well as data transformation steps such as standardization and normalization.
   
3. Gold Layer: Contains business-ready data that is housed within a Star Schema in this case for the purpose of reporting and analysis.

Data is loaded into the Bronze and Silver layers as a Full Load in this project. Truncating and Inserting steps are also put in place to prevent data duplication during each batch processing run.

<img src="https://github.com/bayyangjie/Data-Warehouse-Project/blob/main/docs/Data_Architecture.png?raw=true" width="100%">

## 🚀 Project Requirements

### Building the Data Warehouse

#### Objective
Create a data warehouse using MS SQL server to consolidate the sales data for reporting and decision-making.

#### Specifications
* [Data Sources](https://github.com/bayyangjie/Data-Warehouse-Project/tree/main/datasets): The source data pertains to ERP and CRM information and is imported in CSV file format. 
* Data quality: The imported data undergoes data cleaning and transformations to ensure data consistency and accuracy of the analysis
* Integration: Combination of the ERP and CRM data into a single [data model](https://github.com/bayyangjie/Data-Warehouse-Project/blob/main/docs/Data_Integration_Model.drawio.png?raw=true) to understand relationships between the tables

## 🛠️ Tools used
* SQL Server Management Studio (SSMS): GUI for managing and interacting with databases.
* Github: Code documentation and description of different phases of the Data Warehouse creation.
* Draw.io: Design data architecture, models, flows, and diagrams.

