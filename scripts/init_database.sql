/*
===========================================================================
Create Database and Schemas
===========================================================================

Script Purpose:
This script creates a new database called 'DataWarehouse' after checking if it is currently existing. 
If the DataBase is existing, it is dropped and recreated. 3 schemas 'Bronze' , 'Silver' and 'Gold' are created within the database as well.

Note:
Running this script will drop the entire existing database 'DataWarehose' if it is existing. All data in the current database will be deleted. 
*/

USE master;
GO

-- Drop and recreate the 'DataWarehhouse' database if recreating the database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DatawWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO /* Acts as a separator between executing the different schemas */
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
