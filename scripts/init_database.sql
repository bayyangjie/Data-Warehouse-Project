/*
===========================================================================
Create Database and Schemas
===========================================================================

Script Purpose:
This script creates a new database called 'DataWarehouse' after verifying it's existence. 
If the DataBase is existing, it is firstly dropped and then recreated. The schemas 'Bronze' , 'Silver' and 'Gold' are created within the database.

Note:
Running this script will drop the entire existing database 'DataWarehose' if it is existing. All data in the current database will be deleted. 
*/

USE master;   -- brings user to main section where all databases are present so as not to be in the same database that is going to be dropped
GO

-- Drop and recreate the 'DataWarehhouse' database if recreating the database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')      -- checking if a database named 'DataWarehouse' exists by checking the list of databases in the system view
BEGIN          -- run the following code block to perform the DROP if the above database exists
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;          -- ensures no active connections to the database otherwise DROP DATABASE command will fail
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
