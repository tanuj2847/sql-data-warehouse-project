/*
========================================
Create Database and Schemas
========================================

Purpose:
  It creates a database as datawarehouse if doesn't exists and creates schemas for gold, bronze and silver.

Warning:
	Running this script will drop the entire datawarehouse database if it exists
	All the data will be deleted and ensure you have proper backups before running the script.

*/

USE master;
GO
-- Drop and recreate the datawarehouse database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Datawarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;
GO
--Create database 'Datawarehouse'
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

--Create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
