CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME; 
	BEGIN TRY
		set @batch_start_time=Getdate()
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading crm tables';
		PRINT '-------------------------------------------------';


		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\tanuj\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		Print 'Load Duration: '+ Cast(DATEDIFF(SECOND, @start_time,@end_time) as nvarchar) + 'seconds';
		Print '+++++++++++++++++++++++++++++++++++++++++++++++++++'

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\tanuj\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		Print 'Load Duration: '+ Cast(DATEDIFF(SECOND, @start_time,@end_time) as nvarchar) + 'seconds';
		Print '+++++++++++++++++++++++++++++++++++++++++++++++++++'

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data Into: bronze.crm_sales_deatils';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\tanuj\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		Print 'Load Duration: '+ Cast(DATEDIFF(SECOND, @start_time,@end_time) as nvarchar) + 'seconds';
		Print '+++++++++++++++++++++++++++++++++++++++++++++++++++'

		PRINT '-------------------------------------------------';
		PRINT 'Loading erp tables';
		PRINT '-------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\tanuj\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		Print 'Load Duration: '+ Cast(DATEDIFF(SECOND, @start_time,@end_time) as nvarchar) + 'seconds';
		Print '+++++++++++++++++++++++++++++++++++++++++++++++++++'

		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\tanuj\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		Print 'Load Duration: '+ Cast(DATEDIFF(SECOND, @start_time,@end_time) as nvarchar) + 'seconds';
		Print '+++++++++++++++++++++++++++++++++++++++++++++++++++'

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\tanuj\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		SET @batch_end_time= getdate()
		
		Print 'Load Duration: '+ Cast(DATEDIFF(SECOND, @start_time,@end_time) as nvarchar) + 'seconds';
		Print '+++++++++++++++++++++++++++++++++++++++++++++++++++'
		Print 'BRONZE LAYER HAS BEEN LOADED , TOTAL BATCH LOAD DURATION: '+ Cast(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) as nvarchar) + 'seconds';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'Error occured during Bronze Layer';
		PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END
