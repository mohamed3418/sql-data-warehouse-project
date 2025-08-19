CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    SET NOCOUNT OFF;

    DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationMs BIGINT, @globalStart DATETIME2;
    SET @globalStart = SYSDATETIME();

    BEGIN TRY
        PRINT '===============Starting Bronze Load Procedure===============';

        --------------------------------------------------------------
        -- CRM Tables
        PRINT '============================================================';
        PRINT '>> Starting load from source: CRM Source';

        -- crm_cust_info
        PRINT '------------------------------------------------------------';
        PRINT '>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE [bronze].[crm_cust_info];

        SET @StartTime = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';

        BULK INSERT [bronze].[crm_cust_info] 
        FROM 'D:\G\Courses\youtube\DWH_full_Project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT '   Duration: ' + CAST(@DurationMs AS VARCHAR(20)) + ' ms';

        -- crm_prd_info
        PRINT '------------------------------------------------------------';
        PRINT '>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE [bronze].[crm_prd_info];

        SET @StartTime = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';

        BULK INSERT [bronze].[crm_prd_info] 
        FROM 'D:\G\Courses\youtube\DWH_full_Project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT '   Duration: ' + CAST(@DurationMs AS VARCHAR(20)) + ' ms';

        -- crm_sales_details
        PRINT '------------------------------------------------------------';
        PRINT '>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE [bronze].[crm_sales_details];

        SET @StartTime = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';

        BULK INSERT [bronze].[crm_sales_details] 
        FROM 'D:\G\Courses\youtube\DWH_full_Project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT '   Duration: ' + CAST(@DurationMs AS VARCHAR(20)) + ' ms';

        PRINT '=============Finished loading source: CRM================';

        --------------------------------------------------------------
        -- ERP Tables
        PRINT '============Starting load from source: ERP==================';

        -- erp_loc_a101
        PRINT '>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE [bronze].[erp_loc_a101];

        SET @StartTime = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';

        BULK INSERT [bronze].[erp_loc_a101] 
        FROM 'D:\G\Courses\youtube\DWH_full_Project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT '   Duration: ' + CAST(@DurationMs AS VARCHAR(20)) + ' ms';

        -- erp_cust_az12
        PRINT '------------------------------------------------------------';
        PRINT '>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE [bronze].[erp_cust_az12];

        SET @StartTime = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

        BULK INSERT [bronze].[erp_cust_az12] 
        FROM 'D:\G\Courses\youtube\DWH_full_Project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT '   Duration: ' + CAST(@DurationMs AS VARCHAR(20)) + ' ms';

        -- erp_px_cat_g1v2
        PRINT '------------------------------------------------------------';
        PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];

        SET @StartTime = SYSDATETIME();
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

        BULK INSERT [bronze].[erp_px_cat_g1v2] 
        FROM 'D:\G\Courses\youtube\DWH_full_Project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT '   Duration: ' + CAST(@DurationMs AS VARCHAR(20)) + ' ms';

        PRINT '=============Finished loading source: ERP=================';

        --------------------------------------------------------------
        PRINT '============Finished Bronze Load Procedure=================';
        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(SECOND, @globalStart, @EndTime);
        PRINT 'Stored Procedure Duration: ' + CAST(@DurationMs AS VARCHAR(20)) + ' second';
    END TRY

    BEGIN CATCH
        PRINT '============================================================';
        PRINT '!!! ERROR OCCURRED DURING Bronze Load Procedure !!!';
        PRINT 'Error Message   : ' + ERROR_MESSAGE();
        PRINT 'Error Severity  : ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
        PRINT 'Error State     : ' + CAST(ERROR_STATE() AS VARCHAR(10));
        PRINT 'Error Number    : ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Procedure : ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
        PRINT 'Error Line      : ' + CAST(ERROR_LINE() AS VARCHAR(10));
        PRINT '============================================================';
        -- Optional: Rethrow the error if you want execution to stop and propagate
        -- THROW;
    END CATCH
END;
