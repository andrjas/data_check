BEGIN
    BEGIN TRY
        EXEC('DROP TABLE temp.test_replace');
    END TRY
    BEGIN CATCH
    END CATCH;
    BEGIN TRY
        EXEC('DROP TABLE temp.test_replace2');
    END TRY
    BEGIN CATCH
    END CATCH;
    
    BEGIN TRY
        EXEC('DROP TABLE temp.test_append');
    END TRY
    BEGIN CATCH
    END CATCH;
    BEGIN TRY
        EXEC('DROP TABLE temp.test_append2');
    END TRY
    BEGIN CATCH
    END CATCH;
    
    BEGIN TRY
        EXEC('DROP TABLE temp.test_truncate');
    END TRY
    BEGIN CATCH
    END CATCH;
    BEGIN TRY
        EXEC('DROP TABLE temp.test_truncate2');
    END TRY
    BEGIN CATCH
    END CATCH;
    EXEC('DROP SCHEMA IF EXISTS TEMP');
    EXEC('CREATE SCHEMA TEMP');
END;
