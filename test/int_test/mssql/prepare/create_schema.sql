BEGIN
    BEGIN TRY
        EXEC('DROP TABLE MAIN.test_replace');
    END TRY
    BEGIN CATCH
    END CATCH;
    BEGIN TRY
        EXEC('DROP TABLE MAIN.test_replace2');
    END TRY
    BEGIN CATCH
    END CATCH;
    
    BEGIN TRY
        EXEC('DROP TABLE MAIN.test_append');
    END TRY
    BEGIN CATCH
    END CATCH;
    BEGIN TRY
        EXEC('DROP TABLE MAIN.test_append2');
    END TRY
    BEGIN CATCH
    END CATCH;
    
    BEGIN TRY
        EXEC('DROP TABLE MAIN.test_truncate');
    END TRY
    BEGIN CATCH
    END CATCH;
    BEGIN TRY
        EXEC('DROP TABLE MAIN.test_truncate2');
    END TRY
    BEGIN CATCH
    END CATCH;
    EXEC('DROP SCHEMA IF EXISTS MAIN');
    EXEC('CREATE SCHEMA MAIN');
END;
