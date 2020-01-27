USE [AFS_UTILITIES]
GO

-- =============================================
-- Author:		Istiaque Hassan
-- Create date: 12/20/2019
-- Description:	This store proc is the underlying framework for scrubbing the data. 
--				It takes database name, table name and column name (as comma seperated values) as input and replace the column values with dynamic length values.
-- =============================================
ALTER PROCEDURE [SP_AFS_DBA_SCRUBBING_FRAMEWORK]
	-- Add the parameters for the stored procedure here
	@DATABASE_NAME VARCHAR(100), --EXAMPLE:		'AFSDEV',
	@TABLE_NAME VARCHAR(100), --EXAMPLE:		'HR_Leavers',
	@COLUMN_NAME VARCHAR(MAX) --EXAMPLE:		'Employee Name,Enterprise ID,Employee Email' <-- Note: comma seperated value
AS
BEGIN
	DECLARE @SQL_STATEMENT NVARCHAR(MAX),
		@PARAM_STATEMENT NVARCHAR(MAX),
		@ITERATOR INT,
		@INDEX INT = 0,
		@START INT = 0,
		@MASKED_COLUMN_NAME VARCHAR(100),
		@DATA_TYPE VARCHAR(100),
		@DATA_LENGTH INT,
		@SCHEMA_NAME VARCHAR(100);

	SET @COLUMN_NAME = CONCAT (
			@COLUMN_NAME,
			','
			);-- ADDED AN EXTRA COLUMN SO THAT IT CAN BE SEPERATE IN THE WHILE LOOP
		----PRINT @COLUMN_NAME
		--SET THE ITERATOR
	SET @ITERATOR = CAST((DATALENGTH(@COLUMN_NAME) - DATALENGTH(REPLACE(@COLUMN_NAME, ',', ''))) / DATALENGTH(',') AS INT);

	----PRINT @ITERATOR;
	--INPLACE MASKING
	WHILE @ITERATOR > 0
	BEGIN
		SET @INDEX = CHARINDEX(',', @COLUMN_NAME, @INDEX);
		----PRINT 'INDEX IS: '+ CAST(@INDEX AS VARCHAR);
		----PRINT 'START IS: '+CAST(@START AS VARCHAR);
		SET @MASKED_COLUMN_NAME = SUBSTRING(@COLUMN_NAME, @START + 1, @INDEX - @START - 1)
		----PRINT @MASKED_COLUMN_NAME;
		--INITIATE INPLACE MASKING
		--FIND THE DATA TYPE
		-- --PRINT QUOTENAME(@DATABASE_NAME)
		SET @SQL_STATEMENT = N'SELECT @DATA_TYPE = DATA_TYPE, @DATA_LENGTH = CHARACTER_MAXIMUM_LENGTH, @SCHEMA_NAME = TABLE_SCHEMA FROM ' + QUOTENAME(@DATABASE_NAME) + '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TABLE_NAME AND COLUMN_NAME = @MASKED_COLUMN_NAME';
		SET @PARAM_STATEMENT = N'@DATA_TYPE  VARCHAR(100) OUTPUT, @TABLE_NAME VARCHAR(100),@MASKED_COLUMN_NAME VARCHAR (100), @DATABASE_NAME VARCHAR(100), @DATA_LENGTH INT OUTPUT, @SCHEMA_NAME VARCHAR(100) OUTPUT';

		--PRINT @SQL_STATEMENT;
		EXECUTE sp_executesql @SQL_STATEMENT,
			@PARAM_STATEMENT,
			@TABLE_NAME = @TABLE_NAME,
			@MASKED_COLUMN_NAME = @MASKED_COLUMN_NAME,
			@DATA_TYPE = @DATA_TYPE OUTPUT,
			@DATABASE_NAME = @DATABASE_NAME,
			@DATA_LENGTH = @DATA_LENGTH OUTPUT,
			@SCHEMA_NAME = @SCHEMA_NAME OUTPUT;

		--PRINT @DATA_TYPE;
		--PRINT @DATA_LENGTH;
		--PRINT @SCHEMA_NAME;
		IF (UPPER(@DATA_TYPE) = UPPER('varchar'))
		BEGIN
			DECLARE @RANGE VARCHAR(100) = 'ABCDEFGHJKLMNPQURSUVWXYZabcdefghjkmnpqursuvwxyz',
				@ITERATOR_COLUMN_VALUE VARCHAR(100),
				@ROW_ITERATOR INT = 0;

			--47 LENGTH
			--IF THE CHAR IS NOT AN EMAIL ADDRESS
			SET @SQL_STATEMENT = N'UPDATE ' + QUOTENAME(@DATABASE_NAME) + N'.' + QUOTENAME(@SCHEMA_NAME) + N'.' + QUOTENAME(@TABLE_NAME) + N' SET ' + QUOTENAME(@MASKED_COLUMN_NAME) + N' = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEWID(),''0'',CHAR(Cast(RAND()*(122-97)+97 as int))),''1'',CHAR(Cast(RAND()*(122-97)+97 as int))),''2'',CHAR(Cast(RAND()*(122-97)+97 as int))),''3'',CHAR(Cast(RAND()*(122-97)+97 as int))),''4'',CHAR(Cast(RAND()*(122-97)+97 as int))),''5'',CHAR(Cast(RAND()*(122-97)+97 as int))),''6'',CHAR(Cast(RAND()*(122-97)+97 as int))),''7'',CHAR(Cast(RAND()*(122-97)+97 as int))),''8'',CHAR(Cast(RAND()*(122-97)+97 as int))),''9'',CHAR(Cast(RAND()*(122-97)+97 as int))),''-'',CHAR(Cast(RAND()*(122-97)+97 as int))) WHERE ' + QUOTENAME(@MASKED_COLUMN_NAME) + N' NOT LIKE ''%_@__%.__%'' OR  PATINDEX(''%[^a-z,0-9,@,.,_,\-]%'',  ' + QUOTENAME(@MASKED_COLUMN_NAME) + N') != 0'

			--PRINT @SQL_STATEMENT;
			EXECUTE (@SQL_STATEMENT);

			--IF THE DATA IS EMAIL ADDRESS
			SET @SQL_STATEMENT = N'UPDATE ' + QUOTENAME(@DATABASE_NAME) + N'.' + QUOTENAME(@SCHEMA_NAME) + N'.' + QUOTENAME(@TABLE_NAME) + N' SET ' + QUOTENAME(@MASKED_COLUMN_NAME) + N' = CONCAT(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NEWID(),''0'',CHAR(Cast(RAND()*(122-97)+97 as int))),''1'',CHAR(Cast(RAND()*(122-97)+97 as int))),''2'',CHAR(Cast(RAND()*(122-97)+97 as int))),''3'',CHAR(Cast(RAND()*(122-97)+97 as int))),''4'',CHAR(Cast(RAND()*(122-97)+97 as int))),''5'',CHAR(Cast(RAND()*(122-97)+97 as int))),''6'',CHAR(Cast(RAND()*(122-97)+97 as int))),''7'',CHAR(Cast(RAND()*(122-97)+97 as int))),''8'',CHAR(Cast(RAND()*(122-97)+97 as int))),''9'',CHAR(Cast(RAND()*(122-97)+97 as int))),''-'',CHAR(Cast(RAND()*(122-97)+97 as int))), ''@XYZ.COM'') WHERE ' + QUOTENAME(@MASKED_COLUMN_NAME) + N' LIKE ''%_@__%.__%'' AND  PATINDEX(''%[^a-z,0-9,@,.,_,\-]%'',  ' + QUOTENAME(@MASKED_COLUMN_NAME) + N') = 0'

			EXECUTE (@SQL_STATEMENT);
		END --END OF IF CONDITION : VARCHAR

		SET @START = @INDEX;
		SET @ITERATOR = @ITERATOR - 1;
		SET @INDEX = @INDEX + 1;

		PRINT QUOTENAME(@DATABASE_NAME) + '/' + QUOTENAME(@TABLE_NAME) + '/' + QUOTENAME(@MASKED_COLUMN_NAME) + ' HAS BEEN SCRUBBED'
	END
END
GO


