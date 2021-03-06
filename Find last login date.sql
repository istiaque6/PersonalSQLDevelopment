DECLARE @ITERATOR INT = 0,
	@GROUP_NAME VARCHAR(100),
	@SQL_STRING VARCHAR(MAX),
	@LogType INT = 1,
	@filterstr NVARCHAR(4000) = '',
	@LOGIN_NAME VARCHAR(100),
	@LAST_ACCESS_DATE DATETIME = - 53690,
	@TEXT VARCHAR(MAX) = 'No Login record found';

CREATE TABLE #LOGIN_LISTS (
	[GROUP] VARCHAR(100),
	[LOGIN] VARCHAR(100),
	[IS_SEARCHED] BIT,
	[STATUS] VARCHAR(25),
	[LAST ACCESS DATE] DATETIME,
	[LOG_TEXT] VARCHAR(MAX)
	)

CREATE TABLE #xp_logininfo (
	[ACCOUNTNAME] SYSNAME NULL,
	[TYPE] CHAR(8) NULL,
	[PRIVILEGE] CHAR(9) NULL,
	[MAPPEDLOGINNAME] SYSNAME NULL,
	[PERMISSIONPATH] SYSNAME NULL
	);

CREATE TABLE #LOG_READER (
	LOGDATE DATETIME,
	PROCESS_INFO VARCHAR(50),
	TEXT VARCHAR(MAX)
	);

DECLARE @LogList TABLE (
	LogNumber INT,
	StartDate DATETIME,
	SizeInBytes INT
	)

--lOGINS AND GROUP
SELECT *
INTO #TEMP_SERVER_LOGINS
FROM (
	SELECT sp.name AS LOGIN,
		sp.type_desc AS login_type,
		CASE 
			WHEN sp.is_disabled = 1
				THEN 'Disabled'
			ELSE 'Enabled'
			END AS STATUS
	FROM sys.server_principals sp
	LEFT JOIN sys.sql_logins sl ON sp.principal_id = sl.principal_id
	WHERE sp.type != 'R'
		AND sp.name NOT LIKE '%##%'
		AND sp.name NOT LIKE '%NT'
	) AS X

--INSERT THE INDIVIDUAL LOGIN IN THE MASTER TABLE
INSERT INTO #LOGIN_LISTS (
	[GROUP],
	[LOGIN],
	[STATUS],
	[IS_SEARCHED]
	)
SELECT 'N/A',
	[login],
	[status],
	0
FROM #TEMP_SERVER_LOGINS
WHERE login_type != 'WINDOWS_GROUP'

DELETE
FROM #TEMP_SERVER_LOGINS
WHERE login_type != 'WINDOWS_GROUP'

-- EXTRACT THE INDIVIDUAL USERS FROM THE GROUP
SET @ITERATOR = (
		SELECT COUNT(*)
		FROM #TEMP_SERVER_LOGINS
		WHERE login_type = 'WINDOWS_GROUP'
		);

WHILE @ITERATOR > 0
BEGIN
	SET @GROUP_NAME = (
			SELECT TOP 1 [login]
			FROM #TEMP_SERVER_LOGINS
			WHERE login_type = 'WINDOWS_GROUP'
			)
	SET @SQL_STRING = 'INSERT INTO #xp_logininfo
( [ACCOUNTNAME], [TYPE],[PRIVILEGE], [MAPPEDLOGINNAME],[PERMISSIONPATH] ) EXECUTE XP_LOGININFO ''' + @GROUP_NAME + ''',''MEMBERS'''

	EXECUTE (@SQL_STRING);

	--DELETE THE SELECTED GROUP FROM THE TABLE
	DELETE
	FROM #TEMP_SERVER_LOGINS
	WHERE [login] = @GROUP_NAME;

	SET @ITERATOR = (
			SELECT COUNT(*)
			FROM #TEMP_SERVER_LOGINS
			WHERE login_type = 'WINDOWS_GROUP'
			);
END

--ADD EXTRACTED USERS INTO THE MASTER TABLE
INSERT INTO #LOGIN_LISTS (
	[GROUP],
	[LOGIN],
	[STATUS],
	[IS_SEARCHED]
	)
SELECT A.[PERMISSIONPATH],
	A.[ACCOUNTNAME],
	'Enabled',
	0
FROM #xp_logininfo A

--INNER JOIN #TEMP_SERVER_LOGINS B ON A.[PERMISSIONPATH] = B.[status]
--FIND THE LOGIN ACCESS DATE FOR EACH LOGIN
SET @ITERATOR = (
		SELECT COUNT(*)
		FROM #LOGIN_LISTS
		WHERE IS_SEARCHED = 0
		);

WHILE @ITERATOR > 0
BEGIN
	SET @LOGIN_NAME = (
			SELECT TOP 1 [LOGIN]
			FROM #LOGIN_LISTS
			WHERE IS_SEARCHED = 0
			);
	SET @filterstr = 'Login succeeded for user ''' + @LOGIN_NAME + '''';

	--SELECT @filterstr;
	INSERT INTO @LogList
	EXEC xp_enumerrorlogs @LogType

	-- Iterate on all the logs and collect all log rows
	DECLARE @idx INT = 0

	WHILE @idx <= (
			SELECT MAX(LogNumber)
			FROM @LogList
			)
	BEGIN
		INSERT INTO #LOG_READER
		EXEC xp_readerrorlog @idx -- Log number
			,
			@LogType -- 1=SQL Server log, 2=SQL Agent log
			,
			@filterstr -- filter string
			,
			@filterstr

		SET @idx += 1
	END

	--FIND THE LATEST ENTRY FOR THI LOGIN
	SELECT TOP 1 @LAST_ACCESS_DATE = LOGDATE,
		@TEXT = TEXT
	FROM #LOG_READER
	ORDER BY LOGDATE DESC;

	UPDATE #LOGIN_LISTS
	SET [LOG_TEXT] = @TEXT,
		[LAST ACCESS DATE] = @LAST_ACCESS_DATE
	WHERE LOGIN = @LOGIN_NAME

	TRUNCATE TABLE #LOG_READER

	--UPDATE THE SEARCHED COLUMN
	UPDATE #LOGIN_LISTS
	SET IS_SEARCHED = 1
	WHERE LOGIN = @LOGIN_NAME;

	SET @ITERATOR = (
			SELECT COUNT(*)
			FROM #LOGIN_LISTS
			WHERE IS_SEARCHED = 0
			);
	SET @LAST_ACCESS_DATE = - 53690;
	SET @TEXT = 'No Login record found';
END

SELECT *
FROM #LOGIN_LISTS

--DROPPING ALL THE TEMP TABLE
DROP TABLE #TEMP_SERVER_LOGINS

DROP TABLE #LOGIN_LISTS

DROP TABLE #xp_logininfo

DROP TABLE #LOG_READER
	----Drop the database users
	--EXEC sp_MSForEachDB 'USE [?];
	--        IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N''US\SCOM-SQL-Monitor'')
	--        DROP USER [US\SCOM-SQL-Monitor]; '
	--	--Drop the login
	--IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'US\SCOM-SQL-Monitor')
	--    DROP LOGIN [US\SCOM-SQL-Monitor];
