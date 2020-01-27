--Remove the previous implementation
USE [msdb]
GO

/*Drop if exists*/
--remove the trigger from V1
IF EXISTS (
		SELECT *
		FROM sysobjects
		WHERE name = 'trig_check_for_job_failure'
			AND [type] = 'TR'
		)
BEGIN
	DROP TRIGGER [dbo].[trig_check_for_job_failure];
END
GO

--remove the previous job from V1
IF EXISTS (
		SELECT *
		FROM msdb.dbo.sysjobs
		WHERE name = N'SEND SQL JOB ALERTS'
		)
BEGIN
	EXEC msdb.dbo.sp_delete_job @job_name = N'SEND SQL JOB ALERTS';
END
GO

IF EXISTS (
		SELECT *
		FROM msdb.dbo.sysjobs
		WHERE name = N'AUTOMATION (Job failure check)'
		)
BEGIN
	EXEC msdb.dbo.sp_delete_job @job_name = N'AUTOMATION (Job failure check)';
END
GO

USE [AFS_UTILITIES]
GO

--remove the store proc from V2
IF EXISTS (
		SELECT *
		FROM sysobjects
		WHERE id = object_id(N'[dbo].[Job_Failure_Report]')
			AND OBJECTPROPERTY(id, N'IsProcedure') = 1
		)
BEGIN
	DROP PROCEDURE [dbo].[Job_Failure_Report]
END
GO

/******************************************************************************************************************************************************/
--Implementation starts
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Istiaque Hassan
-- Create date: 10/25/2017
-- Description:	This stored procedure will send out the email with the job failure msg.
-- =============================================
/*Create 3 tables for Job error archive - Purpose: convert and build a machine learning training dataset*/
USE AFS_UTILITIES

--Archive for Failed job hostory
IF NOT EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = 'dbo'
			AND TABLE_NAME = 'Archived_failed_Job'
		)
BEGIN
	CREATE TABLE [dbo].[Archived_failed_Job] (
		[Server_Name] VARCHAR(255),
		[instance_id] INT,
		[job_name] VARCHAR(255),
		[step_id] INT NOT NULL,
		[step_name] VARCHAR(255),
		[process_type] VARCHAR(255),
		[last_ran] [datetime],
		[notified] [bit]
		);
END

--Archive for Job Steps (Non SSIS job records normally recorded in this table)
IF NOT EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = 'dbo'
			AND TABLE_NAME = 'Archived_failed_step'
		)
BEGIN
	CREATE TABLE [dbo].[Archived_failed_step] (
		[instance_id] INT,
		[job_name] VARCHAR(255),
		[failed step] INT,
		[step name] VARCHAR(100),
		[time_of_error] [datetime],
		[duration] VARCHAR(255),
		[error_message] VARCHAR(max),
		[notified] [bit]
		);
END

--Archive for SSIS error log 
IF NOT EXISTS (
		SELECT *
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = 'dbo'
			AND TABLE_NAME = 'Archived_failed_SSIS_message'
		)
BEGIN
	CREATE TABLE [dbo].[Archived_failed_SSIS_message] (
		[instance_id] [int] NULL,
		[Folder Name] [nvarchar](128) NULL,
		[Project Name] [nvarchar](128) NULL,
		[Package Name] [nvarchar](260) NULL,
		[Caller Name] [nvarchar](128) NULL,
		[Duration] [nvarchar](128) NULL,
		[Start Time] [nvarchar](128) NULL,
		[End Time] [nvarchar](128) NULL,
		[Error Time] [nvarchar](128) NULL,
		[Error Message Type] [nvarchar](300) NULL,
		[Message source description] [nvarchar](128) NULL,
		[Error Message] [nvarchar](max) NULL,
		[notified] [bit] NULL,
		[id] [int] IDENTITY(1, 1) NOT NULL
		);
END

IF NOT EXISTS (
		SELECT *
		FROM sys.indexes
		WHERE name = 'IDX-InstanceID'
			AND object_id = OBJECT_ID('[dbo].[Archived_failed_step]')
		)
BEGIN
	CREATE CLUSTERED INDEX [IDX-InstanceID] ON [dbo].[Archived_failed_step] ([instance_id] ASC)
		WITH (
				PAD_INDEX = OFF,
				STATISTICS_NORECOMPUTE = OFF,
				SORT_IN_TEMPDB = OFF,
				DROP_EXISTING = OFF,
				ONLINE = OFF,
				ALLOW_ROW_LOCKS = ON,
				ALLOW_PAGE_LOCKS = ON,
				FILLFACTOR = 70
				)
END

IF NOT EXISTS (
		SELECT *
		FROM sys.indexes
		WHERE name = 'IDX-InstanceID'
			AND object_id = OBJECT_ID('[dbo].[Archived_failed_Job]')
		)
BEGIN
	CREATE CLUSTERED INDEX [IDX-InstanceID] ON [dbo].[Archived_failed_Job] ([instance_id] ASC)
		WITH (
				PAD_INDEX = OFF,
				STATISTICS_NORECOMPUTE = OFF,
				SORT_IN_TEMPDB = OFF,
				DROP_EXISTING = OFF,
				ONLINE = OFF,
				ALLOW_ROW_LOCKS = ON,
				ALLOW_PAGE_LOCKS = ON,
				FILLFACTOR = 70
				)
END

--DROP THE PREVIOUSLY CREATED INDEX
IF EXISTS (
		SELECT *
		FROM sys.indexes
		WHERE name = 'PK_Archived_failed_SSIS_message'
			AND object_id = OBJECT_ID('[dbo].[Archived_failed_SSIS_message]')
		)
BEGIN
	ALTER TABLE [dbo].[Archived_failed_SSIS_message]

	DROP CONSTRAINT [PK_Archived_failed_SSIS_message]
	WITH (ONLINE = OFF)
END

IF NOT EXISTS (
		SELECT *
		FROM sys.indexes
		WHERE name = 'IDX-InstanceID'
			AND object_id = OBJECT_ID('[dbo].[Archived_failed_SSIS_message]')
		)
BEGIN
	CREATE CLUSTERED INDEX [IDX-instanceID] ON [dbo].[Archived_failed_SSIS_message] ([instance_id] ASC)
		WITH (
				PAD_INDEX = OFF,
				STATISTICS_NORECOMPUTE = OFF,
				SORT_IN_TEMPDB = OFF,
				DROP_EXISTING = OFF,
				ONLINE = OFF,
				ALLOW_ROW_LOCKS = ON,
				ALLOW_PAGE_LOCKS = ON,
				FILLFACTOR = 70
				)
END

/*Create the store procedure. This store procedure will scan the error log for reporting*/
USE AFS_UTILITIES
GO

/*Drop if exists*/
IF EXISTS (
		SELECT *
		FROM sysobjects
		WHERE id = object_id(N'[dbo].[Archive_Failed_Job]')
			AND OBJECTPROPERTY(id, N'IsProcedure') = 1
		)
BEGIN
	DROP PROCEDURE [dbo].[Archive_Failed_Job]
END
GO

/*This proc will generate the report based on the instance ID passed to it.*/
CREATE PROCEDURE [dbo].[Archive_Failed_Job] @instance_id INT
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--get basic server info.
	DECLARE @server_name_basic VARCHAR(255) = (
			SELECT cast(serverproperty('servername') AS VARCHAR(255))
			);
	DECLARE @Current_Time VARCHAR(50) = (
			SELECT convert(VARCHAR, getdate(), 0)
			);
	DECLARE @server_time_zone VARCHAR(255);

	EXEC master.dbo.xp_regread 'hkey_local_machine',
		'system\currentcontrolset\control\timezoneinformation',
		'timezonekeyname',
		@server_time_zone OUT;

	--Variables
	--DECLARE @P_statement varchar(max), @ParmDefinition varchar(max);
	DECLARE @last_error VARCHAR(255),
		@last_error_job_name VARCHAR(255),
		@failed_job_id VARCHAR(255);

	/*Find the failed job and step name*/
	SET @last_error = @instance_id;

	SELECT @last_error_job_name = sj.name,
		@failed_job_id = sj.job_id
	FROM msdb.dbo.sysjobs sj
	JOIN msdb.dbo.sysjobhistory sjh ON sj.job_id = sjh.job_id
	WHERE instance_id = @last_error;

	/*****************Find the latest failed job information*****************/
	--Insert new records into the Archive table:
	INSERT INTO [AFS_UTILITIES].[dbo].[Archived_failed_Job] (
		[Server_Name],
		[instance_id],
		[job_name],
		[step_id],
		[step_name],
		[process_type],
		[last_ran],
		[notified]
		)
	SELECT @server_name_basic,
		@instance_id,
		sj.name,
		sjs.step_id,
		sjs.step_name,
		sjs.subsystem,
		dateadd(SECOND, CASE 
				WHEN len(sjs.last_run_time) = 5
					THEN LEFT(sjs.last_run_time, 1) * 3600 + substring(cast(sjs.last_run_time AS VARCHAR), 2, 2) * 60 + right(sjs.last_run_time, 2)
				WHEN len(sjs.last_run_time) = 6
					THEN LEFT(sjs.last_run_time, 2) * 3600 + substring(cast(sjs.last_run_time AS VARCHAR), 3, 2) * 60 + right(sjs.last_run_time, 2)
				ELSE 0
				END, convert(DATETIME, cast(nullif(sjs.last_run_date, 0) AS NVARCHAR(10)))),
		0 --Bit Value for notification
	FROM msdb..sysjobs sj
	JOIN msdb..sysjobsteps sjs ON sj.job_id = sjs.job_id
	WHERE sj.name = @last_error_job_name
	ORDER BY sj.name,
		sjs.step_id ASC;

	/*****************Find the latest step error (SQL) *****************/
	--Find the step error message (looping back)
	DECLARE @break INT = 0;

	WHILE (@break < 1)
	BEGIN
		SET @last_error = @last_error - 1;

		IF (
				(
					SELECT COUNT(*)
					FROM msdb..sysjobhistory
					WHERE run_status = 0
						AND instance_id = @last_error
						AND job_id = @failed_job_id
						AND message LIKE '%The step failed%'
					) > 0
				)
			SET @break = 1;
	END

	--Insert the record in the Archived table:
	INSERT INTO [AFS_UTILITIES].[dbo].[Archived_failed_step] (
		[instance_id],
		[job_name],
		[failed step],
		[step name],
		[time_of_error],
		[duration],
		[error_message],
		[notified]
		)
	SELECT @instance_id,
		'job name' = sj.name,
		'failed step' = sjh.step_id,
		'step name' = sjh.step_name,
		'time of error' = msdb.dbo.agent_datetime(run_date, run_time),
		'duration' = CAST(sjh.run_duration / 10000 AS VARCHAR) + ':' + CAST(sjh.run_duration / 100 % 100 AS VARCHAR) + ':' + CAST(sjh.run_duration % 100 AS VARCHAR),
		'error message' = sjh.message,
		0 --Bit value for notification
	FROM msdb..sysjobs sj
	JOIN msdb..sysjobhistory sjh ON sj.job_id = sjh.job_id
	WHERE instance_id = @last_error
	ORDER BY sj.name,
		sjh.step_id ASC;

	/*****************Find the SSIS error if there is any SSIS step *****************/
	--Check if there is any SSIS step in the job, if So, then it will create a table for SSIS error log
	DECLARE @SSIS_Step INT = (
			SELECT COUNT(*)
			FROM [AFS_UTILITIES].[dbo].[Archived_failed_step] step
			WHERE step.[instance_id] = @instance_id
				AND 'SSIS' = (
					SELECT process_type
					FROM [AFS_UTILITIES].[dbo].[Archived_failed_Job] job
					WHERE job.[instance_id] = @instance_id
						AND job.step_id = step.[failed step]
					)
			);

	IF @SSIS_Step > 0
	BEGIN
		--FIND THE SSIS PACKAGE NAMES
		--select @@SERVERNAME as [check]
		--Extract the package Name
		DECLARE @SSIS_Package VARCHAR(100) = (
				SELECT REVERSE(SUBSTRING(RIGHT(REVERSE(command), LEN(command) - PATINDEX('%\xstd.%', REVERSE(command))), 0, CHARINDEX('\', RIGHT(REVERSE(command), LEN(command) - PATINDEX('%\xstd.%', REVERSE(command))))))
				FROM msdb.[dbo].[sysjobsteps]
				WHERE subsystem = 'SSIS'
					AND job_id = @failed_job_id
					AND step_id = (
						SELECT [failed step]
						FROM [AFS_UTILITIES].[dbo].[Archived_failed_step] step
						WHERE step.[instance_id] = @instance_id
						)
				);

		SELECT @SSIS_Package;

		/*	Store SSIS error information into the Archive table.*/
		INSERT INTO [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message] (
			[instance_id],
			[Folder Name],
			[Project Name],
			[Package Name],
			[Caller Name],
			[Duration],
			[Start Time],
			[End Time],
			[Error Time],
			[Error Message Type],
			[Message source description],
			[Error Message],
			[notified]
			) (
			SELECT @instance_id,
			EX.folder_name,
			EX.project_name,
			EX.package_name,
			'caller_name' = O.caller_name,
			--EX.start_time,EX.end_time,OM.message_time,
			convert(VARCHAR(5), DateDiff(s, EX.[start_time], EX.[end_time]) / 3600) + ':' + convert(VARCHAR(5), DateDiff(s, EX.[start_time], EX.[end_time]) % 3600 / 60) + ':' + convert(VARCHAR(5), (DateDiff(s, EX.[start_time], EX.[end_time]) % 60)) AS [duration],
			LEFT(CONVERT(VARCHAR, EX.start_time, 0), 19) AS [start_time],
			LEFT(CONVERT(VARCHAR, EX.end_time, 0), 19) AS [End_time],
			--EX.end_time,
			--EX.status,
			LEFT(CONVERT(VARCHAR, OM.message_time, 0), 19) AS [Error Time],
			--LEFT(CONVERT(VARCHAR, OM.message_time, 0), 19),
			'Error Message type' = EM.message_desc,
			'message_source_desc' = D.message_source_desc,
			OM.message AS [error_message],
			0 --Bit value to store if this row is emailed or not.
			FROM SSISDB.CATALOG.operation_messages AS OM INNER JOIN SSISDB.CATALOG.operations AS O ON O.operation_id = OM.operation_id INNER JOIN (
			VALUES (
				- 1,
				'Unknown'
				),
				(
				120,
				'Error'
				),
				(
				110,
				'Warning'
				),
				(
				70,
				'Information'
				),
				(
				10,
				'Pre-validate'
				),
				(
				20,
				'Post-validate'
				),
				(
				30,
				'Pre-execute'
				),
				(
				40,
				'Post-execute'
				),
				(
				60,
				'Progress'
				),
				(
				50,
				'StatusChange'
				),
				(
				100,
				'QueryCancel'
				),
				(
				130,
				'TaskFailed'
				),
				(
				90,
				'Diagnostic'
				),
				(
				200,
				'Custom'
				),
				(
				140,
				'DiagnosticEx Whenever an Execute Package task executes a child package, it logs this event. The event message consists of the parameter values passed to child packages.  The value of the message column for DiagnosticEx is XML text.'
				),
				(
				400,
				'NonDiagnostic'
				),
				(
				80,
				'VariableValueChanged'
				)
			) EM(message_type, message_desc) ON EM.message_type = OM.message_type INNER JOIN (
			VALUES (
				10,
				'Entry APIs, such as T-SQL and CLR Stored procedures'
				),
				(
				20,
				'External process used to run package (ISServerExec.exe)'
				),
				(
				30,
				'Package-level objects'
				),
				(
				40,
				'Control Flow tasks'
				),
				(
				50,
				'Control Flow containers'
				),
				(
				60,
				'Data Flow task'
				)
			) D(message_source_type, message_source_desc) ON D.message_source_type = OM.message_source_type INNER JOIN SSISDB.[catalog].[executions] AS EX ON EX.object_id = o.object_id WHERE OM.operation_id = (
				SELECT MAX(OM.operation_id)
				FROM SSISDB.CATALOG.operation_messages AS OM
				WHERE OM.message_type = 120
					OR OM.message_type = 130
				)
			AND OM.message_type IN (120, 130)
			AND EX.execution_id = OM.operation_id
			AND EX.package_name = @SSIS_Package
			--AND OM.operation_id = (select MAX(execution_id) from ssisdb.[catalog].[executions])
			)
	END
END
GO

/*Store proc to generate and send the report*/
USE AFS_UTILITIES
GO

/*Drop if exists*/
IF EXISTS (
		SELECT *
		FROM sysobjects
		WHERE id = object_id(N'[dbo].[Generate_Job_Error_Report]')
			AND OBJECTPROPERTY(id, N'IsProcedure') = 1
		)
BEGIN
	DROP PROCEDURE [dbo].[Generate_Job_Error_Report]
END
GO

/*This proc will generate the report based on the instance ID passed to it.*/
CREATE PROCEDURE [dbo].[Generate_Job_Error_Report] @Email_address VARCHAR(MAX)
AS
BEGIN
	/*check if there is any job failure that has not been notified/emailed yet*/
	--create a temp table to hold the instance ID.
	IF object_id('tempdb..#temp_failed_job_Number') IS NOT NULL
		DROP TABLE #temp_failed_job_Number

	CREATE TABLE #temp_failed_job_Number (
		[instance_id] INT,
		)

	INSERT INTO #temp_failed_job_Number ([instance_id])
	SELECT DISTINCT instance_id
	FROM [AFS_UTILITIES].[dbo].[Archived_failed_Job]
	WHERE notified = 0;

	DECLARE @Failed_Job_Number INT = (
			SELECT count(*)
			FROM #temp_failed_job_Number
			)

	--if there is then send the details in individual emails for each jobs.
	WHILE (@Failed_Job_Number > 0)
	BEGIN
		DECLARE @row_count INT = 0;
		--Pick one of the failed instance ID
		DECLARE @instance_id INT = (
				SELECT TOP 1 [instance_id]
				FROM #temp_failed_job_Number
				);
		--process the report
		DECLARE @server_name_basic VARCHAR(255) = (
				SELECT cast(serverproperty('servername') AS VARCHAR(255))
				);
		DECLARE @Current_Time VARCHAR(50) = (
				SELECT convert(VARCHAR, getdate(), 0)
				);
		--DECLARE @server_name_instance_name varchar(255) = (select replace(cast(serverproperty('servername') as varchar(255)), '\', ' SQL Instance: '));
		DECLARE @server_time_zone VARCHAR(255);

		EXEC master.dbo.xp_regread 'hkey_local_machine',
			'system\currentcontrolset\control\timezoneinformation',
			'timezonekeyname',
			@server_time_zone OUT;

		DECLARE @HTML_total VARCHAR(max) = '',
			@color_high VARCHAR(20),
			@color_reg VARCHAR(20) = '#336699',
			@color_high_text VARCHAR(20),
			@Email_subject VARCHAR(500);
		DECLARE @serverName_Temp VARCHAR(128) = (
				SELECT cast(SERVERPROPERTY('MachineName') AS VARCHAR)
				)
		DECLARE @serverNumber VARCHAR(3) = (
				SELECT SUBSTRING(@serverName_Temp, LEN(@serverName_Temp) - 2, 3)
				)
		--Prepare the process information
		DECLARE @ServerName VARCHAR(100),
			@JobName VARCHAR(100),
			@Recent_failure_Step VARCHAR(100),
			@Recent_failure_date VARCHAR(100),
			@Last_failure_date VARCHAR(100),
			@Last_failure_step VARCHAR(100);

		SELECT @ServerName = [Server_Name],
			@JobName = [job_name]
		FROM [AFS_UTILITIES].[dbo].[Archived_failed_Job]
		WHERE instance_id = @instance_id;

		SELECT @Recent_failure_Step = [failed step],
			@Recent_failure_date = convert(VARCHAR, [time_of_error], 0)
		FROM [AFS_UTILITIES].[dbo].[Archived_failed_step]
		WHERE instance_id = @instance_id;

		SELECT @Last_failure_date = CONVERT(VARCHAR, time_of_error, 0),
			@Last_failure_step = [failed step]
		FROM [dbo].[Archived_failed_step]
		WHERE time_of_error = (
				SELECT max(time_of_error)
				FROM [dbo].[Archived_failed_step]
				WHERE notified = 1
					AND job_name = @JobName
				)

		--SET @Email_subject = CONCAT('(PRODUCTION)Failed process: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
		/*SET UP COLOR CODES*/
		--Prod (server name ends 0## or 1##): red  #C70039  
		IF @serverNumber LIKE '0%'
			OR @serverNumber LIKE '1%'
		BEGIN
			PRINT 'it''s a Prod Server'

			SET @color_high = '#C70039'
			SET @color_high_text = '#FFFFFF'
			SET @Email_subject = CONCAT (
					'**Critical** |PRODUCTION FAILURE| Process name: ',
					@JobName,
					'; Server: ',
					@ServerName,
					';Date: ',
					@Recent_failure_date
					);
				--SET @color_reg = ''
		END
				--Dev (server name ends in 2##): green  #52BE80
		ELSE IF @serverNumber LIKE '2%'
		BEGIN
			PRINT 'it''s a DEV Server'

			SET @color_high = '#1E8449'
			SET @color_high_text = '#FFFFFF'
			SET @Email_subject = CONCAT (
					'DEV Process Failure| Process name: ',
					@JobName,
					'; Server: ',
					@ServerName,
					';Date: ',
					@Recent_failure_date
					);
		END
				--Test (server name ends in 3##: blue  #2E86C1  
		ELSE IF @serverNumber LIKE '3%'
		BEGIN
			PRINT 'it''s a TEST Server'

			SET @color_high = '#2E86C1'
			SET @color_high_text = '#FFFFFF'
			SET @Email_subject = CONCAT (
					'TEST Process Failure| Process name: ',
					@JobName,
					'; Server: ',
					@ServerName,
					';Date: ',
					@Recent_failure_date
					);
		END
				--Stage (server name ends in 4##): yellow #F4D03F  -Black font
		ELSE IF @serverNumber LIKE '4%'
		BEGIN
			PRINT 'it''s a STAGE Server'

			SET @color_high = '#F4D03F'
			SET @color_high_text = '#273746'
			SET @Email_subject = CONCAT (
					'STAGE Process Failure| Process name: ',
					@JobName,
					'; Server: ',
					@ServerName,
					';Date: ',
					@Recent_failure_date
					);
		END
				--Jump Host (server name ends in 7##): black  #273746 - White
		ELSE IF @serverNumber LIKE '7%'
		BEGIN
			PRINT 'it''s a JUMP HOST Server'

			SET @color_high = '#273746'
			SET @color_high_text = '#FFFFFF'
			SET @Email_subject = CONCAT (
					'JUMP HOST Process Failure| Process name: ',
					@JobName,
					'; Server: ',
					@ServerName,
					';Date: ',
					@Recent_failure_date
					);
		END
				--DR (server name ends 9##): orange #D35400  -White
		ELSE IF @serverNumber LIKE '9%'
		BEGIN
			PRINT 'it''s a JUMP DR Server'

			SET @color_high = '#D35400'
			SET @color_high_text = '#FFFFFF'
			SET @Email_subject = CONCAT (
					'DR Process Failure| Process name: ',
					@JobName,
					'; Server: ',
					@ServerName,
					';Date: ',
					@Recent_failure_date
					);
		END
				--Lab (anything in USTEST): purple - #8E44AD
		ELSE
		BEGIN
			PRINT 'it''s a USTEST/LAB Server'

			SET @color_high = '#8E44AD'
			SET @color_high_text = '#FFFFFF'
			SET @Email_subject = CONCAT (
					'LAB Process Failure| Process name: ',
					@JobName,
					'; Server: ',
					@ServerName,
					';Date: ',
					@Recent_failure_date
					);
		END

		DECLARE @HTML_head VARCHAR(max) = '<!doctype html>
		<html>
		<head>
		<style>

		.header {
			text-align: center;
			font-size: small;
			text-transform: uppercase;
			word-spacing: 1ch;
			letter-spacing: 1ch;
			background-color: #169008;
			color: #FFFFFF;
		}

		.heading {
			text-align: center;
			text-transform: uppercase;
			word-spacing: 1ch;
			font-size: 300%;
			text-shadow: 7px 7px 15px #323030;
			letter-spacing: 1ch;

		}

		.background{
			background-color: #e4efe9
		}

		.box_high_level{
			background-color: ' + @color_high + ';
			width: 50%;
			text-align: center;
			font-size: large;
			margin-left: 25%;
			margin-right: 25%;
			display: inline-block;
			border-radius: 25px;
		}

		.box{
			background-color: ' + @color_reg + 
			';
			width: 90%;
			text-align: center;
			font-size: large;
			box-shadow: 0px 0px;
			margin-left: 5%;
			margin-right: 5%;
			min-height: 200px;
			display: inline-block;
		}

		.box_title_high{
			word-spacing: 1ch;
			text-transform: lowercase;
			letter-spacing: 1ch;
			text-align: center;
			font-family: Consolas, "Andale Mono", "Lucida Console", "Lucida Sans Typewriter", Monaco, "Courier New", monospace;
			font-size: large;
			color: ' + @color_high_text + ';
		}
		.box_title{
			word-spacing: 1ch;
			text-transform: lowercase;
			letter-spacing: 1ch;
			text-align: center;
			font-family: Consolas, "Andale Mono", "Lucida Console", "Lucida Sans Typewriter", Monaco, "Courier New", monospace;
			font-size: large;
			color: #FFFFFF;
		}
		.box_footer{
			word-spacing: 1ch;
			text-transform: lowercase;
			letter-spacing: 1ch;
			text-align: center;
			color: ' + @color_high_text + 
			';
			font-family: Consolas, "Andale Mono", "Lucida Console", "Lucida Sans Typewriter", Monaco, "Courier New", monospace;
			font-size: small;
		}

		.table_high{
			border-collapse: collapse;
			text-align: center;
			padding-right: 2%;
			padding-left: 2%;
			min-width: 98%;
			display: inline-table;
			background-color: ' + @color_high + ';
			color: ' + @color_high_text + ';
			font-family: Consolas, "Andale Mono", "Lucida Console", "Lucida Sans Typewriter", Monaco, "Courier New", monospace;
		}
		.table_detail{
			border-collapse: collapse;
			text-align: center;
			margin-top: auto;
			margin-left: auto;
			margin-bottom: auto;
			padding-right: 2%;
			padding-left: 2%;
			min-width: 98%;
			max-width: 98%;
			display: inline-table;
			background-color: ' + @color_reg + 
			';
			color: #FFFFFF;
			font-family: Consolas, "Andale Mono", "Lucida Console", "Lucida Sans Typewriter", Monaco, "Courier New", monospace;
		}
		th {
			padding: 8px;
			text-align: left;
			border-bottom: 1px solid #ddd;
			background-color: #85A3C2;
		}
		td {
			padding: 8px;
			text-align: left;
			border-bottom: 1px solid #ddd;
			font-size: small;
		}

		</style>
		</head>
		<body>
		<div class="background">
			<p class="header">AUTO GENERATED REPORT: SQL PROCESS FAILURE</p>
			<h3 class= "heading">Process failed</h3><br/>',
			@HTML_tail VARCHAR(max) = '<p class="header">THIS IS AN AUTOMATED EMAIL NOTIFICATION GENERATED FROM <b>' + @server_name_basic + '</b> on <b>' + @Current_Time + '</b> (' + @server_time_zone + '). FOR FURTHER INFORMATION PLEASE INVESTIGATE THE SQL SERVER LOG.</p>
</div>
</body>
</html>';
		DECLARE @HTML_Process_information_table VARCHAR(max) = '
		<div class = "box_high_level">
			<h3 class = "box_title_high">PROCESS INFORMATION</h3>
			<table class = "table_high">
				<tr>
					<td style = "text-align:right;">Server name: </td>
					<td >' + ISNULL(@ServerName, 'No record') + '</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Job name: </td>
					<td>' + ISNULL(@JobName, 'No record') + '</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Recent failure Step: </td>
					<td>' + ISNULL(@Recent_failure_Step, 'No record') + '</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Recent failure date/time: </td>
					<td>' + ISNULL(@Recent_failure_date, 'No record') + '</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Last failure date/time: </td>
					<td>' + ISNULL(@Last_failure_date, 'No record') + '</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Last failure step: </td>
					<td>' + ISNULL(@Last_failure_step, 
				'No record') + '</td>
				</tr>
			</table>
			<br/>
			<p class= "box_footer">Please find the error details in the rest of the email</p>
		</div>
		<br/><br/>';
		--SeLeCT @HTML_total =CONCAT(@HTML_head, @HTML_Process_information_table,@HTML_tail);
		--select @HTML_total;
		-- Job Info
		DECLARE @HTML_job_info_head VARCHAR(max) = '<div class = "box">
		<h3 class = "box_title">PROCESS DETAILS</h3>
		<table class = "table_detail">
			<tr>
				<th>Process name</th>
				<th>Step ID</th>
				<th>Step Name</th>
				<th>Step Type</th>
				<th>Last ran</th>
			</tr>',
			@HTML_job_info_tail VARCHAR(100) = '</table>
			<br/><br/>
			</div>
			<br/><br/>',
			@HTML_job_info_table VARCHAR(max) = '';

		-- put it in a temp table
		SELECT *
		INTO #Temp_failed_Job
		FROM [AFS_UTILITIES].[dbo].[Archived_failed_Job]
		WHERE [instance_id] = @instance_id;

		SET @row_count = (
				SELECT count(*)
				FROM #Temp_failed_Job
				);

		DECLARE @isSSIS INT = (
				SELECT COUNT(*)
				FROM #Temp_failed_Job
				WHERE [process_type] LIKE '%SSIS%'
				);

		--populate the table content
		WHILE (@row_count > 0)
		BEGIN
			DECLARE @ProcessName VARCHAR(255),
				@step_id VARCHAR(3),
				@StepName VARCHAR(255),
				@StepType VARCHAR(255),
				@LastRan VARCHAR(50);

			SELECT @ProcessName = [job_name],
				@step_id = [step_id],
				@StepName = [step_name],
				@StepType = [process_type],
				@LastRan = CONVERT(VARCHAR, [last_ran], 0)
			FROM #Temp_failed_Job
			WHERE step_id = (
					SELECT min(step_id)
					FROM #Temp_failed_Job
					);

			--remove from the temp table
			DELETE
			FROM #Temp_failed_Job
			WHERE step_id = @step_id;

			SET @row_count = (
					SELECT count(*)
					FROM #Temp_failed_Job
					);
			SET @HTML_job_info_table = CONCAT (
					@HTML_job_info_table,
					'<tr><td>',
					ISNULL(@ProcessName, 'No record'),
					'</td><td>',
					ISNULL(@step_id, 'No record'),
					'</td><td>',
					ISNULL(@StepName, 'No record'),
					'</td><td>',
					ISNULL(@StepType, 'No record'),
					'</td><td>',
					ISNULL(@LastRan, 'No record'),
					'</td></tr>'
					);
		END

		DECLARE @HTML_job_info_total VARCHAR(max) = CONCAT (
				@HTML_job_info_head,
				@HTML_job_info_table,
				@HTML_job_info_tail
				);

		DROP TABLE #Temp_failed_Job

		--Job step
		DECLARE @HTML_Job_step_head VARCHAR(max) = '<div class = "box">
		<h3 class = "box_title">Error Message</h3>
		<table class = "table_detail">
			<tr>
				<th>Process name</th>
				<th>Failed step</th>
				<th>Step Name</th>
				<th>Time of error</th>
				<th>Duration</th>
				<th>Error message</th>
			</tr>',
			@HTML_job_step_tail VARCHAR(100) = '</table>
			<br/><br/>
			</div>
			<br/><br/>',
			@HTML_job_step_table VARCHAR(max) = '';

		-- put it in a temp table
		SELECT *
		INTO #Temp_failed_Job_step
		FROM [AFS_UTILITIES].[dbo].[Archived_failed_step]
		WHERE [instance_id] = @instance_id;

		SET @row_count = (
				SELECT count(*)
				FROM #Temp_failed_Job_step
				);

		--populate the table content
		WHILE (@row_count > 0)
		BEGIN
			DECLARE @job_Name VARCHAR(255),
				@failedStep VARCHAR(255),
				@failed_step_name VARCHAR(100),
				@time_error VARCHAR(50),
				@duration VARCHAR(50),
				@error_message VARCHAR(max);

			SELECT @job_Name = [job_name],
				@failedStep = [failed step],
				@failed_step_name = [failed step],
				@time_error = CONVERT(VARCHAR, [time_of_error], 0),
				@duration = [duration],
				@error_message = [error_message]
			FROM #Temp_failed_Job_step

			SET @HTML_job_step_table = CONCAT (
					@HTML_job_step_table,
					'<tr><td>',
					ISNULL(@job_Name, 'No record'),
					'</td><td>',
					ISNULL(@failedStep, 'No record'),
					'</td><td>',
					ISNULL(@failed_step_name, 'No record'),
					'</td><td>',
					ISNULL(@time_error, 'No record'),
					'</td><td>',
					ISNULL(@duration, 'No record'),
					'</td><td>',
					ISNULL(@error_message, 'No record'),
					'</td></tr>'
					);

			--remove from the temp table
			DELETE
			FROM #Temp_failed_Job_step
			WHERE instance_id = @instance_id;

			SET @row_count = (
					SELECT count(*)
					FROM #Temp_failed_Job_step
					);
		END

		DROP TABLE #Temp_failed_Job_step;

		DECLARE @HTML_job_step_total VARCHAR(max) = CONCAT (
				@HTML_Job_step_head,
				@HTML_job_step_table,
				@HTML_job_step_tail
				);

		--SSIS table
		IF @isSSIS > 0
		BEGIN
			--Generate the highlevel SSIS information
			DECLARE @HTML_ssis_high_head VARCHAR(max) = '<div class = "box">
		<h3 class = "box_title">ssis package information</h3>
		<table class = "table_detail">
			<tr>
				<th>Folder name</th>
				<th>Project name</th>
				<th>Package name</th>
			</tr>',
				@HTML_ssis_high_tail VARCHAR(100) = '</table>
			<br/><br/>
			</div>
			<br/><br/>',
				@HTML_ssis_high_table VARCHAR(max) = '';

			--Create a temp table for ssis package information
			SELECT DISTINCT [Package Name],
				[Folder Name],
				[Project Name]
			INTO #Temp_SSIS_high
			FROM [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message]
			WHERE [instance_id] = @instance_id;

			SET @row_count = (
					SELECT count(*)
					FROM #Temp_SSIS_high
					);

			WHILE (@row_count > 0)
			BEGIN
				DECLARE @FolderName VARCHAR(128),
					@ProjectName VARCHAR(128),
					@PackageName_high VARCHAR(260);

				SELECT TOP 1 @PackageName_high = [Package Name],
					@FolderName = [Folder Name],
					@ProjectName = [Project Name]
				FROM #Temp_SSIS_high;

				SET @HTML_ssis_high_table = CONCAT (
						@HTML_ssis_high_table,
						'<tr><td>',
						ISNULL(@FolderName, 'No record'),
						'</td><td>',
						ISNULL(@ProjectName, 'No record'),
						'</td><td>',
						ISNULL(@PackageName_high, 'No record'),
						'</td></tr>'
						);

				DELETE
				FROM #Temp_SSIS_high
				WHERE [Package Name] = @PackageName_high;

				SET @row_count = (
						SELECT count(*)
						FROM #Temp_SSIS_high
						);
			END -- END of high level table

			DROP TABLE #Temp_SSIS_high

			DECLARE @HTML_ssis_high_total VARCHAR(max) = CONCAT (
					@HTML_ssis_high_head,
					@HTML_ssis_high_table,
					@HTML_ssis_high_tail
					);
			DECLARE @HTML_ssis_head VARCHAR(max) = '<div class = "box">
		<h3 class = "box_title">ssis error message</h3>
		<table class = "table_detail">
			<tr>
				<th>Package name</th>
				<th>Caller name</th>
				<th>Duration</th>
				<th>Start time</th>
				<th>Error time</th>
				<th>Error type</th>
				<th>Message source description</th>
				<th>Error Message</th>
			</tr>',
				@HTML_ssis_tail VARCHAR(100) = '</table>
			<br/><br/>
			</div>
			<br/><br/>',
				@HTML_ssis_table VARCHAR(max) = '';

			-- put it in a temp table
			SELECT *
			INTO #Temp_SSIS
			FROM [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message]
			WHERE [instance_id] = @instance_id;

			SET @row_count = (
					SELECT count(*)
					FROM #Temp_SSIS
					);

			--populate the table content
			WHILE (@row_count > 0)
			BEGIN
				DECLARE @PackageName VARCHAR(260),
					@CallerName VARCHAR(128),
					@Duration_ssis VARCHAR(128),
					@StartTime VARCHAR(50),
					@ErrorTime VARCHAR(50),
					@ErrorMessageType VARCHAR(300),
					@Messagesource VARCHAR(128),
					@ErrorMessage VARCHAR(max),
					@id INT;

				SELECT @id = [id],
					@PackageName = [Package Name],
					@CallerName = [Caller Name],
					@Duration_ssis = [Duration],
					@StartTime = CONVERT(VARCHAR, [Start Time], 0),
					@ErrorTime = CONVERT(VARCHAR, [Error Time], 0),
					@ErrorMessageType = [Error Message Type],
					@Messagesource = [Message source description],
					@ErrorMessage = [Error Message]
				FROM #Temp_SSIS
				WHERE [id] = (
						SELECT MAX([id])
						FROM #Temp_SSIS
						);

				SET @HTML_ssis_table = CONCAT (
						@HTML_ssis_table,
						'<tr><td>',
						ISNULL(@PackageName, 'No record'),
						'</td><td>',
						ISNULL(@CallerName, 'No record'),
						'</td><td>',
						ISNULL(@Duration_ssis, 'No record'),
						'</td><td>',
						ISNULL(@StartTime, 'No record'),
						'</td><td>',
						ISNULL(@ErrorTime, 'No record'),
						'</td><td>',
						ISNULL(@ErrorMessageType, 'No record'),
						'</td><td>',
						ISNULL(@Messagesource, 'No record'),
						'</td><td>',
						ISNULL(@ErrorMessage, 'No record'),
						'</td></tr>'
						);

				--remove from the temp table
				DELETE
				FROM #Temp_SSIS
				WHERE id = @id;

				SET @row_count = (
						SELECT count(*)
						FROM #Temp_SSIS
						);
			END --end of ssis while loop

			DROP TABLE #Temp_SSIS;

			DECLARE @HTML_ssis_total VARCHAR(max) = CONCAT (
					@HTML_ssis_head,
					@HTML_ssis_table,
					@HTML_ssis_tail
					);
		END --END of IF SSIS

		-- prepare the for th next row int he temp table for the loop: A) remove the instance ID from the temp table B) Increment the counter.
		DELETE
		FROM #temp_failed_job_Number
		WHERE instance_id = @instance_id;

		SET @Failed_Job_Number = @Failed_Job_Number - 1;

		--SET the notification to 1
		UPDATE [AFS_UTILITIES].[dbo].[Archived_failed_Job]
		SET [notified] = 1
		WHERE [instance_id] = @instance_id;

		UPDATE [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message]
		SET [notified] = 1
		WHERE [instance_id] = @instance_id;

		UPDATE [AFS_UTILITIES].[dbo].[Archived_failed_step]
		SET [notified] = 1
		WHERE [instance_id] = @instance_id;

		--Combine the HTML variables and send out the report
		SET @HTML_total = CONCAT (
				@HTML_head,
				@HTML_Process_information_table,
				@HTML_job_info_total,
				@HTML_job_step_total,
				@HTML_ssis_high_total,
				@HTML_ssis_total,
				@HTML_tail
				);

		SELECT @HTML_total;

		/*Send out the email*/
		EXEC msdb.dbo.sp_send_dbmail @profile_name = 'Administrator_SQL', --Need to Check Profile
			@recipients = @Email_address,
			@body_format = 'html',
			@body = @HTML_total,
			@subject = @Email_subject,
			@query_result_header = 0,
			@importance = 'High'
	END --END of while loop for individual failed job loop
END
GO

/*Create view: */
USE AFS_UTILITIES

IF EXISTS (
		SELECT *
		FROM sys.all_views
		WHERE name = 'View_Job_Failure_Human_Redable'
		)
BEGIN
	DROP VIEW [dbo].[View_Job_Failure_Human_Redable]
END
GO

CREATE VIEW [dbo].[View_Job_Failure_Human_Redable]
AS
SELECT instance_id,
	'Log_Date' = dateadd(SECOND, CASE 
			WHEN len(sjs.run_time) = 5
				THEN LEFT(sjs.run_time, 1) * 3600 + substring(cast(sjs.run_time AS VARCHAR), 2, 2) * 60 + right(sjs.run_time, 2)
			WHEN len(sjs.run_time) = 6
				THEN LEFT(sjs.run_time, 2) * 3600 + substring(cast(sjs.run_time AS VARCHAR), 3, 2) * 60 + right(sjs.run_time, 2)
			ELSE 0
			END, convert(DATETIME, cast(nullif(sjs.run_date, 0) AS NVARCHAR(10))))
FROM msdb.dbo.sysjobhistory sjs
WHERE [message] LIKE '%The job failed%'
GO

/*Create store procedure to check any job failure. If there exist one or many invoke Job_Failure_Report store procedure for every job failure*/
USE AFS_UTILITIES

/*Drop if exists*/
IF EXISTS (
		SELECT *
		FROM sysobjects
		WHERE id = object_id(N'[dbo].[check_job_failure]')
			AND OBJECTPROPERTY(id, N'IsProcedure') = 1
		)
BEGIN
	DROP PROCEDURE [dbo].[check_job_failure]
END
GO

CREATE PROCEDURE [dbo].[check_job_failure] @interval INT --in minutes
AS
DECLARE @job_fail_count INT = 0;
DECLARE @instance_id INT;

--DECLARE @interval int = 30;  --Mention the interval in Minutes how often it should chek for any job failure.
IF object_id('tempdb.. #job_failure_place_holder') IS NOT NULL
	DROP TABLE #job_failure_place_holder

CREATE TABLE #job_failure_place_holder (
	instance_id INT,
	Log_Date DATETIME
	);

INSERT INTO #job_failure_place_holder (
	instance_id,
	Log_Date
	)
SELECT instance_id,
	Log_Date
FROM [AFS_UTILITIES].[dbo].[View_Job_Failure_Human_Redable]
WHERE instance_id NOT IN (
		SELECT instance_id
		FROM [AFS_UTILITIES].[dbo].[Archived_failed_Job]
		)
	AND log_date >= dateadd(mi, - @interval, getdate());

SET @job_fail_count = (
		SELECT count(*)
		FROM #job_failure_place_holder
		);

WHILE (@job_fail_count > 0)
BEGIN
	SELECT TOP 1 instance_id AS [current_id]
	FROM #job_failure_place_holder;

	SET @instance_id = (
			SELECT TOP 1 instance_id
			FROM #job_failure_place_holder
			);

	EXECUTE [AFS_UTILITIES].[dbo].[Archive_Failed_Job] @instance_id = @instance_id;

	DELETE
	FROM #job_failure_place_holder
	WHERE instance_id = @instance_id;

	SET @job_fail_count = (
			SELECT count(*)
			FROM #job_failure_place_holder
			);
END
GO

/*Create the job that will invoke the store procedure*/
--Drop if EXISTS
IF EXISTS (
		SELECT *
		FROM msdb.dbo.sysjobs
		WHERE name = N'Report: failed job'
		)
BEGIN
	EXEC msdb.dbo.sp_delete_job @job_name = N'Report: failed job';
END
GO

USE [msdb]
GO

DECLARE @jobId BINARY (16)

EXEC msdb.dbo.sp_add_job @job_name = N'Report: failed job',
	@enabled = 1,
	@notify_level_eventlog = 0,
	@notify_level_email = 2,
	@notify_level_netsend = 2,
	@notify_level_page = 2,
	@delete_level = 0,
	@category_name = N'Data Collector',
	@owner_login_name = N'sa',
	@notify_email_operator_name = N'AFS.DBA.DIST',
	@job_id = @jobId OUTPUT

SELECT @jobId AS JobID;
GO

DECLARE @server_name_basic VARCHAR(255) = (
		SELECT cast(serverproperty('servername') AS VARCHAR(255))
		);

EXEC msdb.dbo.sp_add_jobserver @job_name = N'Report: failed job',
	@server_name = @server_name_basic
GO

USE [msdb]
GO

EXEC msdb.dbo.sp_add_jobstep @job_name = N'Report: failed job',
	@step_name = N'Check new failed job',
	@step_id = 1,
	@cmdexec_success_code = 0,
	@on_success_action = 3,
	@on_fail_action = 2,
	@retry_attempts = 1,
	@retry_interval = 0,
	@os_run_priority = 0,
	@subsystem = N'TSQL',
	@command = N'EXEC [AFS_UTILITIES].[dbo].[check_job_failure] @interval = 1440;',
	@database_name = N'AFS_UTILITIES',
	@flags = 16
GO

USE [msdb]
GO

EXEC msdb.dbo.sp_add_jobstep @job_name = N'Report: failed job',
	@step_name = N'Send out the report',
	@step_id = 2,
	@cmdexec_success_code = 0,
	@on_success_action = 1,
	@on_fail_action = 2,
	@retry_attempts = 1,
	@retry_interval = 0,
	@os_run_priority = 0,
	@subsystem = N'TSQL',
	@command = N'EXEC [AFS_UTILITIES].[dbo].[Generate_Job_Error_Report] @Email_address = ''AFS.DBA.OPS.DIST-AccentureFederal-com@Afs365.onMicrosoft.com''',
	@database_name = N'AFS_UTILITIES',
	@flags = 16
GO

USE [msdb]
GO

EXEC msdb.dbo.sp_update_job @job_name = N'Report: failed job',
	@enabled = 1,
	@start_step_id = 1,
	@notify_level_eventlog = 0,
	@notify_level_email = 2,
	@notify_level_netsend = 2,
	@notify_level_page = 2,
	@delete_level = 0,
	@description = N'',
	@category_name = N'Data Collector',
	@owner_login_name = N'sa',
	@notify_email_operator_name = N'AFS.DBA.DIST',
	@notify_netsend_operator_name = N'',
	@notify_page_operator_name = N''
GO

USE [msdb]
GO

DECLARE @schedule_id INT

EXEC msdb.dbo.sp_add_jobschedule @job_name = N'Report: failed job',
	@name = N'Interval: 15 Mins',
	@enabled = 1,
	@freq_type = 4,
	@freq_interval = 1,
	@freq_subday_type = 4,
	@freq_subday_interval = 15,
	@freq_relative_interval = 0,
	@freq_recurrence_factor = 1,
	@active_start_date = 20180119,
	@active_end_date = 99991231,
	@active_start_time = 0,
	@active_end_time = 235959,
	@schedule_id = @schedule_id OUTPUT

SELECT @schedule_id AS ScheduleID;
GO


