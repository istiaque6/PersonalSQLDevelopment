--Remove the previous implementation
USE [msdb]
GO

/*Drop if exists*/

--remove the trigger from V1
IF EXISTS ( select * from sysobjects where name ='trig_check_for_job_failure' and [type] = 'TR' )
BEGIN
    DROP TRIGGER [dbo].[trig_check_for_job_failure];
END
GO

--remove the previous job from V1
IF EXISTS ( SELECT * FROM msdb.dbo.sysjobs WHERE name = N'SEND SQL JOB ALERTS' )
BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name=N'SEND SQL JOB ALERTS';
END
GO

IF EXISTS ( SELECT * FROM msdb.dbo.sysjobs WHERE name = N'AUTOMATION (Job failure check)' )
	BEGIN
		EXEC msdb.dbo.sp_delete_job @job_name=N'AUTOMATION (Job failure check)';
	END
GO


USE [AFS_UTILITIES]
GO

--remove the store proc from V2
IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[Job_Failure_Report]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
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
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'Archived_failed_Job')
	BEGIN
	
			CREATE TABLE [dbo].[Archived_failed_Job]
		(
			[Server_Name]		varchar (255),
			[instance_id]		int
		,	[job_name]          varchar(255)
		,   [step_id]           int not null
		,   [step_name]         varchar(255)
		,   [process_type]      varchar(255)
		,   [last_ran]          [datetime]
		,	[notified]			[bit]
		);
	END

--Archive for Job Steps (Non SSIS job records normally recorded in this table)
IF  NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'Archived_failed_step')
BEGIN

create table [dbo].[Archived_failed_step]
	(
		[instance_id]		int
	,	[job_name]			varchar(255)
	,   [failed step]		INT
	,	[step name]			varchar(100)
	,	[time_of_error]     [datetime]
	,   [duration]			varchar(255)
	,   [error_message]     varchar(max)
	,	[notified]			[bit]
	);

END


	
--Archive for SSIS error log 
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'Archived_failed_SSIS_message')
BEGIN

CREATE TABLE [dbo].[Archived_failed_SSIS_message](
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
	[id] [int] IDENTITY(1,1) NOT NULL
);

END

IF NOT EXISTS (SELECT * FROM sys.indexes 
				WHERE name='IDX-InstanceID' AND object_id = OBJECT_ID('[dbo].[Archived_failed_step]'))
	BEGIN
		CREATE CLUSTERED INDEX [IDX-InstanceID] ON [dbo].[Archived_failed_step]
		(
			[instance_id] ASC
		)
		WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 70)
	END

IF NOT EXISTS (SELECT * FROM sys.indexes 
				WHERE name='IDX-InstanceID' AND object_id = OBJECT_ID('[dbo].[Archived_failed_Job]'))
	BEGIN
		CREATE CLUSTERED INDEX [IDX-InstanceID] ON [dbo].[Archived_failed_Job]
		(
			[instance_id] ASC
		)
		WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 70)
	END

--DROP THE PREVIOUSLY CREATED INDEX
IF EXISTS (SELECT * FROM sys.indexes 
				WHERE name='PK_Archived_failed_SSIS_message' AND object_id = OBJECT_ID('[dbo].[Archived_failed_SSIS_message]'))
	BEGIN
		ALTER TABLE [dbo].[Archived_failed_SSIS_message] DROP CONSTRAINT [PK_Archived_failed_SSIS_message] WITH ( ONLINE = OFF )
	END


IF NOT EXISTS (SELECT * FROM sys.indexes 
				WHERE name='IDX-InstanceID' AND object_id = OBJECT_ID('[dbo].[Archived_failed_SSIS_message]'))
	BEGIN
		CREATE CLUSTERED INDEX [IDX-instanceID] ON [dbo].[Archived_failed_SSIS_message]
		(
			[instance_id] ASC
		)
		 WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 70)
	END
	
	


			

/*Create the store procedure. This store procedure will scan the error log for reporting*/

USE AFS_UTILITIES
GO

/*Drop if exists*/
IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[Archive_Failed_Job]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
BEGIN
    DROP PROCEDURE [dbo].[Archive_Failed_Job]
END
GO

/*This proc will generate the report based on the instance ID passed to it.*/
CREATE PROCEDURE [dbo].[Archive_Failed_Job]
						@instance_id int
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	--get basic server info.
	DECLARE @server_name_basic varchar(255)= (select cast(serverproperty('servername') as varchar(255)));
	DECLARE @Current_Time varchar (50) = (select convert(varchar, getdate(), 0));
	DECLARE @server_time_zone varchar(255);
	exec master.dbo.xp_regread 'hkey_local_machine', 'system\currentcontrolset\control\timezoneinformation','timezonekeyname', @server_time_zone out;

	--Variables
	--DECLARE @P_statement varchar(max), @ParmDefinition varchar(max);
	DECLARE @last_error varchar(255), @last_error_job_name varchar(255), @failed_job_id varchar (255);
	
	
	/*Find the failed job and step name*/
	SET @last_error = @instance_id;	
	SELECT @last_error_job_name = sj.name, @failed_job_id = sj.job_id from msdb.dbo.sysjobs sj join msdb.dbo.sysjobhistory sjh on sj.job_id = sjh.job_id where instance_id = @last_error;

	
	/*****************Find the latest failed job information*****************/

	--Insert new records into the Archive table:
	INSERT INTO [AFS_UTILITIES].[dbo].[Archived_failed_Job] ([Server_Name],[instance_id],[job_name],[step_id],[step_name],[process_type],[last_ran],[notified])
	select
	@server_name_basic,	
	@instance_id,
	sj.name, 
	sjs.step_id,
	sjs.step_name,
	sjs.subsystem, 
	dateadd(SECOND, 
	CASE
			when len(sjs.last_run_time) = 5 then LEFT(sjs.last_run_time, 1)*3600 +substring(cast (sjs.last_run_time as varchar), 2,2) * 60 + right(sjs.last_run_time,2)
			when len(sjs.last_run_time) = 6 then LEFT(sjs.last_run_time, 2)*3600 +substring(cast (sjs.last_run_time as varchar), 3,2) * 60 + right(sjs.last_run_time,2)
		else 0 
		end
		,convert(datetime,cast(nullif(sjs.last_run_date,0) as nvarchar(10)))),
	0 --Bit Value for notification
		from
		msdb..sysjobs sj join msdb..sysjobsteps sjs on sj.job_id = sjs.job_id
	where
		sj.name = @last_error_job_name
	order by
		sj.name, sjs.step_id asc;


 /*****************Find the latest step error (SQL) *****************/

 	--Find the step error message (looping back)

	DECLARE @break int = 0;
	WHILE (@break < 1)
	BEGIN
		SET @last_error = @last_error -1;
		IF ((SELECT COUNT(*) FROM msdb..sysjobhistory where run_status = 0 and instance_id =  @last_error and job_id = @failed_job_id and message like '%The step failed%') > 0)
			SET @break = 1;
	END

	--Insert the record in the Archived table:
	INSERT INTO [AFS_UTILITIES].[dbo].[Archived_failed_step] ([instance_id],[job_name],[failed step],[step name],[time_of_error],[duration], [error_message],[notified])
    select	@instance_id
	,	'job name'			= sj.name
	,	'failed step'		= sjh.step_id
	,	'step name'			=sjh.step_name
	,	'time of error'		= msdb.dbo.agent_datetime(run_date, run_time)
	,	'duration'			= CAST(sjh.run_duration/10000 as varchar)  + ':' + CAST(sjh.run_duration/100%100 as varchar) + ':' + CAST(sjh.run_duration%100 as varchar)
	,	'error message'		= sjh.message
	,	0 --Bit value for notification
	from
		msdb..sysjobs sj 
		join msdb..sysjobhistory sjh on sj.job_id = sjh.job_id
	where
		instance_id = @last_error
	order by sj.name, sjh.step_id asc;

	
	/*****************Find the SSIS error if there is any SSIS step *****************/

	--Check if there is any SSIS step in the job, if So, then it will create a table for SSIS error log
	DECLARE @SSIS_Step int = (SELECT COUNT(*) FROM [AFS_UTILITIES].[dbo].[Archived_failed_step] step WHERE step.[instance_id] = @instance_id AND 'SSIS' = (SELECT process_type FROM [AFS_UTILITIES].[dbo].[Archived_failed_Job] job WHERE job.[instance_id] = @instance_id AND job.step_id = step.[failed step]));

	IF @SSIS_Step > 0
		BEGIN
		--FIND THE SSIS PACKAGE NAMES
		--select @@SERVERNAME as [check]
		
		--Extract the package Name
		DECLARE @SSIS_Package varchar (100) = 
		(SELECT REVERSE(SUBSTRING( RIGHT(REVERSE(command), LEN(command)  - PATINDEX('%\xstd.%', REVERSE(command))) , 0, CHARINDEX('\', RIGHT(REVERSE(command), LEN(command) - PATINDEX('%\xstd.%', REVERSE(command)))))) 
		from msdb.[dbo].[sysjobsteps]
		where subsystem = 'SSIS' and job_id = @failed_job_id and step_id = (SELECT [failed step] FROM [AFS_UTILITIES].[dbo].[Archived_failed_step] step WHERE step.[instance_id] = @instance_id ));

		SELECT @SSIS_Package;

	/*	Store SSIS error information into the Archive table.*/
		INSERT INTO [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message]
           ([instance_id]
           ,[Folder Name]
           ,[Project Name]
           ,[Package Name]
           ,[Caller Name]
           ,[Duration]
           ,[Start Time]
           ,[End Time]
           ,[Error Time]
           ,[Error Message Type]
           ,[Message source description]
           ,[Error Message]
		   ,[notified] )
    
	(		SELECT
			@instance_id,
			EX.folder_name,
			EX.project_name,
			EX.package_name,
			'caller_name' = O.caller_name,
			--EX.start_time,EX.end_time,OM.message_time,
			convert(varchar(5),DateDiff(s, EX.[start_time], EX.[end_time])/3600)+':'+convert(varchar(5),DateDiff(s, EX.[start_time], EX.[end_time])%3600/60)+':'+convert(varchar(5),(DateDiff(s, EX.[start_time], EX.[end_time])%60)) as [duration],
			LEFT(CONVERT (varchar, EX.start_time,0),19) AS [start_time],
			LEFT(CONVERT (varchar, EX.end_time,0),19) AS [End_time],
			--EX.end_time,
			--EX.status,
			LEFT(CONVERT (varchar, OM.message_time,0),19) AS [Error Time],
			--LEFT(CONVERT(VARCHAR, OM.message_time, 0), 19),
			'Error Message type' = EM.message_desc,
			'message_source_desc' = D.message_source_desc, 
			 OM.message as [error_message],
			 0 --Bit value to store if this row is emailed or not.
		FROM
			SSISDB.catalog.operation_messages AS OM
			INNER JOIN
				SSISDB.catalog.operations AS O
				ON O.operation_id = OM.operation_id
			INNER JOIN
			(
				VALUES
					(-1,'Unknown')
				,   (120,'Error')
				,   (110,'Warning')
				,   (70,'Information')
				,   (10,'Pre-validate')
				,   (20,'Post-validate')
				,   (30,'Pre-execute')
				,   (40,'Post-execute')
				,   (60,'Progress')
				,   (50,'StatusChange')
				,   (100,'QueryCancel')
				,   (130,'TaskFailed')
				,   (90,'Diagnostic')
				,   (200,'Custom')
				,   (140,'DiagnosticEx Whenever an Execute Package task executes a child package, it logs this event. The event message consists of the parameter values passed to child packages.  The value of the message column for DiagnosticEx is XML text.')
				,   (400,'NonDiagnostic')
				,   (80,'VariableValueChanged')
			) EM (message_type, message_desc)
				ON EM.message_type = OM.message_type
			INNER JOIN
			(
				VALUES
					(10,'Entry APIs, such as T-SQL and CLR Stored procedures')
				,   (20,'External process used to run package (ISServerExec.exe)')
				,   (30,'Package-level objects')
				,   (40,'Control Flow tasks')
				,   (50,'Control Flow containers')
				,   (60,'Data Flow task')
			) D (message_source_type, message_source_desc)
				ON D.message_source_type = OM.message_source_type
			INNER JOIN SSISDB.[catalog].[executions] AS EX
				ON EX.object_id = o.object_id


		WHERE
			OM.operation_id = 
			(  
				SELECT 
					MAX(OM.operation_id)
				FROM
					SSISDB.catalog.operation_messages AS OM
				WHERE
					OM.message_type = 120 or OM.message_type = 130
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
IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[Generate_Job_Error_Report]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
BEGIN
    DROP PROCEDURE [dbo].[Generate_Job_Error_Report]
END
GO

/*This proc will generate the report based on the instance ID passed to it.*/
CREATE PROCEDURE [dbo].[Generate_Job_Error_Report] @Email_address varchar (MAX)
AS
BEGIN
	
	/*check if there is any job failure that has not been notified/emailed yet*/
	
	--create a temp table to hold the instance ID.
	if object_id('tempdb..#temp_failed_job_Number') is not null
		drop table #temp_failed_job_Number
 
	create table #temp_failed_job_Number
	(
		[instance_id]	int,
	)
		
	INSERT INTO #temp_failed_job_Number ([instance_id])
	SELECT DISTINCT instance_id from [AFS_UTILITIES].[dbo].[Archived_failed_Job] WHERE notified = 0;

	

	DECLARE @Failed_Job_Number int = (select count(*) from #temp_failed_job_Number)
	
	--if there is then send the details in individual emails for each jobs.
	WHILE (@Failed_Job_Number > 0)
	BEGIN
		DECLARE @row_count int = 0;
		--Pick one of the failed instance ID
		DECLARE @instance_id int = (select top 1 [instance_id] from #temp_failed_job_Number);
		
		--process the report
		DECLARE @server_name_basic varchar(255)= (select cast(serverproperty('servername') as varchar(255)));
		DECLARE @Current_Time varchar (50) = (select convert(varchar, getdate(), 0));
		--DECLARE @server_name_instance_name varchar(255) = (select replace(cast(serverproperty('servername') as varchar(255)), '\', ' SQL Instance: '));
		DECLARE @server_time_zone varchar(255);
		exec master.dbo.xp_regread 'hkey_local_machine', 'system\currentcontrolset\control\timezoneinformation','timezonekeyname', @server_time_zone out;
		DECLARE @HTML_total varchar (max)='', @color_high varchar (20),@color_reg varchar(20) = '#336699', @color_high_text varchar (20) , @Email_subject varchar (500);

		DECLARE @serverName_Temp varchar (128) = (SELECT cast(SERVERPROPERTY('MachineName') as varchar))
		DECLARE @serverNumber varchar(3) = (SELECT SUBSTRING(@serverName_Temp,LEN(@serverName_Temp)-2,3))

		
		--Prepare the process information
		DECLARE @ServerName varchar(100), @JobName varchar(100), @Recent_failure_Step varchar(100),@Recent_failure_date varchar(100),@Last_failure_date varchar(100),@Last_failure_step varchar(100);

		SELECT @ServerName= [Server_Name], @JobName=[job_name] from [AFS_UTILITIES].[dbo].[Archived_failed_Job] where instance_id = @instance_id;
		SELECT @Recent_failure_Step = [failed step], @Recent_failure_date = convert(varchar, [time_of_error],0) from [AFS_UTILITIES].[dbo].[Archived_failed_step] where instance_id = @instance_id;
		select @Last_failure_date = CONVERT(varchar, time_of_error,0),@Last_failure_step= [failed step] from [dbo].[Archived_failed_step] where time_of_error = (select max(time_of_error) FROM [dbo].[Archived_failed_step] where notified = 1 and job_name = @JobName)
		--SET @Email_subject = CONCAT('(PRODUCTION)Failed process: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);


		/*SET UP COLOR CODES*/
		--Prod (server name ends 0## or 1##): red  #C70039  
		IF @serverNumber like '0%' or @serverNumber like '1%'
			BEGIN
				PRINT 'it''s a Prod Server'
				SET @color_high = '#C70039'
				SET @color_high_text = '#FFFFFF'
				SET @Email_subject = CONCAT('**Critical** |PRODUCTION FAILURE| Process name: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
				--SET @color_reg = ''
			END
		--Dev (server name ends in 2##): green  #52BE80
		ELSE IF @serverNumber like '2%'
			BEGIN
				PRINT 'it''s a DEV Server'
				SET @color_high = '#1E8449'
				SET @color_high_text = '#FFFFFF'
				SET @Email_subject = CONCAT('DEV Process Failure| Process name: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
			END


		--Test (server name ends in 3##: blue  #2E86C1  
		ELSE IF @serverNumber like '3%'
			BEGIN
				PRINT 'it''s a TEST Server'
				SET @color_high = '#2E86C1'
				SET @color_high_text = '#FFFFFF'
				SET @Email_subject = CONCAT('TEST Process Failure| Process name: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
			END
		--Stage (server name ends in 4##): yellow #F4D03F  -Black font
		ELSE IF @serverNumber like '4%'
			BEGIN
				PRINT 'it''s a STAGE Server'
				SET @color_high = '#F4D03F'
				SET @color_high_text = '#273746'
				SET @Email_subject = CONCAT('STAGE Process Failure| Process name: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
			END
		--Jump Host (server name ends in 7##): black  #273746 - White
		ELSE IF @serverNumber like '7%'
			BEGIN
				PRINT 'it''s a JUMP HOST Server'
				SET @color_high = '#273746'
				SET @color_high_text = '#FFFFFF'
				SET @Email_subject = CONCAT('JUMP HOST Process Failure| Process name: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
			END

		--DR (server name ends 9##): orange #D35400  -White
		ELSE IF @serverNumber like '9%'
			BEGIN
				PRINT 'it''s a JUMP DR Server'
				SET @color_high = '#D35400'
				SET @color_high_text = '#FFFFFF'
				SET @Email_subject = CONCAT('DR Process Failure| Process name: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
			END
		--Lab (anything in USTEST): purple - #8E44AD
		ELSE
			BEGIN
				PRINT 'it''s a USTEST/LAB Server'
				SET @color_high = '#8E44AD'
				SET @color_high_text = '#FFFFFF'
				SET @Email_subject = CONCAT('LAB Process Failure| Process name: ',@JobName,'; Server: ',@ServerName,';Date: ',@Recent_failure_date);
			END



		DECLARE @HTML_head varchar (max) =
		'<!doctype html>
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
			background-color: '+@color_high+';
			width: 50%;
			text-align: center;
			font-size: large;
			margin-left: 25%;
			margin-right: 25%;
			display: inline-block;
			border-radius: 25px;
		}

		.box{
			background-color: '+@color_reg+';
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
			color: '+@color_high_text+';
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
			color: '+@color_high_text+';
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
			background-color: '+@color_high+';
			color: '+@color_high_text+';
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
			background-color: '+@color_reg+';
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
		
		@HTML_tail varchar(max) = 
		'<p class="header">THIS IS AN AUTOMATED EMAIL NOTIFICATION GENERATED FROM <b>'+@server_name_basic+'</b> on <b>'+@Current_Time +'</b> ('+@server_time_zone+'). FOR FURTHER INFORMATION PLEASE INVESTIGATE THE SQL SERVER LOG.</p>
</div>
</body>
</html>';


		

		DECLARE @HTML_Process_information_table varchar (max) = '
		<div class = "box_high_level">
			<h3 class = "box_title_high">PROCESS INFORMATION</h3>
			<table class = "table_high">
				<tr>
					<td style = "text-align:right;">Server name: </td>
					<td >'+ISNULL(@ServerName,'No record')+'</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Job name: </td>
					<td>'+ISNULL(@JobName,'No record')+'</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Recent failure Step: </td>
					<td>'+ISNULL(@Recent_failure_Step,'No record')+'</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Recent failure date/time: </td>
					<td>'+ISNULL(@Recent_failure_date,'No record') +'</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Last failure date/time: </td>
					<td>'+ISNULL(@Last_failure_date,'No record')+'</td>
				</tr>
				<tr>
					<td style = "text-align:right;">Last failure step: </td>
					<td>'+ISNULL(@Last_failure_step,'No record')+'</td>
				</tr>
			</table>
			<br/>
			<p class= "box_footer">Please find the error details in the rest of the email</p>
		</div>
		<br/><br/>';
		--SeLeCT @HTML_total =CONCAT(@HTML_head, @HTML_Process_information_table,@HTML_tail);
		--select @HTML_total;
		
		-- Job Info
		DECLARE @HTML_job_info_head varchar (max) = 
		'<div class = "box">
		<h3 class = "box_title">PROCESS DETAILS</h3>
		<table class = "table_detail">
			<tr>
				<th>Process name</th>
				<th>Step ID</th>
				<th>Step Name</th>
				<th>Step Type</th>
				<th>Last ran</th>
			</tr>',
			@HTML_job_info_tail varchar (100) =
			'</table>
			<br/><br/>
			</div>
			<br/><br/>',
			@HTML_job_info_table varchar (max) = '';

			-- put it in a temp table

			select * into #Temp_failed_Job from [AFS_UTILITIES].[dbo].[Archived_failed_Job] where [instance_id] = @instance_id;
			SET @row_count = (select count(*) from #Temp_failed_Job);
			
			DECLARE @isSSIS int = (SELECT COUNT(*) from #Temp_failed_Job where [process_type] like '%SSIS%');
			--populate the table content
			while (@row_count > 0)
			BEGIN

				DECLARE @ProcessName varchar(255), @step_id varchar (3), @StepName varchar (255), @StepType varchar (255), @LastRan varchar (50);
				SELECT @ProcessName = [job_name],@step_id = [step_id], @StepName = [step_name],@StepType = [process_type], @LastRan = CONVERT(varchar,[last_ran] , 0) from #Temp_failed_Job where step_id = (select min(step_id) from #Temp_failed_Job);
				--remove from the temp table
				delete from #Temp_failed_Job where step_id = @step_id;
				set @row_count = (select count (*) from #Temp_failed_Job);
				SET @HTML_job_info_table = CONCAT(@HTML_job_info_table,'<tr><td>',ISNULL(@ProcessName,'No record'),'</td><td>',ISNULL(@step_id,'No record'),'</td><td>',ISNULL(@StepName,'No record'),'</td><td>',ISNULL(@StepType,'No record'),'</td><td>',ISNULL(@LastRan,'No record'),'</td></tr>');

			END
			DECLARE @HTML_job_info_total varchar (max) = CONCAT(@HTML_job_info_head,@HTML_job_info_table,@HTML_job_info_tail);

			DROP table #Temp_failed_Job
			
			
			--Job step
			DECLARE @HTML_Job_step_head varchar (max) = '<div class = "box">
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
			@HTML_job_step_tail varchar (100) =
			'</table>
			<br/><br/>
			</div>
			<br/><br/>',
			@HTML_job_step_table varchar (max) = '';
			
			-- put it in a temp table

			select * into #Temp_failed_Job_step from [AFS_UTILITIES].[dbo].[Archived_failed_step] where [instance_id] = @instance_id;
			SET @row_count = (select count(*) from #Temp_failed_Job_step);
			
			--populate the table content
			while (@row_count > 0)
			BEGIN
				DECLARE @job_Name varchar (255), @failedStep varchar (255), @failed_step_name varchar(100),@time_error varchar (50), @duration varchar (50), @error_message varchar (max);
				SELECT @job_Name = [job_name], @failedStep= [failed step], @failed_step_name =  [failed step],@time_error = CONVERT(varchar , [time_of_error], 0), @duration = [duration], @error_message = [error_message]
				from #Temp_failed_Job_step
				SET @HTML_job_step_table = CONCAT(@HTML_job_step_table, '<tr><td>', ISNULL(@job_Name,'No record'),'</td><td>',ISNULL(@failedStep,'No record'),'</td><td>',ISNULL(@failed_step_name,'No record'),'</td><td>',ISNULL(@time_error,'No record'),'</td><td>',ISNULL(@duration,'No record'),'</td><td>',ISNULL(@error_message,'No record'),'</td></tr>');


			--remove from the temp table
				delete from #Temp_failed_Job_step where instance_id = @instance_id;
				set @row_count = (select count (*) from #Temp_failed_Job_step);
			END

			DROP table #Temp_failed_Job_step;
			DECLARE @HTML_job_step_total varchar (max) = CONCAT(@HTML_Job_step_head,@HTML_job_step_table,@HTML_job_step_tail);
			

			--SSIS table

			if @isSSIS > 0
			BEGIN

			--Generate the highlevel SSIS information
			DECLARE @HTML_ssis_high_head varchar (max) =  
			'<div class = "box">
		<h3 class = "box_title">ssis package information</h3>
		<table class = "table_detail">
			<tr>
				<th>Folder name</th>
				<th>Project name</th>
				<th>Package name</th>
			</tr>',
			@HTML_ssis_high_tail varchar (100) =
			'</table>
			<br/><br/>
			</div>
			<br/><br/>',
			@HTML_ssis_high_table varchar (max) = '';

			--Create a temp table for ssis package information
			select DISTINCT  [Package Name], [Folder Name] ,[Project Name]into #Temp_SSIS_high from [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message] where [instance_id] = @instance_id;
			SET @row_count = (select count(*) from #Temp_SSIS_high);

			WHILE (@row_count > 0)

			BEGIN
				DECLARE  @FolderName varchar (128),@ProjectName varchar (128), @PackageName_high varchar (260);
				SELECT top 1  @PackageName_high= [Package Name], @FolderName=[Folder Name] ,@ProjectName=[Project Name] from  #Temp_SSIS_high;

				SET @HTML_ssis_high_table = CONCAT(@HTML_ssis_high_table, '<tr><td>',ISNULL(@FolderName,'No record'),'</td><td>',ISNULL(@ProjectName,'No record'),'</td><td>',ISNULL(@PackageName_high,'No record'),'</td></tr>');

				delete from #Temp_SSIS_high where [Package Name] = @PackageName_high;
				SET @row_count = (select count(*) from #Temp_SSIS_high);

			END-- END of high level table
			drop table #Temp_SSIS_high
			
			DECLARE @HTML_ssis_high_total varchar (max) = CONCAT(@HTML_ssis_high_head,@HTML_ssis_high_table,@HTML_ssis_high_tail);




			DECLARE @HTML_ssis_head varchar (max) = '<div class = "box">
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
			@HTML_ssis_tail varchar (100) =
			'</table>
			<br/><br/>
			</div>
			<br/><br/>',
			@HTML_ssis_table varchar (max) = '';

			




			-- put it in a temp table

			select * into #Temp_SSIS from [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message] where [instance_id] = @instance_id;
			SET @row_count = (select count(*) from #Temp_SSIS);
			
			--populate the table content
			while (@row_count > 0)
			BEGIN
			
			DECLARE  @PackageName varchar (260), @CallerName varchar (128), @Duration_ssis varchar (128), @StartTime varchar (50), @ErrorTime varchar (50),@ErrorMessageType varchar (300), @Messagesource varchar (128), @ErrorMessage varchar (max), @id int;

			select @id = [id], @PackageName = [Package Name], @CallerName = [Caller Name], @Duration_ssis = [Duration], @StartTime = CONVERT(varchar, [Start Time],0), @ErrorTime =CONVERT(varchar, [Error Time],0) ,@ErrorMessageType = [Error Message Type], @Messagesource = [Message source description], @ErrorMessage =  [Error Message] FROM #Temp_SSIS where [id] = (select MAX([id]) from #Temp_SSIS);

			set @HTML_ssis_table = CONCAT(@HTML_ssis_table,'<tr><td>', ISNULL( @PackageName,'No record'),'</td><td>',ISNULL(@CallerName,'No record'),'</td><td>',ISNULL(@Duration_ssis,'No record'),'</td><td>',ISNULL( @StartTime,'No record'),'</td><td>',ISNULL(@ErrorTime,'No record'),'</td><td>',ISNULL(@ErrorMessageType,'No record'),'</td><td>',ISNULL(@Messagesource,'No record'),'</td><td>',ISNULL(@ErrorMessage,'No record'),'</td></tr>');
			
			--remove from the temp table
			delete from #Temp_SSIS where id = @id;

			set @row_count = (select count (*) from #Temp_SSIS);
			
			END --end of ssis while loop
			
			DROP table #Temp_SSIS;
			DECLARE @HTML_ssis_total varchar (max) = CONCAT(@HTML_ssis_head,@HTML_ssis_table,@HTML_ssis_tail);
		

			END --END of IF SSIS


			
		-- prepare the for th next row int he temp table for the loop: A) remove the instance ID from the temp table B) Increment the counter.
		delete from #temp_failed_job_Number where instance_id = @instance_id;
		SET @Failed_Job_Number = @Failed_Job_Number -1;

		--SET the notification to 1
		update  [AFS_UTILITIES].[dbo].[Archived_failed_Job] SET [notified] = 1 WHERE [instance_id] = @instance_id;
		update  [AFS_UTILITIES].[dbo].[Archived_failed_SSIS_message] SET [notified] = 1 WHERE [instance_id] = @instance_id;
		update  [AFS_UTILITIES].[dbo].[Archived_failed_step] SET [notified] = 1 WHERE [instance_id] = @instance_id;

		--Combine the HTML variables and send out the report
		set @HTML_total = CONCAT(@HTML_head, @HTML_Process_information_table, @HTML_job_info_total,@HTML_job_step_total,@HTML_ssis_high_total,@HTML_ssis_total,@HTML_tail);
		SELECT @HTML_total;

		
		/*Send out the email*/
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'Administrator_SQL',--Need to Check Profile
		@recipients = @Email_address,
		@body_format='html',
		@body = @HTML_total,
		@subject = @Email_subject,
		@query_result_header=0,
		@importance= 'High'



	END --END of while loop for individual failed job loop
	


END
GO


/*Create view: */
use AFS_UTILITIES
IF EXISTS(select * FROM sys.all_views where name = 'View_Job_Failure_Human_Redable')
BEGIN
    DROP VIEW [dbo].[View_Job_Failure_Human_Redable]
END
GO

create view [dbo].[View_Job_Failure_Human_Redable]
as
select
instance_id, 
'Log_Date' = dateadd(SECOND, 
	CASE
			when len(sjs.run_time) = 5 then LEFT(sjs.run_time, 1)*3600 +substring(cast (sjs.run_time as varchar), 2,2) * 60 + right(sjs.run_time,2)
			when len(sjs.run_time) = 6 then LEFT(sjs.run_time, 2)*3600 +substring(cast (sjs.run_time as varchar), 3,2) * 60 + right(sjs.run_time,2)
		else 0 
		end
		,convert(datetime,cast(nullif(sjs.run_date,0) as nvarchar(10))))
from msdb.dbo.sysjobhistory sjs where [message] like '%The job failed%'
GO

/*Create store procedure to check any job failure. If there exist one or many invoke Job_Failure_Report store procedure for every job failure*/
USE AFS_UTILITIES


/*Drop if exists*/
IF EXISTS ( SELECT * 
            FROM   sysobjects 
            WHERE  id = object_id(N'[dbo].[check_job_failure]') 
                   and OBJECTPROPERTY(id, N'IsProcedure') = 1 )
BEGIN
    DROP PROCEDURE [dbo].[check_job_failure]
END
GO

CREATE PROCEDURE [dbo].[check_job_failure] @interval int --in minutes
AS
	Declare @job_fail_count int = 0;
	Declare @instance_id int;

	--DECLARE @interval int = 30;  --Mention the interval in Minutes how often it should chek for any job failure.
	if object_id('tempdb.. #job_failure_place_holder') is not null
				drop table  #job_failure_place_holder
 
			create table  #job_failure_place_holder
			(
				instance_id int,
				Log_Date datetime
			);

	insert into #job_failure_place_holder (instance_id,Log_Date )
	select instance_id, Log_Date from [AFS_UTILITIES].[dbo].[View_Job_Failure_Human_Redable] where  instance_id not in (SELECT instance_id FROM [AFS_UTILITIES].[dbo].[Archived_failed_Job] ) and log_date >= dateadd(mi,-@interval,getdate());

	set @job_fail_count = (select count(*) from #job_failure_place_holder);
	
	while (@job_fail_count >0)

		Begin
			select top 1 instance_id as [current_id] from #job_failure_place_holder;
			set @instance_id = (select top 1 instance_id from #job_failure_place_holder);
			execute [AFS_UTILITIES].[dbo].[Archive_Failed_Job] @instance_id = @instance_id;
			delete from #job_failure_place_holder where instance_id = @instance_id;
			set @job_fail_count = (select count(*) from #job_failure_place_holder);
		end

GO


	/*Create the job that will invoke the store procedure*/
	
	--Drop if EXISTS
IF EXISTS ( SELECT * FROM msdb.dbo.sysjobs WHERE name = N'Report: failed job' )
	BEGIN
		EXEC msdb.dbo.sp_delete_job @job_name=N'Report: failed job';
	END
GO
		
USE [msdb]
GO
DECLARE @jobId BINARY(16)
EXEC  msdb.dbo.sp_add_job @job_name=N'Report: failed job', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'AFS.DBA.DIST', @job_id = @jobId OUTPUT
select @jobId as JobID;
GO
DECLARE @server_name_basic varchar(255)= (select cast(serverproperty('servername') as varchar(255)));
EXEC msdb.dbo.sp_add_jobserver @job_name=N'Report: failed job', @server_name = @server_name_basic
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'Report: failed job', @step_name=N'Check new failed job', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_fail_action=2, 
		@retry_attempts=1, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [AFS_UTILITIES].[dbo].[check_job_failure] @interval = 1440;', 
		@database_name=N'AFS_UTILITIES', 
		@flags=16
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'Report: failed job', @step_name=N'Send out the report', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=1, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [AFS_UTILITIES].[dbo].[Generate_Job_Error_Report] @Email_address = ''AFS.DBA.OPS.DIST-AccentureFederal-com@Afs365.onMicrosoft.com''', 
		@database_name=N'AFS_UTILITIES', 
		@flags=16
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'Report: failed job', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_netsend=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=N'', 
		@category_name=N'Data Collector', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'AFS.DBA.DIST', 
		@notify_netsend_operator_name=N'', 
		@notify_page_operator_name=N''
GO
USE [msdb]
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'Report: failed job', @name=N'Interval: 15 Mins', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=15, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20180119, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id as ScheduleID;
GO
