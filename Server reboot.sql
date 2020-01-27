USE [master]
GO

/****** Object:  StoredProcedure [dbo].[usp_SQLServerService_StartUp_Email]    Script Date: 7/18/2017 2:42:57 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF EXISTS (
		SELECT *
		FROM sysobjects
		WHERE id = object_id(N'[dbo].[usp_SQLServerService_StartUp_Email]')
			AND OBJECTPROPERTY(id, N'IsProcedure') = 1
		)
BEGIN
	DROP PROCEDURE [dbo].[usp_SQLServerService_StartUp_Email]
END
GO

CREATE PROCEDURE [dbo].[usp_SQLServerService_StartUp_Email]
AS
--Get the server name and Time:
DECLARE @ServerName VARCHAR(50),
	@Time VARCHAR(50),
	@Date VARCHAR(50),
	@subject VARCHAR(Max),
	@emailcontent VARCHAR(Max),
	@bg_color VARCHAR(50),
	@txt_color VARCHAR(50);

SET @ServerName = (
		SELECT @@SERVERNAME
		);
SET @Time = (
		SELECT FORMAT(CAST(GETDATE() AS DATETIME), 'hh:mm tt')
		);
SET @Date = (
		SELECT convert(VARCHAR(10), GETDATE(), 120)
		)
SET @subject = 'SQL Server Service has been started on ' + @ServerName;

DECLARE @serverName_Temp VARCHAR(128) = (
		SELECT cast(SERVERPROPERTY('MachineName') AS VARCHAR)
		);
DECLARE @serverNumber VARCHAR(3) = (
		SELECT SUBSTRING(@serverName_Temp, LEN(@serverName_Temp) - 2, 3)
		);

--	background-color:'+@bg_color+';
--	color: '+@txt_color+';;
/*SET UP COLOR CODES*/
--Prod (server name ends 0## or 1##): red  #C70039  
IF @serverNumber LIKE '0%'
	OR @serverNumber LIKE '1%'
BEGIN
	PRINT 'it''s a Prod Server'

	SET @bg_color = '#C70039'
	SET @txt_color = '#FFFFFF'
END
		--Dev (server name ends in 2##): green  #52BE80
ELSE IF @serverNumber LIKE '2%'
BEGIN
	PRINT 'it''s a DEV Server'

	SET @bg_color = '#1E8449'
	SET @txt_color = '#FFFFFF'
END
		--Test (server name ends in 3##: blue  #2E86C1  
ELSE IF @serverNumber LIKE '3%'
BEGIN
	PRINT 'it''s a TEST Server'

	SET @bg_color = '#2E86C1'
	SET @txt_color = '#FFFFFF'
END
		--Stage (server name ends in 4##): yellow #F4D03F  -Black font
ELSE IF @serverNumber LIKE '4%'
BEGIN
	PRINT 'it''s a STAGE Server'

	SET @bg_color = '#F4D03F'
	SET @txt_color = '#273746'
END
		--Jump Host (server name ends in 7##): black  #273746 - White
ELSE IF @serverNumber LIKE '7%'
BEGIN
	PRINT 'it''s a JUMP HOST Server'

	SET @bg_color = '#273746'
	SET @txt_color = '#FFFFFF'
END
		--DR (server name ends 9##): orange #D35400  -White
ELSE IF @serverNumber LIKE '9%'
BEGIN
	PRINT 'it''s a JUMP DR Server'

	SET @bg_color = '#D35400'
	SET @txt_color = '#FFFFFF'
END
		--Lab (anything in USTEST): purple - #8E44AD
ELSE
BEGIN
	PRINT 'it''s a USTEST/LAB Server'

	SET @bg_color = '#8E44AD'
	SET @txt_color = '#FFFFFF'
END

SET @emailcontent = '<!DOCTYPE html>
<html>
<head>
<style>
h3 {
    padding-top: 5px;
    padding-right: 5px;
    padding-bottom: 5px;
    padding-left: 5px;
    color:  ' + @bg_color + ';}

h4 {
    border-radius: 10px;
	padding-top: 5px;
    padding-right: 5px;
    padding-bottom: 5px;
    padding-left: 5px;
	background-color:' + @bg_color + ';
	color: ' + @txt_color + ';}

em{
    padding-top: 5px;
    padding-right: 5px;
    padding-bottom: 5px;
    padding-left: 5px;}
</style>
</head>
<body>

<h3>Automated Notification</h3>
<h4>
Info: SQL Server Service has been started on ' + @ServerName + '</h4>
<br>
<em>This is an automated email notification email to document that the ' + @ServerName + ' has been restarted at ' + @Time + ' on ' + @Date + '. For further information please investigate the windows event log or SQL server log.</em>
</body>
</html>

'

EXEC msdb.dbo.sp_send_dbmail @profile_name = 'Administrator_SQL', --Need to Check Profile
	@recipients = 'AFS.DBA.OPS.DIST-AccentureFederal-com@Afs365.onMicrosoft.com',
	@importance = 'High',
	@body_format = 'html',
	@body = @emailcontent,
	@subject = @subject,
	@query_result_header = 0
GO

USE master
GO

sp_procoption 'usp_SQLServerService_StartUp_Email',
	'startup',
	'on'
GO


