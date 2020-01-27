USE tempdb

-- Show Size, Space Used, Unused Space, and Name of all database files
SELECT [FileSizeMB] = convert(NUMERIC(10, 2), round(a.size / 128., 2)),
	[UsedSpaceMB] = convert(NUMERIC(10, 2), round(fileproperty(a.name, 'SpaceUsed') / 128., 2)),
	[UnusedSpaceMB] = convert(NUMERIC(10, 2), round((a.size - fileproperty(a.name, 'SpaceUsed')) / 128., 2)),
	[DBFileName] = a.name
FROM sysfiles a

SELECT SUBSTRING(a.FILENAME, 1, 1) Drive,
	[FILE_SIZE_MB] = convert(DECIMAL(12, 2), round(a.size / 128.000, 2)),
	[SPACE_USED_MB] = convert(DECIMAL(12, 2), round(fileproperty(a.name, 'SpaceUsed') / 128.000, 2)),
	[FREE_SPACE_MB] = convert(DECIMAL(12, 2), round((a.size - fileproperty(a.name, 'SpaceUsed')) / 128.000, 2)),
	[FREE_SPACE_%] = convert(DECIMAL(12, 2), (convert(DECIMAL(12, 2), round((a.size - fileproperty(a.name, 'SpaceUsed')) / 128.000, 2)) / convert(DECIMAL(12, 2), round(a.size / 128.000, 2)) * 100)),
	a.NAME,
	a.FILENAME
FROM dbo.sysfiles a
ORDER BY Drive,
	[Name]
