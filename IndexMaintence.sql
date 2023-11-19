
--Created by Shlomi Kiko
--Topic: This procedure helps maintain indexes in a relatively easy way
--Linkedin: https://www.linkedin.com/in/shlomikiko/

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [USP_IndexMaintenance]
AS
BEGIN
SET NOCOUNT ON

	SELECT * INTO #TmpIndexTableRebuild
	FROM
	(
		SELECT ROW_NUMBER() OVER(ORDER BY DatabaseID, SchemaID, ObjID) AS RowNum, *
		FROM
		(
			SELECT DISTINCT database_id AS DatabaseID, 'WideWorldImporters' AS DatabaseName, sch.[schema_id] AS SchemaID, sch.name AS SchemaName, 
			obj.create_date AS ObjDateCreated, obj.modify_date AS ObjDateModified, Obj.object_id AS [ObjID], obj.name AS ObjName, obj.type AS ObjType, obj.type_desc AS ObjTypeDescription, 
			ix.index_id AS IndexID, ix.name AS IndexName, index_type_desc, index_depth, index_level, avg_fragmentation_in_percent, avg_fragment_size_in_pages, avg_page_space_used_in_percent, page_count
			FROM sys.dm_db_index_physical_stats (DB_ID('WideWorldImporters'), NULL, NULL, NULL, NULL) AS  physStat
			INNER JOIN sys.indexes AS ix
			ON(physStat.index_id = ix.index_id)
			INNER JOIN sys.objects AS obj
			ON(ix.object_id = obj.object_id)
			INNER JOIN sys.schemas AS sch
			ON(obj.schema_id = sch.schema_id)
			WHERE 1 = 1
			AND Obj.type = 'U'
			AND ix.name IS NOT NULL
			AND avg_fragmentation_in_percent > 30
		)AS c
	)AS TmpRebuild

	SELECT * INTO #TmpIndexTableReorg
	FROM
	(
		SELECT ROW_NUMBER() OVER(ORDER BY DatabaseID, SchemaID, ObjID) AS RowNum, *
		FROM
		(
			SELECT DISTINCT database_id AS DatabaseID, 'WideWorldImporters' AS DatabaseName, sch.[schema_id] AS SchemaID, sch.name AS SchemaName, 
			obj.create_date AS ObjDateCreated, obj.modify_date AS ObjDateModified, Obj.object_id AS [ObjID], obj.name AS ObjName, obj.type AS ObjType, obj.type_desc AS ObjTypeDescription, 
			ix.index_id AS IndexID, ix.name AS IndexName, index_type_desc, index_depth, index_level, avg_fragmentation_in_percent, avg_fragment_size_in_pages, avg_page_space_used_in_percent, page_count
			FROM sys.dm_db_index_physical_stats (DB_ID('WideWorldImporters'), NULL, NULL, NULL, NULL) AS  physStat
			INNER JOIN sys.indexes AS ix
			ON(physStat.index_id = ix.index_id)
			INNER JOIN sys.objects AS obj
			ON(ix.object_id = obj.object_id)
			INNER JOIN sys.schemas AS sch
			ON(obj.schema_id = sch.schema_id)
			WHERE 1 = 1
			AND Obj.type = 'U'
			AND ix.name IS NOT NULL
			AND avg_fragmentation_in_percent BETWEEN 5 AND 30
		)AS b
	)AS TmpReorg

	--Starting variable for loop
	DECLARE @Counter int = 1

	--TotalCount
	DECLARE @TotalCountReorg int = (SELECT MAX(RowNum) FROM #TmpIndexTableReorg)
	DECLARE @TotalCountRebuild int = (SELECT MAX(RowNum) FROM #TmpIndexTableRebuild)

	--Reorg variables
	DECLARE @IndexToReorgAvgFrag float = (SELECT avg_fragmentation_in_percent FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
	DECLARE @IndexToReorgName varchar(MAX) = (SELECT IndexName FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
	DECLARE @IndexToReorgSchema varchar(MAX) = (SELECT SchemaName FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
	DECLARE @IndexToReorgObject varchar(MAX) = (SELECT ObjName FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
	DECLARE @ReorgCmd nvarchar(MAX)

	--Rebuild variables
	DECLARE @IndexToRebuildAvgFrag float = (SELECT avg_fragmentation_in_percent FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
	DECLARE @IndexToRebuildName varchar(MAX) = (SELECT IndexName FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
	DECLARE @IndexToRebuildSchema varchar(MAX) = (SELECT SchemaName FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
	DECLARE @IndexToRebuildObject varchar(MAX) = (SELECT ObjName FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
	DECLARE @RebuildCmd nvarchar(MAX)

	--Reorg for working days to reduce pressure:
	IF(DATENAME(WEEKDAY, GETDATE()) = 'SUNDAY')
	BEGIN
		EXEC sp_updatestats

		WHILE(@TotalCountRebuild >= @Counter )
		BEGIN
			SELECT @IndexToRebuildName AS RebuildName, @IndexToRebuildSchema AS RebuildSchema, @IndexToRebuildObject AS RebuildObject

			SET @RebuildCmd = 
			'ALTER INDEX ' + @IndexToRebuildName + 
			' ON ' + @IndexToRebuildSchema + '.' + @IndexToRebuildObject +
			' REBUILD'
			
			EXEC sp_executesql @RebuildCmd
		
			SET @Counter += 1

			SET @IndexToRebuildAvgFrag = (SELECT avg_fragmentation_in_percent FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
			SET @IndexToRebuildName = (SELECT IndexName FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
			SET @IndexToRebuildSchema = (SELECT SchemaName FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
			SET @IndexToRebuildObject = (SELECT ObjName FROM #TmpIndexTableRebuild WHERE RowNum = @Counter)
		END	
	END

	ELSE
	BEGIN
		WHILE(@TotalCountReorg >= @Counter )
		BEGIN

			SELECT @IndexToReorgName AS ReorgName, @IndexToReorgSchema AS ReorgSchema, @IndexToReorgObject AS ReorgObject

			SET @ReorgCmd = 
			' ALTER INDEX ' + @IndexToReorgName + 
			' ON ' + @IndexToReorgSchema + '.' + @IndexToReorgObject +
			' REBUILD'

			EXEC sp_executesql @ReorgCmd

			SET @Counter += 1

			SET @IndexToReorgAvgFrag = (SELECT avg_fragmentation_in_percent FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
			SET @IndexToReorgName = (SELECT IndexName FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
			SET @IndexToReorgSchema = (SELECT SchemaName FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
			SET @IndexToReorgObject = (SELECT ObjName FROM #TmpIndexTableReorg WHERE RowNum = @Counter)
		END
	END

	SELECT 'Indexes improved for Rebuild:'
	SELECT SchemaName, ObjName, IndexName FROM #TmpIndexTableRebuild

	SELECT 'Indexes improved for Reorg:'
	SELECT SchemaName, ObjName, IndexName FROM #TmpIndexTableReorg

	--Drop tables in the end
	DROP TABLE #TmpIndexTableRebuild
	
	DROP TABLE #TmpIndexTableReorg
END
GO


