namespace SqlServerInterrogator.SqlScripts;

internal static class DatabaseInterrogatorSqlScripts
{
    internal const string GetDatabaseInfoAsyncSql =
        @"
        SELECT 
            t.object_id AS TableId,
            t.name AS Name,
            s.name AS SchemaName,
            t.type AS Type,
            t.type_desc AS TypeDesc,
            CASE 
                WHEN t.type = 'U' THEN 1 
                ELSE 0 
            END AS IsUserTable,
            CASE 
                WHEN t.type = 'S' THEN 1 
                ELSE 0 
            END AS IsSystemTable,
            t.is_memory_optimized AS IsMemoryOptimized,
            t.is_filetable AS IsFileTable,
            t.create_date AS CreateDate,
            t.modify_date AS ModifyDate,
            p.rows AS [RowCount]
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.indexes i ON t.object_id = i.object_id
        INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        WHERE i.index_id <= 1
        ORDER BY s.name, t.name";
}
