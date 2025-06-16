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

    internal const string GetColumnInfoEnumerableAsyncSql =
        @"
        SELECT 
            c.column_id AS ColumnId,
            c.name AS Name,
            t.object_id AS TableId,
            tp.name AS DataType,
            c.is_nullable AS IsNullable,
            c.max_length AS MaxLength,
            c.precision AS Precision,
            c.scale AS Scale,
            c.is_identity AS IsIdentity,
            c.is_computed AS IsComputed,
            c.column_id AS OrdinalPosition,
            dc.definition AS DefaultValue
        FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
                INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id
                    LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
        WHERE t.object_id = @TableId
        ORDER BY c.column_id";

    internal static string GetKeyInfoEnumerableAsyncSql =
        @"
        SELECT 
            kc.object_id AS KeyId,
            kc.name AS Name,
            kc.parent_object_id AS TableId,
            kc.type AS Type,
            CAST(CASE WHEN kc.type = 'PK' THEN 1 ELSE 0 END AS bit) AS IsPrimaryKey,
            CAST(CASE WHEN kc.type = 'UQ' THEN 1 ELSE 0 END AS bit) AS IsUnique,
            CAST(0 AS bit) AS IsForeignKey,
            kc.is_system_named AS IsSystemNamed,
            kc.create_date AS CreateDate,
            kc.modify_date AS ModifyDate,
            NULL AS ReferencedTableSchema,
            NULL AS ReferencedTableName,
            NULL AS ReferencedColumnName
        FROM sys.tables t
            INNER JOIN sys.key_constraints kc ON t.object_id = kc.parent_object_id
        WHERE t.object_id = @TableId
        UNION ALL
        SELECT 
            fk.object_id AS KeyId,
            fk.name AS Name,
            fk.parent_object_id AS TableId,
            'F' AS Type,
            CAST(0 AS bit) AS IsPrimaryKey,
            CAST(0 AS bit) AS IsUnique,
            CAST(1 AS bit) AS IsForeignKey,
            fk.is_system_named AS IsSystemNamed,
            fk.create_date AS CreateDate,
            fk.modify_date AS ModifyDate,
            referenced_schema.name AS ReferencedTableSchema,
            referenced_table.name AS ReferencedTableName,
            referenced_column.name AS ReferencedColumnName
        FROM sys.tables t
            INNER JOIN sys.foreign_keys fk ON t.object_id = fk.parent_object_id
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables referenced_table ON fk.referenced_object_id = referenced_table.object_id
            INNER JOIN sys.schemas referenced_schema ON referenced_table.schema_id = referenced_schema.schema_id
            INNER JOIN sys.columns referenced_column ON fkc.referenced_object_id = referenced_column.object_id 
                AND fkc.referenced_column_id = referenced_column.column_id
        WHERE t.object_id = @TableId;";

    internal static string GetIndexInfoEnumerableAsyncSql =
        @"
        SELECT 
            i.index_id AS IndexId,
            i.name AS Name,
            i.type AS Type,
            i.type_desc AS TypeDesc,
            i.is_primary_key AS IsPrimaryKey,
            i.is_unique AS IsUnique,
            i.is_disabled AS IsDisabled,
            i.is_hypothetical AS IsHypothetical,
            o.create_date AS CreateDate,
            o.modify_date AS ModifyDate,
            p.rows AS [RowCount]
        FROM sys.indexes i
            INNER JOIN sys.objects o ON i.object_id = o.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        WHERE i.object_id = @TableId
        AND i.index_id > 0  -- Skip heap (clustered index)
        ORDER BY i.index_id;";
}
