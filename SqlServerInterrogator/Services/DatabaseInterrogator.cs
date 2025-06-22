namespace SqlServerInterrogator.Services;

using Microsoft.Data.SqlClient;
using SqlServerInterrogator.Models;
using SqlServerInterrogator.SqlScripts;
using System.Runtime.CompilerServices;

/// <summary>
/// Provides functionality to retrieve detailed schema information from SQL Server databases.
/// This includes information about tables, columns, keys, and indexes.
/// </summary>
public class DatabaseInterrogator
{
    /// <summary>
    /// Retrieves detailed information about all tables in the specified database.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database to interrogate.</param>
    /// <param name="populateColumnKeysAndIndexes">When true, populates detailed column, key, and index information for each table. 
    /// When false, only basic table information is retrieved, improving performance for large databases.</param>
    /// <param name="cancellationToken">Optional token to cancel the operation.</param>
    /// <returns>A list of <see cref="TableInfo"/> objects containing detailed table information.</returns>
    /// <remarks>
    /// This method retrieves comprehensive information about each table. When <paramref name="populateColumnKeysAndIndexes"/> is true,
    /// it includes columns, keys, and indexes by performing additional database queries. Setting this parameter to false will
    /// significantly improve performance when detailed schema information is not needed.
    /// </remarks>
    public static async Task<List<TableInfo>> GetTableInfoAsync(
        string connectionString,
        string databaseName,
        bool populateColumnKeysAndIndexes = true,
        CancellationToken cancellationToken = default)
    {
        var tables = new List<TableInfo>();
        await foreach (var table in GetTableInfoEnumerableAsync(
            connectionString,
            databaseName,
            populateColumnKeysAndIndexes,
            cancellationToken)
            .WithCancellation(cancellationToken))
        {
            tables.Add(table);
        }

        return tables;
    }

    /// <summary>
    /// Provides an asynchronous enumerable of table information from the specified database.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database to interrogate.</param>
    /// <param name="populateColumnKeysAndIndexes">When true, populates detailed column, key, and index information for each table. 
    /// When false, only basic table information is retrieved, improving performance for large databases.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An asynchronous enumerable of <see cref="TableInfo"/> objects.</returns>
    /// <remarks>
    /// This method retrieves comprehensive information about each table. When <paramref name="populateColumnKeysAndIndexes"/> is true,
    /// it includes columns, keys, and indexes by performing additional database queries. Setting this parameter to false will
    /// significantly improve performance when detailed schema information is not needed.
    /// </remarks>
    public static async IAsyncEnumerable<TableInfo> GetTableInfoEnumerableAsync(
        string connectionString,
        string databaseName,
        bool populateColumnKeysAndIndexes = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        var sql = $"USE [{databaseName}]; {DatabaseInterrogatorSqlScripts.GetDatabaseInfoAsyncSql}";
        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken) && !cancellationToken.IsCancellationRequested)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            var table = new TableInfo()
            {
                TableId = reader.GetInt32(reader.GetOrdinal("TableId")),
                Name = reader.GetString(reader.GetOrdinal("Name")),
                SchemaName = reader.GetString(reader.GetOrdinal("SchemaName")),
                Type = reader.GetString(reader.GetOrdinal("Type")),
                TypeDesc = reader.GetString(reader.GetOrdinal("TypeDesc")),
                IsUserTable = reader.GetInt32(reader.GetOrdinal("IsUserTable")) == 1,
                IsSystemTable = reader.GetInt32(reader.GetOrdinal("IsSystemTable")) == 1,
                IsMemoryOptimized = reader.GetBoolean(reader.GetOrdinal("IsMemoryOptimized")),
                IsFileTable = reader.GetBoolean(reader.GetOrdinal("IsFileTable")),
                CreateDate = reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                ModifyDate = reader.GetDateTime(reader.GetOrdinal("ModifyDate")),
                RowCount = reader.GetInt64(reader.GetOrdinal("RowCount"))
            };

            if (!populateColumnKeysAndIndexes)
            {
                yield return table;
                continue;
            }

            table.Columns = await GetColumnInfoAsync(
                connectionString,
                databaseName,
                table.TableId,
                cancellationToken);
            table.Keys = await GetKeyInfoAsync(
                connectionString,
                databaseName,
                table.TableId,
                cancellationToken);
            table.Indexes = await GetIndexInfoAsync(
                connectionString,
                databaseName,
                table.TableId,
                cancellationToken);

            yield return table;
        }
    }

    /// <summary>
    /// Retrieves column information for a specific table.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database containing the table.</param>
    /// <param name="tableId">The ID of the table to query.</param>
    /// <param name="cancellationToken">Optional token to cancel the operation.</param>
    /// <returns>A list of <see cref="ColumnInfo"/> objects describing the table's columns.</returns>
    public static async Task<List<ColumnInfo>> GetColumnInfoAsync(
        string connectionString,
        string databaseName,
        int tableId,
        CancellationToken cancellationToken = default)
    {
        var columns = new List<ColumnInfo>();
        await foreach (var column in GetColumnInfoEnumerableAsync(
            connectionString,
            databaseName,
            tableId,
            cancellationToken)
            .WithCancellation(cancellationToken))
        {
            columns.Add(column);
        }

        return columns;
    }

    /// <summary>
    /// Provides an asynchronous enumerable of column information for a specific table.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database containing the table.</param>
    /// <param name="tableId">The ID of the table to query.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An asynchronous enumerable of <see cref="ColumnInfo"/> objects.</returns>
    public static async IAsyncEnumerable<ColumnInfo> GetColumnInfoEnumerableAsync(
        string connectionString,
        string databaseName,
        int tableId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"USE [{databaseName}]; {DatabaseInterrogatorSqlScripts.GetColumnInfoEnumerableAsyncSql}";

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@TableId", tableId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken) && !cancellationToken.IsCancellationRequested)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return new ColumnInfo
            {
                ColumnId = reader.GetInt32(reader.GetOrdinal("ColumnId")),
                Name = reader.GetString(reader.GetOrdinal("Name")),
                TableId = reader.GetInt32(reader.GetOrdinal("TableId")),
                DataType = reader.GetString(reader.GetOrdinal("DataType")),
                IsNullable = reader.GetBoolean(reader.GetOrdinal("IsNullable")),
                MaxLength = reader.GetInt16(reader.GetOrdinal("MaxLength")),
                Precision = reader.GetByte(reader.GetOrdinal("Precision")),
                Scale = reader.GetByte(reader.GetOrdinal("Scale")),
                IsIdentity = reader.GetBoolean(reader.GetOrdinal("IsIdentity")),
                IsComputed = reader.GetBoolean(reader.GetOrdinal("IsComputed")),
                OrdinalPosition = reader.GetInt32(reader.GetOrdinal("OrdinalPosition")),
                DefaultValue = reader.IsDBNull(reader.GetOrdinal("DefaultValue"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("DefaultValue")),
                DatabaseName = databaseName
            };
        }
    }

    /// <summary>
    /// Retrieves key information (primary, foreign, unique) for a specific table.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database containing the table.</param>
    /// <param name="tableId">The ID of the table to query.</param>
    /// <param name="cancellationToken">Optional token to cancel the operation.</param>
    /// <returns>A list of <see cref="KeyInfo"/> objects describing the table's keys.</returns>
    public static async Task<List<KeyInfo>> GetKeyInfoAsync(
        string connectionString,
        string databaseName,
        int tableId,
        CancellationToken cancellationToken = default)
    {
        var keys = new List<KeyInfo>();
        await foreach (var key in GetKeyInfoEnumerableAsync(
            connectionString,
            databaseName,
            tableId,
            cancellationToken)
            .WithCancellation(cancellationToken))
        {
            keys.Add(key);
        }

        return keys;
    }

    /// <summary>
    /// Provides an asynchronous enumerable of key information for a specific table.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database containing the table.</param>
    /// <param name="tableId">The ID of the table to query.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An asynchronous enumerable of <see cref="KeyInfo"/> objects.</returns>
    public static async IAsyncEnumerable<KeyInfo> GetKeyInfoEnumerableAsync(
        string connectionString,
        string databaseName,
        int tableId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"USE [{databaseName}]; {DatabaseInterrogatorSqlScripts.GetKeyInfoEnumerableAsyncSql}";

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@TableId", tableId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken) && !cancellationToken.IsCancellationRequested)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return new KeyInfo
            {
                KeyId = reader.GetInt32(reader.GetOrdinal("KeyId")),
                Name = reader.GetString(reader.GetOrdinal("Name")),
                TableId = reader.GetInt32(reader.GetOrdinal("TableId")),
                Type = reader.GetString(reader.GetOrdinal("Type")),
                TypeDesc = reader.GetString(reader.GetOrdinal("TypeDesc")),
                IsPrimaryKey = reader.GetBoolean(reader.GetOrdinal("IsPrimaryKey")),
                IsUnique = reader.GetBoolean(reader.GetOrdinal("IsUnique")),
                IsForeignKey = reader.GetBoolean(reader.GetOrdinal("IsForeignKey")),
                IsDisabled = reader.GetBoolean(reader.GetOrdinal("IsDisabled")),
                IsSystemNamed = reader.GetBoolean(reader.GetOrdinal("IsSystemNamed")),
                CreateDate = reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                ModifyDate = reader.GetDateTime(reader.GetOrdinal("ModifyDate")),
                ReferencedTableSchema = reader.IsDBNull(reader.GetOrdinal("ReferencedTableSchema"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("ReferencedTableSchema")),
                ReferencedTableName = reader.IsDBNull(reader.GetOrdinal("ReferencedTableName"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("ReferencedTableName")),
                ReferencedColumnName = reader.IsDBNull(reader.GetOrdinal("ReferencedColumnName"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("ReferencedColumnName")),
                SourceColumnName = reader.IsDBNull(reader.GetOrdinal("SourceColumnName"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("SourceColumnName")),
                DeleteReferentialAction = reader.IsDBNull(reader.GetOrdinal("DeleteReferentialAction"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("DeleteReferentialAction")),
                UpdateReferentialAction = reader.IsDBNull(reader.GetOrdinal("UpdateReferentialAction"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("UpdateReferentialAction"))
            };
        }
    }

    /// <summary>
    /// Retrieves index information for a specific table.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database containing the table.</param>
    /// <param name="tableId">The ID of the table to query.</param>
    /// <param name="cancellationToken">Optional token to cancel the operation.</param>
    /// <returns>A list of <see cref="IndexInfo"/> objects describing the table's indexes.</returns>
    public static async Task<List<IndexInfo>> GetIndexInfoAsync(
        string connectionString,
        string databaseName,
        int tableId,
        CancellationToken cancellationToken = default)
    {
        var indexes = new List<IndexInfo>();
        await foreach (var index in GetIndexInfoEnumerableAsync(
            connectionString,
            databaseName,
            tableId,
            cancellationToken)
            .WithCancellation(cancellationToken))
        {
            indexes.Add(index);
        }

        return indexes;
    }

    /// <summary>
    /// Provides an asynchronous enumerable of index information for a specific table.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database containing the table.</param>
    /// <param name="tableId">The ID of the table to query.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An asynchronous enumerable of <see cref="IndexInfo"/> objects.</returns>
    public static async IAsyncEnumerable<IndexInfo> GetIndexInfoEnumerableAsync(
        string connectionString,
        string databaseName,
        int tableId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"USE [{databaseName}]; {DatabaseInterrogatorSqlScripts.GetIndexInfoEnumerableAsyncSql}";

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@TableId", tableId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken) && !cancellationToken.IsCancellationRequested)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return new IndexInfo
            {
                IndexId = reader.GetInt32(reader.GetOrdinal("IndexId")),
                Name = reader.GetString(reader.GetOrdinal("Name")),
                Type = reader.GetByte(reader.GetOrdinal("Type")).ToString(),
                TypeDesc = reader.GetString(reader.GetOrdinal("TypeDesc")),
                IsPrimaryKey = reader.GetBoolean(reader.GetOrdinal("IsPrimaryKey")),
                IsUnique = reader.GetBoolean(reader.GetOrdinal("IsUnique")),
                IsDisabled = reader.GetBoolean(reader.GetOrdinal("IsDisabled")),
                IsHypothetical = reader.GetBoolean(reader.GetOrdinal("IsHypothetical")),
                CreateDate = reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                ModifyDate = reader.GetDateTime(reader.GetOrdinal("ModifyDate")),
                RowCount = reader.GetInt64(reader.GetOrdinal("RowCount"))
            };
        }
    }

    /// <summary>
    /// Retrieves a list of stored procedures in the specified database.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database to interrogate.</param>
    /// <param name="cancellationToken">Optional token to cancel the operation.</param>
    /// <returns>A list of <see cref="StoredProcedureInfo"/> objects containing procedure information.</returns>
    public static async Task<List<StoredProcedureInfo>> GetStoredProcedureInfoAsync(
        string connectionString,
        string databaseName,
        CancellationToken cancellationToken = default)
    {
        var procedures = new List<StoredProcedureInfo>();
        await foreach (var proc in GetStoredProcedureInfoEnumerableAsync(
            connectionString,
            databaseName,
            cancellationToken)
            .WithCancellation(cancellationToken))
        {
            procedures.Add(proc);
        }

        return procedures;
    }

    /// <summary>
    /// Provides an asynchronous enumerable of stored procedure information from the specified database.
    /// </summary>
    /// <param name="connectionString">The connection string to the SQL Server instance.</param>
    /// <param name="databaseName">The name of the database to interrogate.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>An asynchronous enumerable of <see cref="StoredProcedureInfo"/> objects.</returns>
    /// <remarks>
    /// This method retrieves comprehensive information about each stored procedure,
    /// including its definition and parameters.
    /// </remarks>
    public static async IAsyncEnumerable<StoredProcedureInfo> GetStoredProcedureInfoEnumerableAsync(
        string connectionString,
        string databaseName,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $@"USE [{databaseName}]; {DatabaseInterrogatorSqlScripts.GetStoredProcedureInfoEnumerableAsyncSql}";

        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken) && !cancellationToken.IsCancellationRequested)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            var procId = reader.GetInt32(reader.GetOrdinal("ProcedureId"));

            var procedure = new StoredProcedureInfo
            {
                ProcedureId = procId,
                Name = reader.GetString(reader.GetOrdinal("Name")),
                SchemaName = reader.GetString(reader.GetOrdinal("SchemaName")),
                Type = reader.GetString(reader.GetOrdinal("Type")),
                TypeDesc = reader.GetString(reader.GetOrdinal("TypeDesc")),
                IsSystemObject = reader.GetBoolean(reader.GetOrdinal("IsSystemObject")),
                CreateDate = reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                ModifyDate = reader.GetDateTime(reader.GetOrdinal("ModifyDate")),
                Definition = reader.IsDBNull(reader.GetOrdinal("Definition"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("Definition")),
                Parameters = await GetStoredProcedureParametersAsync(
                    connection,
                    procId,
                    cancellationToken)
            };

            yield return procedure;
        }
    }

    /// <summary>
    /// Populates table join relationships for each table in the database by analyzing direct and indirect
    /// foreign key relationships between tables.
    /// </summary>
    /// <param name="databaseInfo">The database information containing tables and their relationships.</param>
    /// <remarks>
    /// For each table, this method finds all other tables it can join to using FindJoinPath.
    /// </remarks>
    public static void PopulateDatabaseForeignAndPrimaryTables(DatabaseInfo databaseInfo)
    {
        // For each table, find all other tables it can join to using SqlGenerator.FindJoinPath
        foreach (var table in databaseInfo.Tables)
        {
            table.TablesICanJoinTo = [];
            foreach (var candidate in databaseInfo.Tables)
            {
                if (table.TableId == candidate.TableId)
                    continue;

                // Use FindJoinPath to determine if a join path exists
                var joinPath = FindJoinPath(table, candidate, databaseInfo.Tables);
                if (joinPath != null)
                {
                    table.TablesICanJoinTo.Add(candidate);
                }
            }
        }
    }

    /// <summary>
    /// Finds a path between two tables using their relationships through foreign keys.
    /// Implements a breadth-first search algorithm to find the shortest possible join path.
    /// </summary>
    /// <param name="source">The source table to start the path from.</param>
    /// <param name="target">The target table to find a path to.</param>
    /// <param name="allTables">List of all available tables in the database.</param>
    /// <returns>A JoinPath containing the sequence of tables and their join keys, or null if no path is found.</returns>
    internal static JoinPath? FindJoinPath(TableInfo source, TableInfo target, List<TableInfo> allTables)
    {
        var visited = new HashSet<int>();
        var queue = new Queue<JoinPath>();
        var initial = new JoinPath();
        initial.Tables.Add((source, null));
        queue.Enqueue(initial);
        _ = visited.Add(source.TableId);

        while (queue.Count > 0)
        {
            var currentPath = queue.Dequeue();
            var currentTable = currentPath.Tables[^1].Table;

            // Check if we found a path to the target
            if (currentTable.TableId == target.TableId)
            {
                return currentPath;
            }
            // Find all tables we can join to from here
            foreach (var nextTable in allTables)
            {
                if (visited.Contains(nextTable.TableId))
                {
                    continue;
                }

                var joinKey = FindJoinKey(currentTable, nextTable);
                if (joinKey != null)
                {
                    var newPath = new JoinPath();
                    newPath.Tables.AddRange(currentPath.Tables);
                    newPath.Tables.Add((nextTable, joinKey));
                    queue.Enqueue(newPath);
                    _ = visited.Add(nextTable.TableId);
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Finds a foreign key relationship between two tables by examining both tables' keys.
    /// </summary>
    /// <param name="source">The source table to check for relationships.</param>
    /// <param name="target">The target table to check for relationships.</param>
    /// <returns>A KeyInfo object representing the foreign key relationship, or null if no relationship exists.</returns>
    private static KeyInfo? FindJoinKey(TableInfo source, TableInfo target)
    {
        // Check foreign keys in the source table pointing to the target
        var sourceToTarget = source.Keys
            .FirstOrDefault(k => k.IsForeignKey &&
                string.Equals(k.ReferencedTableName, target.Name, StringComparison.OrdinalIgnoreCase));
        if (sourceToTarget != null)
        {
            return sourceToTarget;
        }
        // Check foreign keys in the target table pointing to the source
        return target.Keys
            .FirstOrDefault(k => k.IsForeignKey &&
                string.Equals(k.ReferencedTableName, source.Name, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Retrieves the parameters for a specific stored procedure.
    /// </summary>
    /// <param name="connection">An open SQL connection.</param>
    /// <param name="procedureId">The ID of the stored procedure.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A list of parameter names and types.</returns>
    private static async Task<List<string>> GetStoredProcedureParametersAsync(
        SqlConnection connection,
        int procedureId,
        CancellationToken cancellationToken)
    {
        var sql = DatabaseInterrogatorSqlScripts.GetStoredProcedureParametersAsyncSql;

        await using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@ProcedureId", procedureId);

        var parameters = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var paramName = reader.GetString(reader.GetOrdinal("ParameterName"));
            var dataType = reader.GetString(reader.GetOrdinal("DataType"));
            var maxLength = reader.GetInt16(reader.GetOrdinal("MaxLength"));
            var precision = reader.GetByte(reader.GetOrdinal("Precision"));
            var scale = reader.GetByte(reader.GetOrdinal("Scale"));

            var parameterDefinition = $"{paramName} {dataType}";

            if (dataType is "varchar" or "nvarchar" or "char" or "nchar")
            {
                parameterDefinition += maxLength == -1 ? "(MAX)" : $"({maxLength})";
            }
            else if (dataType is "decimal" or "numeric")
            {
                parameterDefinition += $"({precision},{scale})";
            }

            parameters.Add(parameterDefinition);
        }

        return parameters;
    }
}

/// <summary>
/// Represents a path of joined tables and their connecting relationships.
/// Used internally for tracking and building complex table joins.
/// </summary>
internal class JoinPath
{
    /// <summary>
    /// Gets the list of tables and their join keys in the path.
    /// The first table in the list is the source table and has a null JoinKey.
    /// Subsequent tables include the foreign key relationship used to join to the previous table.
    /// </summary>
    /// <remarks>
    /// The list maintains the order of joins, where each entry after the first contains:
    /// - The table to join to
    /// - The foreign key relationship used to establish the join
    /// This structure ensures proper join order and relationship tracking.
    /// </remarks>
    public List<(TableInfo Table, KeyInfo? JoinKey)> Tables { get; } = [];
}