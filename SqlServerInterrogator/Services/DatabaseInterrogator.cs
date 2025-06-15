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

            if(!populateColumnKeysAndIndexes)
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
                    : reader.GetString(reader.GetOrdinal("DefaultValue"))
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
                IsPrimaryKey = reader.GetBoolean(reader.GetOrdinal("IsPrimaryKey")),
                IsUnique = reader.GetBoolean(reader.GetOrdinal("IsUnique")),
                IsForeignKey = reader.GetBoolean(reader.GetOrdinal("IsForeignKey")),
                IsSystemNamed = reader.GetBoolean(reader.GetOrdinal("IsSystemNamed")),
                CreateDate = reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                ModifyDate = reader.GetDateTime(reader.GetOrdinal("ModifyDate"))
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
}