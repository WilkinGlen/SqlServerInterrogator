namespace SqlServerInterrogator.Services;
using Microsoft.Data.SqlClient;
using SqlServerInterrogator.Models;
using SqlServerInterrogator.SqlScripts;
using System.Runtime.CompilerServices;

public class DatabaseInterrogator
{
    public static async Task<List<TableInfo>> GetTableInfoAsync(
        string connectionString,
        string databaseName,
        CancellationToken cancellationToken = default)
    {
        var tables = new List<TableInfo>();
        await foreach (var table in GetTableInfoEnumerableAsync(connectionString, databaseName, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            tables.Add(table);
        }

        return tables;
    }

    public static async IAsyncEnumerable<TableInfo> GetTableInfoEnumerableAsync(
        string connectionString, 
        string databaseName, 
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

            table.Columns = await GetColumnInfoAsync(connectionString, databaseName, table.TableId, cancellationToken);
            table.Keys = await GetKeyInfoAsync(connectionString, databaseName, table.TableId, cancellationToken);

            yield return table;
        }
    }

    public static async Task<List<ColumnInfo>> GetColumnInfoAsync(
        string connectionString,
        string databaseName,
        int tableId,
        CancellationToken cancellationToken = default)
    {
        var columns = new List<ColumnInfo>();
        await foreach (var column in GetColumnInfoEnumerableAsync(connectionString, databaseName, tableId, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            columns.Add(column);
        }

        return columns;
    }

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

    public static async Task<List<KeyInfo>> GetKeyInfoAsync(
        string connectionString,
        string databaseName,
        int tableId,
        CancellationToken cancellationToken = default)
    {
        var keys = new List<KeyInfo>();
        await foreach (var key in GetKeyInfoEnumerableAsync(connectionString, databaseName, tableId, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            keys.Add(key);
        }

        return keys;
    }

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
}