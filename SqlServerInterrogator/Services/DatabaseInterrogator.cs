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
            yield return new TableInfo
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
        }
    }
}
