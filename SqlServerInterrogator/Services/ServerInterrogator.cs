﻿namespace SqlServerInterrogator.Services;

using Microsoft.Data.SqlClient;
using SqlServerInterrogator.Models;
using SqlServerInterrogator.SqlScripts;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

public sealed class ServerInterrogator
{
    public static async Task<List<DatabaseInfo>> GetDatabasesAsync(
        string serverConnectionString,
        CancellationToken cancellationToken = default)
    {
        var databases = new List<DatabaseInfo>();
        await foreach (var database in GetDatabasesEnumerableAsync(serverConnectionString, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            databases.Add(database);
        }

        return databases;
    }

    public static async IAsyncEnumerable<DatabaseInfo> GetDatabasesEnumerableAsync(
        string serverConnectionString,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var connection = new SqlConnection(serverConnectionString);
        await connection.OpenAsync(cancellationToken);

        const string sql = ServerInterrogatorSqlScripts.GetDatabasesAsyncSql;

        using var command = new SqlCommand(sql, connection);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            yield return new DatabaseInfo
            {
                DatabaseId = reader.GetInt32(reader.GetOrdinal("DatabaseId")),
                Name = reader.GetString(reader.GetOrdinal("Name")),
                Owner = reader.IsDBNull(reader.GetOrdinal("Owner")) ? null :
                    reader.GetString(reader.GetOrdinal("Owner")),
                CreateDate = reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                CollationName = reader.IsDBNull(reader.GetOrdinal("CollationName")) ? null :
                    reader.GetString(reader.GetOrdinal("CollationName")),
                RecoveryModel = reader.IsDBNull(reader.GetOrdinal("RecoveryModel")) ? null :
                    reader.GetString(reader.GetOrdinal("RecoveryModel")),
                CompatibilityLevel = reader.GetByte(reader.GetOrdinal("CompatibilityLevel")),
                IsReadOnly = reader.GetBoolean(reader.GetOrdinal("IsReadOnly")),
                IsAutoCloseOn = reader.GetBoolean(reader.GetOrdinal("IsAutoCloseOn")),
                IsAutoShrink = reader.GetBoolean(reader.GetOrdinal("IsAutoShrink")),
                IsEncrypted = reader.GetBoolean(reader.GetOrdinal("IsEncrypted")),
                SizeMB = reader.GetInt64(reader.GetOrdinal("SizeMB")),
                SpaceAvailableMB = reader.GetInt64(reader.GetOrdinal("SpaceAvailableMB")),
                State = reader.IsDBNull(reader.GetOrdinal("State")) ? null :
                    reader.GetString(reader.GetOrdinal("State")),
                DefaultFileGroup = reader.IsDBNull(reader.GetOrdinal("DefaultFileGroup")) ? null :
                    reader.GetString(reader.GetOrdinal("DefaultFileGroup")),
                LastBackupDate = reader.GetDateTime(reader.GetOrdinal("LastBackupDate")),
                LastLogBackupDate = reader.GetDateTime(reader.GetOrdinal("LastLogBackupDate")),
                IsInStandby = reader.GetBoolean(reader.GetOrdinal("IsInStandby")),
                IsBrokerEnabled = reader.GetBoolean(reader.GetOrdinal("IsBrokerEnabled")),
                UserAccess = reader.IsDBNull(reader.GetOrdinal("UserAccess")) ? null :
                    reader.GetString(reader.GetOrdinal("UserAccess"))
            };
        }
    }
}
