﻿namespace SqlServerInterrogator.SqlScripts;
internal static class ServerInterrogatorSqlScripts
{
    internal const string GetDatabasesAsyncSql =
        @"
            SELECT
                d.database_id as DatabaseId,
                d.name as Name,
                SUSER_SNAME(d.owner_sid) as Owner,
                d.create_date as CreateDate,
                d.collation_name as CollationName,
                d.recovery_model_desc as RecoveryModel,
                d.compatibility_level as CompatibilityLevel,
                d.is_read_only as IsReadOnly,
                d.is_auto_close_on as IsAutoCloseOn,
                d.is_auto_shrink_on as IsAutoShrink,
                d.is_encrypted as IsEncrypted,
                CAST(ROUND((SUM(CAST(mf.size AS BIGINT)) * 8.0 / 1024), 0) as BIGINT) as SizeMB,
                CAST(ROUND((SUM(CAST(
                    CASE WHEN mf.type = 0 THEN 
                        size - FILEPROPERTY(mf.name, 'SpaceUsed') 
                    ELSE 0 END AS BIGINT)) * 8.0 / 1024), 0) as BIGINT) as SpaceAvailableMB,
                d.state_desc as State,
                (SELECT name FROM sys.filegroups WHERE is_default = 1) as DefaultFileGroup,
                ISNULL(MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END), CAST('1900-01-01' AS DATETIME)) as LastBackupDate,
                ISNULL(MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END), CAST('1900-01-01' AS DATETIME)) as LastLogBackupDate,
                d.is_in_standby as IsInStandby,
                d.is_broker_enabled as IsBrokerEnabled,
                d.user_access_desc as UserAccess
            FROM sys.databases d
            LEFT JOIN sys.master_files mf ON d.database_id = mf.database_id
            LEFT JOIN msdb.dbo.backupset bs ON d.name = bs.database_name
            WHERE d.database_id > 4  -- Excludes system databases (master=1, tempdb=2, model=3, msdb=4)
            AND d.name NOT IN ('master', 'tempdb', 'model', 'msdb')  -- Additional safety check
            GROUP BY
                d.database_id, d.name, d.owner_sid, d.create_date, d.collation_name,
                d.recovery_model_desc, d.compatibility_level, d.is_read_only,
                d.is_auto_close_on, d.is_auto_shrink_on, d.is_encrypted,
                d.state_desc, d.is_in_standby,
                d.is_broker_enabled, d.user_access_desc";

    internal const string GetServerInfoAsyncSql =
        @"SELECT 
            SERVERPROPERTY('ServerName') AS ServerName,
            SERVERPROPERTY('ProductVersion') AS ProductVersion,
            SERVERPROPERTY('Edition') AS Edition,
            SERVERPROPERTY('ProductLevel') AS ProductLevel,
            CAST(COALESCE(SERVERPROPERTY('IsClustered'), 0) AS BIT) AS IsClustered,
            sqlserver_start_time AS StartTime,
            SERVERPROPERTY('Collation') AS Collation,
            CAST(SERVERPROPERTY('ProcessID') AS INT) AS ProcessId,
            CASE SERVERPROPERTY('IsIntegratedSecurityOnly')
                WHEN 1 THEN 'Windows Authentication'
                ELSE 'SQL Server and Windows Authentication'
            END AS AuthenticationMode,
            SERVERPROPERTY('BuildClrVersion') AS BuildClrVersion,
            CAST(COALESCE(SERVERPROPERTY('IsHadrEnabled'), 0) AS BIT) AS IsHadrEnabled,
            CASE SERVERPROPERTY('HadrManagerStatus')
                WHEN 0 THEN 'Not started'
                WHEN 1 THEN 'Started'
                WHEN 2 THEN 'Not available'
                ELSE NULL
            END AS HadrManagerStatus
        FROM sys.dm_os_sys_info";
}
