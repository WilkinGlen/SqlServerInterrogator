namespace SqlServerInterrogator.Services;

using SqlServerInterrogator.Models;
using System.Text.RegularExpressions;

/// <summary>
/// Provides methods for generating SQL SELECT statements based on database schema and relationships.
/// This class specializes in creating SELECT statements with automatic join path discovery using a graph-based approach.
/// </summary>
/// <remarks>
/// - Supports direct and indirect (multi-hop) joins via foreign keys (shortest path).
/// - Table aliases are used in the FROM and JOIN clauses; column aliasing is not applied.
/// - Only the [dbo] schema is supported for table references.
/// - Throws <see cref="InvalidOperationException"/> if no join path exists between required tables.
/// - Handles bi-directional foreign key relationships, but does not support self-joins or multiple FKs between the same tables with disambiguation.
/// - Disconnected tables (no join path) will cause an exception if columns from both are requested.
/// </remarks>
public class SqlGenerator
{
    /// <summary>
    /// Generates a SQL SELECT statement based on the specified columns and database information.
    /// Automatically determines and includes necessary table joins based on foreign key relationships.
    /// </summary>
    /// <param name="columns">Collection of columns to include in the SELECT statement.</param>
    /// <param name="databaseInfo">Database metadata containing table and relationship information.</param>
    /// <returns>A complete SQL SELECT statement string with proper joins and aliasing, or empty string if no valid columns are provided.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no valid join path can be found between tables containing the selected columns.</exception>
    public static string GenerateSelectStatement(IEnumerable<ColumnInfo> columns, DatabaseInfo databaseInfo, IEnumerable<(string TableName, string ColumnName, object? Value)>? parameters = null)
    {
        if (columns == null || !columns.Any() || databaseInfo == null)
        {
            throw new ArgumentException("Columns and database information must be provided.");
        }

        // Filter out null columns before processing
        var validColumns = columns.Where(c => c != null).ToList();

        if (validColumns.Count == 0)
        {
            return string.Empty;
        }

        var firstColumn = validColumns.First();

        // Group columns by their table IDs
        var columnsByTable = validColumns.GroupBy(c => c.TableId);

        // Get the main table (from first column)
        var mainTable = databaseInfo.Tables.First(t => t.TableId == firstColumn.TableId);
        var usedTables = new HashSet<int> { mainTable.TableId };

        // Build column selections with table aliases and dot-separated alias
        var columnSelections = validColumns
            .Select(c =>
            {
                var table = databaseInfo.Tables.First(t => t.TableId == c.TableId);
                return $"[{table.Name}].[{c.Name}] AS [{table.Name}.{c.Name}]";
            });

        // Start building the SQL
        var sql = new System.Text.StringBuilder();
        _ = sql.AppendLine($"USE [{databaseInfo.Name}];");
        _ = sql.AppendLine($"SELECT {string.Join(", ", columnSelections)}");
        _ = sql.AppendLine($"FROM [{databaseInfo.Name}].[dbo].[{mainTable.Name}] AS [{mainTable.Name}]");

        // Build necessary joins
        foreach (var tableGroup in columnsByTable)
        {
            if (tableGroup.Key == mainTable.TableId)
            {
                continue;
            }

            var targetTable = databaseInfo.Tables.First(t => t.TableId == tableGroup.Key);
            // Use the new location for FindJoinPath (DatabaseInterrogator)
            var joinPath =
                DatabaseInterrogator.FindJoinPath(mainTable, targetTable, databaseInfo.Tables) ??
                throw new InvalidOperationException($"No join path found between tables {mainTable.Name} and {targetTable.Name}");

            // Skip the first table as it's our main table
            for (var i = 1; i < joinPath.Tables.Count; i++)
            {
                var (table, joinKey) = joinPath.Tables[i];

                if (!usedTables.Contains(table.TableId))
                {
                    _ = sql.AppendLine($"LEFT JOIN [{databaseInfo.Name}].[dbo].[{table.Name}] AS [{table.Name}]");
                    // Generate the join condition based on the found key
                    var joinCondition = GenerateJoinCondition(joinKey!, joinPath.Tables[i - 1].Table, table);
                    _ = sql.AppendLine($"    ON {joinCondition}");
                    _ = usedTables.Add(table.TableId);
                }
            }
        }

        // Add WHERE clause for parameters if provided
        if (parameters != null && parameters.Any())
        {
            var whereClause = GenerateWhereClause(parameters);
            if (!string.IsNullOrEmpty(whereClause))
            {
                _ = sql.AppendLine(whereClause);
            }
        }

        return sql.ToString();
    }

    public static Dictionary<string, object?> GenerateParameters(string sql, Dictionary<string, string> parameters)
    {
        if (string.IsNullOrWhiteSpace(sql) || parameters == null || !parameters.Any())
        {
            return [];
        }

        var whereClauseCount = CountWhereConditions(sql);
        if (whereClauseCount != parameters.Count)
        {
            throw new ArgumentException($"Number of parameters ({parameters.Count}) does not match number of WHERE conditions ({whereClauseCount})");
        }

        Dictionary<string, string> columnNameMap = new(StringComparer.OrdinalIgnoreCase);
        var whereIndex = sql.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereIndex >= 0)
        {
            var whereClause = sql[(whereIndex + 6)..];
            var fieldNames = whereClause.Split([" AND "], StringSplitOptions.RemoveEmptyEntries)
                .Select(condition =>
                {
                    var parts = condition.Trim().Split('=', StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 0)
                    {
                        var match = Regex.Match(parts[0].Trim(), @"\[([^\]]+)\]\.?\[?([^\]]+)\]?");
                        var columnName = match.Success ? match.Groups[2].Value : parts[0].Trim();
                        if (!string.IsNullOrEmpty(columnName))
                        {
                            columnNameMap[columnName] = columnName;
                        }

                        return columnName;
                    }

                    return string.Empty;
                })
                .Where(name => !string.IsNullOrEmpty(name))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var parameterKeys = parameters.Keys
                .Select(k => k.TrimStart('@'))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            if (!fieldNames.SetEquals(parameterKeys))
            {
                var missingFields = parameterKeys.Except(fieldNames, StringComparer.OrdinalIgnoreCase);
                var extraFields = fieldNames.Except(parameterKeys, StringComparer.OrdinalIgnoreCase);
                throw new ArgumentException(
                    $"Parameter names do not match WHERE clause fields. " +
                    (missingFields.Any() ? $"Missing fields: {string.Join(", ", missingFields)}. " : "") +
                    (extraFields.Any() ? $"Extra fields: {string.Join(", ", extraFields)}" : ""));
            }
        }

        var result = new Dictionary<string, object?>();
        foreach (var param in parameters)
        {
            var paramName = param.Key;
            var resultKey = "@" + (columnNameMap.TryGetValue(paramName, out var exactName) ? exactName : paramName);

            result[resultKey] = int.TryParse(param.Value, out var intValue)
                ? intValue
                : decimal.TryParse(param.Value, out var decimalValue)
                    ? decimalValue
                    : float.TryParse(param.Value, out var floatValue)
                        ? floatValue
                        : double.TryParse(param.Value, out var doubleValue)
                            ? doubleValue
                            : DateTime.TryParse(param.Value, out var dateValue)
                                ? dateValue
                                : bool.TryParse(param.Value, out var boolValue)
                                    ? boolValue
                                    : Guid.TryParse(param.Value, out var guidValue)
                                        ? guidValue
                                        : param.Value.StartsWith('\'') && param.Value.EndsWith('\'') // Fix for CA1866
                                            ? param.Value[1..^1].Replace("''", "'")
                                            : param.Value.Equals("NULL", StringComparison.OrdinalIgnoreCase) ? null : (object)param.Value;
        }

        return result;
    }

    private static int CountWhereConditions(string sql)
    {
        var whereIndex = sql.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereIndex == -1)
        {
            return 0;
        }

        var whereClause = sql[(whereIndex + 6)..];

        return whereClause
            .Split([" AND "], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Length;
    }

    /// <summary>
    /// Generates a SQL JOIN condition based on a foreign key relationship between two tables.
    /// Handles both forward and reverse foreign key relationships correctly.
    /// </summary>
    /// <param name="key">The foreign key information defining the relationship.</param>
    /// <param name="sourceTable">The source table in the join.</param>
    /// <param name="targetTable">The target table in the join.</param>
    /// <returns>A string containing the SQL JOIN condition using the appropriate column names and table aliases.</returns>
    /// <remarks>
    /// The method:
    /// 1. Determines the direction of the foreign key relationship
    /// 2. Creates the appropriate join condition with correct column references
    /// 3. Uses table aliases to prevent naming conflicts
    /// 4. Maintains proper SQL syntax with bracketed identifiers
    /// </remarks>
    private static string GenerateJoinCondition(KeyInfo key, TableInfo sourceTable, TableInfo targetTable)
    {
        // If the foreign key is in the source table
        if (string.Equals(key.ReferencedTableName, targetTable.Name, StringComparison.OrdinalIgnoreCase))
        {
            return $"[{sourceTable.Name}].[{key.SourceColumnName}] = " +
                   $"[{targetTable.Name}].[{key.ReferencedColumnName}]";
        }

        // If the foreign key is in the target table
        return $"[{sourceTable.Name}].[{key.ReferencedColumnName}] = " +
               $"[{targetTable.Name}].[{key.SourceColumnName}]";
    }

    /// <summary>
    /// Generates a SQL WHERE clause based on the provided parameters.
    /// </summary>
    /// <param name="parameters">Collection of parameters containing table name, column name, and value.</param>
    /// <returns>A string containing the SQL WHERE clause, or an empty string if no parameters are provided.</returns>
    private static string GenerateWhereClause(IEnumerable<(string TableName, string ColumnName, object? Value)>? parameters)
    {
        if (parameters == null || !parameters.Any())
        {
            return string.Empty;
        }

        var whereConditions = parameters
            .Select(p =>
            {
                var valueLiteral = p.Value switch
                {
                    string strValue => $"'{strValue.Replace("'", "''")}'",
                    bool boolValue => boolValue ? "1" : "0",
                    DateTime dateValue => $"'{dateValue:yyyy-MM-dd HH:mm:ss}'",
                    null => "NULL",
                    _ => p.Value is { } obj && obj.GetType().GetProperty("ToString")?.GetValue(obj) is Func<string> toString
                        ? toString()
                        : p.Value.ToString()
                };

                return p.Value is null
                    ? $"[{p.TableName}].[{p.ColumnName}] IS NULL"
                    : $"[{p.TableName}].[{p.ColumnName}] = {valueLiteral}";
            })
            .ToList();

        return whereConditions.Count != 0
            ? $"WHERE {string.Join(" AND ", whereConditions)}"
            : string.Empty;
    }
}
