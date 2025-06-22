namespace SqlServerInterrogator.Services;

using SqlServerInterrogator.Models;

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
    public static string GenerateSelectStatement(IEnumerable<ColumnInfo> columns, DatabaseInfo databaseInfo)
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

        // Build column selections with table aliases
        var columnSelections = validColumns
            .Select(c =>
            {
                var table = databaseInfo.Tables.First(t => t.TableId == c.TableId);
                return $"[{table.Name}].[{c.Name}]";  // Add brackets around table name
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
                SqlServerInterrogator.Services.DatabaseInterrogator.FindJoinPath(mainTable, targetTable, databaseInfo.Tables) ??
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

        return sql.ToString();
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
}
