namespace SqlServerInterrogator.Services;

using SqlServerInterrogator.Models;

/// <summary>
/// Provides methods for generating SQL statements based on database schema and relationships.
/// This class specializes in creating optimized SELECT statements by analyzing database metadata
/// and automatically determining the most efficient join paths between tables.
/// </summary>
/// <remarks>
/// The class uses a graph-based approach to determine optimal join paths between tables,
/// supporting both direct and indirect relationships through foreign keys. It handles:
/// - Multi-table joins with proper aliasing
/// - Bi-directional foreign key relationships
/// - Validation of table relationships
/// - Proper SQL syntax generation with schema qualification
/// </remarks>
public class SqlGenerator
{
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

    /// <summary>
    /// Generates a SQL SELECT statement based on the specified columns and database information.
    /// Automatically determines and includes necessary table joins based on foreign key relationships.
    /// </summary>
    /// <param name="columns">Collection of columns to include in the SELECT statement.</param>
    /// <param name="databaseInfo">Database metadata containing table and relationship information.</param>
    /// <returns>A complete SQL SELECT statement string with proper joins and aliasing, or empty string if no valid columns are provided.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no valid join path can be found between tables containing the selected columns.</exception>
    /// <remarks>
    /// The method performs the following steps:
    /// 1. Validates input columns and database info
    /// 2. Determines the main (first) table and creates proper table aliases
    /// 3. Analyzes required joins based on column table relationships
    /// 4. Generates optimized join paths using foreign key metadata
    /// 5. Produces a fully-qualified SQL statement with proper syntax
    /// </remarks>
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
            var joinPath =
                FindJoinPath(mainTable, targetTable, databaseInfo.Tables) ??
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
    /// Finds a path between two tables using their relationships through foreign keys.
    /// Implements a breadth-first search algorithm to find the shortest possible join path.
    /// </summary>
    /// <param name="source">The source table to start the path from.</param>
    /// <param name="target">The target table to find a path to.</param>
    /// <param name="allTables">List of all available tables in the database.</param>
    /// <returns>A JoinPath containing the sequence of tables and their join keys, or null if no path is found.</returns>
    /// <remarks>
    /// The algorithm:
    /// 1. Uses breadth-first search to guarantee the shortest path
    /// 2. Tracks visited tables to prevent cycles
    /// 3. Examines both incoming and outgoing foreign key relationships
    /// 4. Builds a complete path including intermediate tables when necessary
    /// </remarks>
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
    /// <remarks>
    /// The method performs a bi-directional search for relationships:
    /// 1. First checks foreign keys in the source table pointing to the target
    /// 2. If no relationship is found, checks foreign keys in the target table pointing to the source
    /// 3. Comparison of table names is case-insensitive for flexibility
    /// </remarks>
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
