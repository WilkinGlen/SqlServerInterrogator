namespace SqlServerInterrogator.Models;

public sealed class ColumnToSelect
{
    public ColumnInfo? ColumnInfo { get; set; }
    public DatabaseInfo? DatabaseInfo { get; set; }
}
