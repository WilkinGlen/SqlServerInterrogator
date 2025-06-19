namespace SqlServerInterrogator.Models;

public sealed class TableInfo
{
    public int TableId { get; set; }
    public string? Name { get; set; }
    public string? SchemaName { get; set; }
    public string? Type { get; set; }
    public string? TypeDesc { get; set; }
    public bool IsUserTable { get; set; }
    public bool IsSystemTable { get; set; }
    public bool IsMemoryOptimized { get; set; }
    public bool IsFileTable { get; set; }
    public DateTime CreateDate { get; set; }
    public DateTime ModifyDate { get; set; }
    public long RowCount { get; set; }
    public List<ColumnInfo> Columns { get; set; } = [];
    public List<KeyInfo> Keys { get; set; } = [];
    public List<IndexInfo> Indexes { get; set; } = [];
    public List<TableInfo> TablesICanJoinTo { get; set; } = [];
}
