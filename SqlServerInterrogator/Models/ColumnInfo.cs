namespace SqlServerInterrogator.Models;

public sealed class ColumnInfo
{
    public int ColumnId { get; set; }
    public string? Name { get; set; }
    public string? DataType { get; set; }
    public int MaxLength { get; set; }
    public bool IsNullable { get; set; }
    public bool IsIdentity { get; set; }
    public bool IsComputed { get; set; }
    public string? CollationName { get; set; }
    public string? DefaultValue { get; set; }
    public int Precision { get; set; }
    public int Scale { get; set; }
    public string? Description { get; set; }
    public int TableId { get; internal set; }
    public int OrdinalPosition { get; internal set; }
}
