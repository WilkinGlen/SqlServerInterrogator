namespace SqlServerInterrogator.Models;

public sealed class IndexInfo
{
    public int IndexId { get; set; }
    public string? Name { get; set; }
    public string? Type { get; set; }
    public string? TypeDesc { get; set; }
    public bool IsPrimaryKey { get; set; }
    public bool IsUnique { get; set; }
    public bool IsDisabled { get; set; }
    public bool IsHypothetical { get; set; }
    public DateTime CreateDate { get; set; }
    public DateTime ModifyDate { get; set; }
    public long RowCount { get; set; }
}
