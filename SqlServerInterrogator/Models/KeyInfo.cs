namespace SqlServerInterrogator.Models;

public sealed class KeyInfo
{
    public int KeyId { get; set; }
    public string? Name { get; set; }
    public string? Type { get; set; }
    public string? TypeDesc { get; set; }
    public bool IsPrimaryKey { get; set; }
    public bool IsForeignKey { get; set; }
    public bool IsUnique { get; set; }
    public bool IsDisabled { get; set; }
    public DateTime CreateDate { get; set; }
    public DateTime ModifyDate { get; set; }
    public int TableId { get; internal set; }
    public bool IsSystemNamed { get; internal set; }
    public string? ReferencedTableSchema { get; set; }
    public string? ReferencedTableName { get; set; }
    public string? ReferencedColumnName { get; set; }
    public string? SourceColumnName { get; set; }
    public string? DeleteReferentialAction { get; set; }
    public string? UpdateReferentialAction { get; set; }
}
