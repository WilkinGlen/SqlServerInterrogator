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
}
