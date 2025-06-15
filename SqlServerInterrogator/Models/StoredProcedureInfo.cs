namespace SqlServerInterrogator.Models;

public sealed class StoredProcedureInfo
{
    public int ProcedureId { get; set; }
    public string? Name { get; set; }
    public string? SchemaName { get; set; }
    public string? Type { get; set; }
    public string? TypeDesc { get; set; }
    public bool IsSystemObject { get; set; }
    public DateTime CreateDate { get; set; }
    public DateTime ModifyDate { get; set; }
    public string? Definition { get; set; }
    public List<string>? Parameters { get; set; } = [];
}
