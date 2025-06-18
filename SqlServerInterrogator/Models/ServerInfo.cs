namespace SqlServerInterrogator.Models;

public sealed class ServerInfo
{
    public string? ServerName { get; set; }
    public string? ProductVersion { get; set; }
    public string? Edition { get; set; }
    public string? ProductLevel { get; set; }
    public bool IsClustered { get; set; }
    public DateTime StartTime { get; set; }
    public string? Collation { get; set; }
    public int ProcessId { get; set; }
    public string? AuthenticationMode { get; set; }
    public string? BuildClrVersion { get; set; }
    public bool IsHadrEnabled { get; set; }
    public string? HadrManagerStatus { get; set; }
    public List<DatabaseInfo> Databases { get; set; } = [];
}