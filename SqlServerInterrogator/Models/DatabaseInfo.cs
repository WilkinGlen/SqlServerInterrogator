namespace SqlServerInterrogator.Models;

public class DatabaseInfo
{
    public int DatabaseId { get; set; }
    public string? Name { get; set; }
    public string? Owner { get; set; }
    public DateTime CreateDate { get; set; }
    public string? CollationName { get; set; }
    public string? RecoveryModel { get; set; }
    public byte CompatibilityLevel { get; set; }
    public bool IsReadOnly { get; set; }
    public bool IsAutoCloseOn { get; set; }
    public bool IsAutoShrink { get; set; }
    public bool IsEncrypted { get; set; }
    public long SizeMB { get; set; }
    public long SpaceAvailableMB { get; set; }
    public string? State { get; set; }
    public string? DefaultFileGroup { get; set; }
    public DateTime LastBackupDate { get; set; }
    public DateTime LastLogBackupDate { get; set; }
    public bool IsInStandby { get; set; }
    public bool IsBrokerEnabled { get; set; }
    public string? UserAccess { get; set; }
    public List<TableInfo> Tables { get; set; } = [];
    public List<StoredProcedureInfo>? StoredProcedures { get; set; } = [];
}
