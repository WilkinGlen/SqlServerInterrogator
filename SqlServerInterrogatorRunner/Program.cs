// See https://aka.ms/new-console-template for more information
using SqlServerInterrogator.Services;

Console.WriteLine("Hello, World!");

var connectionString = "Server=localhost;Integrated Security=True;TrustServerCertificate=True;";

var databases = await ServerInterrogator.GetDatabasesAsync(connectionString);

foreach (var db in databases)
{
    Console.WriteLine($"Database: {db.Name}, Owner: {db.Owner}, Size: {db.SizeMB} MB, State: {db.State}");
}

var tables = await DatabaseInterrogator.GetTableInfoAsync(connectionString, "ApiSelfService");

foreach (var table in tables)
{
    Console.WriteLine($"Table: {table.Name}, Schema: {table.SchemaName}, Type: {table.Type}, Row Count: {table.RowCount}");
}

Console.WriteLine("");