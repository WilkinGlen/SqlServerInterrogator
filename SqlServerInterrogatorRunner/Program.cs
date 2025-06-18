// See https://aka.ms/new-console-template for more information
using SqlServerInterrogator.Services;

Console.WriteLine("Hello, World!");

var connectionString = "Server=localhost;Integrated Security=True;TrustServerCertificate=True;MultipleActiveResultSets=True;";

var serverInfo = await ServerInterrogator.GetServerInfoAsync(connectionString, CancellationToken.None);

await foreach (var db in ServerInterrogator.GetDatabasesEnumerableAsync(connectionString, CancellationToken.None))
{
    serverInfo.Databases.Add(db);
}

foreach (var database in serverInfo.Databases)
{
    database.Tables = await DatabaseInterrogator.GetTableInfoAsync(connectionString, database.Name!, cancellationToken: CancellationToken.None);
    database.StoredProcedures = await DatabaseInterrogator.GetStoredProcedureInfoAsync(connectionString, database.Name!, CancellationToken.None);
}

Console.WriteLine("");