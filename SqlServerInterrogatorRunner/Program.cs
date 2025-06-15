// See https://aka.ms/new-console-template for more information
using SqlServerInterrogator.Services;

Console.WriteLine("Hello, World!");

var connectionString = "Server=localhost;Integrated Security=True;TrustServerCertificate=True;MultipleActiveResultSets=True;";

var databases = await ServerInterrogator.GetDatabasesAsync(connectionString);
var database = databases.FirstOrDefault(d => d.Name == "ApiSelfService");
database!.Tables = await DatabaseInterrogator.GetTableInfoAsync(connectionString, "ApiSelfService");
database.StoredProcedures = await DatabaseInterrogator.GetStoredProcedureInfoAsync(connectionString, "ApiSelfService");

Console.WriteLine("");