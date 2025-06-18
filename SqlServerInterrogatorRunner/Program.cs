// See https://aka.ms/new-console-template for more information
using SqlServerInterrogator.Services;

Console.WriteLine("Hello, World!");

var connectionString = "Server=localhost;Integrated Security=True;TrustServerCertificate=True;MultipleActiveResultSets=True;";

var serverInfo = await ServerInterrogator.GetFullServerInfoAsync(connectionString, CancellationToken.None);


Console.WriteLine("");