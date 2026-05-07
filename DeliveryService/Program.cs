using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);


var app = builder.Build();

try
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();
    Console.WriteLine("RestaurantService er klar og lytter!");
}
catch (Exception ex)
{
    Console.WriteLine($"RestaurantService fejl: {ex.Message}");
}

app.Run();