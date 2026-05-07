using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

// Registrerer lytteren (Worker), så den starter sammen med appen
builder.Services.AddHostedService<OrderConsumer>();

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