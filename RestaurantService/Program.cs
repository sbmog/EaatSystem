using RabbitMQ.Client;
using RestaurantService.Messages;
using Scalar.AspNetCore;
using System.Text;
using System.Text.Json;


var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOpenApi();

// Registrerer lytteren (Worker), så den starter sammen med appen
builder.Services.AddHostedService<OrderConsumer>();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();

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

app.MapPost("/api/restaurant/orders/{id}/ready", async (Guid id) =>
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();

    await channel.QueueDeclareAsync("OrderReadyQueue", false, false, false);

    var readyMsg = new OrderReadyMessage(id);
    var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(readyMsg));

    await channel.BasicPublishAsync(string.Empty, "OrderReadyQueue", body);

    return Results.Ok(new { message = $"Ordre {id} er klar og udbudt til bude!" });
});

app.Run();