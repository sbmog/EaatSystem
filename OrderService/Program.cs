using RabbitMQ.Client;
using System.Text.Json;
using System.Text;
using OrderService.Messages;
using System.Collections.Concurrent;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOpenApi();

builder.Services.AddSingleton<ConcurrentDictionary<Guid, string>>();
builder.Services.AddHostedService<OrderStatusConsumer>();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();

try
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();
    Console.WriteLine("OrderService er klar og forbundet til RabbitMQ!");
}
catch (Exception ex)
{
    Console.WriteLine($"OrderService kunne ikke forbinde: {ex.Message}");
}

// POST - Kunden placerer en ordre
app.MapPost("/api/orders", async (ConcurrentDictionary<Guid, string> db) =>
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();

    await channel.QueueDeclareAsync("OrderQueue", false, false, false);

    var order = new OrderMessage(Guid.NewGuid());
    var message = JsonSerializer.Serialize(order);
    var body = Encoding.UTF8.GetBytes(message);

    await channel.BasicPublishAsync(string.Empty, "OrderQueue", body);

    // Gem i vores simulerede database
    db[order.OrderId] = "Oprettet";

    return Results.Ok(new { message = "Ordre sendt!", OrderId = order.OrderId });
});

// GET - Ordre
app.MapGet("/api/orders/{id}/status", (Guid id, ConcurrentDictionary<Guid, string> db) =>
{
    return db.TryGetValue(id, out var status)
        ? Results.Ok(new { OrderId = id, Status = status })
        : Results.NotFound();
});

app.Run();