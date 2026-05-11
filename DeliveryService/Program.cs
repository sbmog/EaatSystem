using RabbitMQ.Client;
using Scalar.AspNetCore;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);

// Opsæt Scalar og in-memory database
builder.Services.AddOpenApi();
builder.Services.AddSingleton<ConcurrentDictionary<Guid, bool>>();
builder.Services.AddHostedService<OrderReadyConsumer>();

var app = builder.Build();

app.MapOpenApi();
app.MapScalarApiReference();

try
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    await using var connection = await factory.CreateConnectionAsync();
    await using var channel = await connection.CreateChannelAsync();
    Console.WriteLine("DeliveryService er klar og lytter!");
}
catch (Exception ex)
{
    Console.WriteLine($"DeliveryService fejl: {ex.Message}");
}

// Endpoint for bud: Først til mølle
app.MapPost("/api/deliveries/{id}/accept", (Guid id, string budNavn, ConcurrentDictionary<Guid, bool> availableOrders) =>
{
    // Tjek om ordren overhovedet findes i udbuddet
if (!availableOrders.ContainsKey(id))
{
    return Results.NotFound(new { message = "Ordren findes ikke eller er ikke klar." });
}

    // FØRST TIL MØLLE LOGIK:
    // TryUpdate forsøger at ændre false til true. Den returnerer true, hvis det lykkes.
bool wasAvailable = availableOrders.TryUpdate(id, true, false);

if (wasAvailable)
{
        // Du vandt løbet!
        // TODO: Her kunne du sende en besked til RabbitMQ om at ordren er "På vej" (OrderPickedUp)
    return Results.Ok(new { message = $"Tillykke {budNavn}! Du har fået turen." });
}
else
{
        // En anden nåede det før dig (værdien var allerede true)
    return Results.Conflict(new { message = $"Beklager {budNavn}, opgaven er ikke længere tilgængelig." });
}
});

app.Run();