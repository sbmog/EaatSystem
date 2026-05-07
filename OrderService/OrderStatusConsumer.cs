using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using System.Collections.Concurrent;
using OrderService.Messages;

public class OrderStatusConsumer : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, string> _db;

    // Vi injicerer vores in-memory database
    public OrderStatusConsumer(ConcurrentDictionary<Guid, string> db)
    {
        _db = db;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync("OrderStatusQueue", false, false, false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var statusUpdate = JsonSerializer.Deserialize<OrderStatusMessage>(message);

            if (statusUpdate != null)
            {
                // Opdaterer status i databasen
                _db[statusUpdate.OrderId] = statusUpdate.Status;
                Console.WriteLine($"[ORDER SERVICE] Ordre {statusUpdate.OrderId} er nu: {statusUpdate.Status}");
            }

            await channel.BasicAckAsync(ea.DeliveryTag, false);
        };

        await channel.BasicConsumeAsync("OrderStatusQueue", false, consumer);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}