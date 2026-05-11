using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using System.Collections.Concurrent;
using DeliveryService.Messages;
public class OrderReadyConsumer : BackgroundService
{
    // Databasen: Nøglen er OrderId, Værdien (bool) er om turen er TAGET (true/false)
    private readonly ConcurrentDictionary<Guid, bool> _availableOrders;

    public OrderReadyConsumer(ConcurrentDictionary<Guid, bool> availableOrders)
    {
        _availableOrders = availableOrders;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync("OrderReadyQueue", false, false, false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var orderReady = JsonSerializer.Deserialize<OrderReadyMessage>(message);

            if (orderReady != null)
            {
                // Sætter ordren tilgængelig (false = ikke taget)
                _availableOrders.TryAdd(orderReady.OrderId, false);
                Console.WriteLine($"[DELIVERY] Opgave i udbud! Første bud til mølle for ordre: {orderReady.OrderId}");
            }

            await channel.BasicAckAsync(ea.DeliveryTag, false);
        };

        await channel.BasicConsumeAsync("OrderReadyQueue", false, consumer);
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}