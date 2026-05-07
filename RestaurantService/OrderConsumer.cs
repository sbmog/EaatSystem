using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using RestaurantService.Messages;
public class OrderConsumer : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(queue: "OrderQueue", durable: false, exclusive: false, autoDelete: false);

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var order = JsonSerializer.Deserialize<OrderMessage>(message);

            Console.WriteLine($"[RESTAURANT] Modtog ordre: {order?.OrderId}. Accepterer...");

            // Opret svar og send til ny kø
            var statusMsg = new OrderStatusMessage(order.OrderId, "Tilberedes");
            var replyBody = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(statusMsg));

            await channel.QueueDeclareAsync("OrderStatusQueue", false, false, false);
            await channel.BasicPublishAsync(string.Empty, "OrderStatusQueue", replyBody);

            // Marker oprindelig besked som håndteret
            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
        };

        await channel.BasicConsumeAsync(queue: "OrderQueue", autoAck: false, consumer: consumer);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}
