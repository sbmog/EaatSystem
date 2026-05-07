namespace OrderService.Messages;
public record OrderStatusMessage(Guid OrderId, string Status);