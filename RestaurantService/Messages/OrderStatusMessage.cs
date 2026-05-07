namespace RestaurantService.Messages;
public record OrderStatusMessage(Guid OrderId, string Status);