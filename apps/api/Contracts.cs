using System.Text.Json;
using System.Text.Json.Serialization;

namespace OrderFlow.Api;

public sealed record CreateOrderRequest(
    [property: JsonPropertyName("customerName")] string? CustomerName,
    [property: JsonPropertyName("items")] IReadOnlyList<CreateOrderItemRequest>? Items);

public sealed record CreateOrderItemRequest(
    [property: JsonPropertyName("productId")] Guid ProductId,
    [property: JsonPropertyName("quantity")] int Quantity);

public sealed record ProductResponse(Guid Id, string Name, string Description, decimal Price, int AvailableQuantity);
public sealed record OrderSummaryResponse(Guid Id, string CustomerName, decimal TotalAmount, string Status, DateTime CreatedAt, DateTime UpdatedAt);
public sealed record OrderItemResponse(Guid ProductId, string Name, int Quantity, decimal UnitPrice);
public sealed record OrderEventResponse(Guid Id, string EventType, JsonElement EventData, DateTime CreatedAt);
public sealed record OrderResponse(
    Guid Id,
    string CustomerName,
    decimal TotalAmount,
    string Status,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    IReadOnlyList<OrderItemResponse> Items,
    IReadOnlyList<OrderEventResponse> Events);

public sealed record CreatedOrderResponse(Guid Id, string Status, decimal TotalAmount);
