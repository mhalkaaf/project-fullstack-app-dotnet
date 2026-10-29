using System.Data;
using System.Diagnostics;
using System.Text.Json;
using Dapper;
using Npgsql;
using OrderFlow.Shared;

namespace OrderFlow.Api;

public sealed class OrderRepository(NpgsqlDataSource dataSource, ILogger<OrderRepository> logger)
{
    private static readonly JsonSerializerOptions EventJson = new(JsonSerializerDefaults.Web);

    public async Task<IReadOnlyList<ProductResponse>> GetProducts(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var products = await connection.QueryAsync<ProductResponse>(new CommandDefinition(
            """
            SELECT id, name, description, price, available_quantity
            FROM products
            ORDER BY name
            """,
            cancellationToken: cancellationToken));
        return products.AsList();
    }

    public async Task<IReadOnlyList<OrderSummaryResponse>> GetOrders(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var orders = await connection.QueryAsync<OrderSummaryResponse>(new CommandDefinition(
            """
            SELECT id, customer_name, total_amount, status, created_at, updated_at
            FROM orders
            ORDER BY created_at DESC
            LIMIT 50
            """,
            cancellationToken: cancellationToken));
        return orders.AsList();
    }

    public async Task<OrderResponse?> GetOrder(Guid orderId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var order = await connection.QuerySingleOrDefaultAsync<OrderSummaryResponse>(new CommandDefinition(
            """
            SELECT id, customer_name, total_amount, status, created_at, updated_at
            FROM orders
            WHERE id = @OrderId
            """,
            new { OrderId = orderId }, cancellationToken: cancellationToken));
        if (order is null) return null;

        var items = (await connection.QueryAsync<OrderItemResponse>(new CommandDefinition(
            """
            SELECT p.id AS product_id, p.name, oi.quantity, oi.unit_price
            FROM order_items oi
            JOIN products p ON p.id = oi.product_id
            WHERE oi.order_id = @OrderId
            ORDER BY p.name
            """,
            new { OrderId = orderId }, cancellationToken: cancellationToken))).AsList();

        var rows = (await connection.QueryAsync<EventRow>(new CommandDefinition(
            """
            SELECT id, event_type, event_data::text AS event_data, created_at
            FROM order_events
            WHERE order_id = @OrderId
            ORDER BY created_at, id
            """,
            new { OrderId = orderId }, cancellationToken: cancellationToken))).AsList();
        var events = rows.Select(row => new OrderEventResponse(
            row.Id, row.EventType, JsonSerializer.Deserialize<JsonElement>(row.EventData, EventJson), row.CreatedAt)).ToList();

        return new(order.Id, order.CustomerName, order.TotalAmount, order.Status,
            order.CreatedAt, order.UpdatedAt, items, events);
    }

    public async Task<CreatedOrderResponse> CreateOrder(CreateOrderRequest request, CancellationToken cancellationToken)
    {
        var customerName = request.CustomerName?.Trim() ?? string.Empty;
        var items = request.Items ?? [];
        if (customerName.Length is < 1 or > 120)
            throw new ApiException(400, "customerName must contain between 1 and 120 characters");
        if (items.Count == 0 || items.Any(item => item.ProductId == Guid.Empty || item.Quantity < 1))
            throw new ApiException(400, "items must contain a productId and a positive integer quantity");

        var normalized = items
            .GroupBy(item => item.ProductId)
            .ToDictionary(group => group.Key, group => group.Sum(item => item.Quantity));

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        try
        {
            var products = (await connection.QueryAsync<ProductRow>(new CommandDefinition(
                """
                SELECT id, name, price, available_quantity
                FROM products
                WHERE id = ANY(@ProductIds)
                FOR SHARE
                """,
                new { ProductIds = normalized.Keys.ToArray() }, transaction,
                cancellationToken: cancellationToken))).AsList();
            if (products.Count != normalized.Count)
                throw new ApiException(400, "One or more products do not exist");

            var total = products.Sum(product => product.Price * normalized[product.Id]);
            var orderId = Guid.NewGuid();
            var eventId = Guid.NewGuid();
            var occurredAt = DateTimeOffset.UtcNow;
            var traceId = Activity.Current?.TraceId.ToHexString();
            var traceparent = Activity.Current?.Id;
            var eventPayload = JsonSerializer.Serialize(new
            {
                eventId,
                eventType = "OrderCreated",
                eventVersion = 1,
                occurredAt,
                traceId,
                data = new
                {
                    orderId,
                    customerName,
                    totalAmount = total.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture),
                    items = products.Select(product => new
                    {
                        productId = product.Id,
                        product.Name,
                        quantity = normalized[product.Id],
                        unitPrice = product.Price
                    })
                }
            }, EventJson);

            await connection.ExecuteAsync(new CommandDefinition(
                "INSERT INTO orders (id, customer_name, total_amount, status) VALUES (@OrderId, @CustomerName, @Total, 'PENDING')",
                new { OrderId = orderId, CustomerName = customerName, Total = total }, transaction,
                cancellationToken: cancellationToken));

            foreach (var product in products)
            {
                await connection.ExecuteAsync(new CommandDefinition(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (@OrderId, @ProductId, @Quantity, @UnitPrice)
                    """,
                    new
                    {
                        OrderId = orderId,
                        ProductId = product.Id,
                        Quantity = normalized[product.Id],
                        UnitPrice = product.Price
                    }, transaction, cancellationToken: cancellationToken));
            }

            await connection.ExecuteAsync(new CommandDefinition(
                """
                INSERT INTO order_events (id, order_id, event_type, event_data)
                VALUES (@EventId, @OrderId, 'OrderCreated', CAST(@Payload AS jsonb));
                INSERT INTO outbox_events (id, aggregate_id, event_type, topic, payload, traceparent)
                VALUES (@EventId, @OrderId, 'OrderCreated', 'orders.created', CAST(@Payload AS jsonb), @Traceparent)
                """,
                new { EventId = eventId, OrderId = orderId, Payload = eventPayload, Traceparent = traceparent }, transaction,
                cancellationToken: cancellationToken));

            await transaction.CommitAsync(cancellationToken);
            Telemetry.OrdersCreated.Add(1);
            logger.LogInformation("Created order {OrderId} with event {EventId}", orderId, eventId);
            return new(orderId, "PENDING", total);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private sealed record ProductRow(Guid Id, string Name, decimal Price, int AvailableQuantity);
    private sealed record EventRow(Guid Id, string EventType, string EventData, DateTime CreatedAt);
}
