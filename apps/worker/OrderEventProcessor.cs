using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Dapper;
using Npgsql;
using OrderFlow.Shared;

namespace OrderFlow.Worker;

public sealed class OrderEventProcessor(
    NpgsqlDataSource dataSource,
    OrderFlowOptions options,
    ILogger<OrderEventProcessor> logger)
{
    private static readonly JsonSerializerOptions EventJson = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    public async Task<string> Process(string topic, OrderEvent orderEvent, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        try
        {
            var claimed = await connection.ExecuteAsync(new CommandDefinition(
                """
                INSERT INTO processed_events (event_id, consumer_name)
                VALUES (@EventId, @ConsumerName)
                ON CONFLICT DO NOTHING
                """,
                new { orderEvent.EventId, ConsumerName = options.KafkaGroupId }, transaction,
                cancellationToken: cancellationToken));
            if (claimed == 0)
            {
                await transaction.CommitAsync(cancellationToken);
                logger.LogInformation("Ignored duplicate event {EventId} from {Topic}", orderEvent.EventId, topic);
                return "duplicate";
            }

            switch (topic)
            {
                case "orders.created":
                    await HandleOrderCreated(connection, transaction, orderEvent, cancellationToken);
                    break;
                case "inventory.reserved":
                    await HandleInventoryReserved(connection, transaction, orderEvent, cancellationToken);
                    break;
                case "inventory.rejected":
                case "payments.rejected":
                    await HandleRejected(connection, transaction, orderEvent, cancellationToken);
                    break;
                case "payments.approved":
                    await HandlePaymentApproved(connection, transaction, orderEvent, cancellationToken);
                    break;
                default:
                    throw new InvalidDataException($"No handler is registered for topic {topic}");
            }

            await transaction.CommitAsync(cancellationToken);
            logger.LogInformation("Processed event {EventId} for order {OrderId} from {Topic}",
                orderEvent.EventId, GetOrderId(orderEvent), topic);
            return "success";
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }
    }

    private async Task HandleOrderCreated(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        OrderEvent orderEvent,
        CancellationToken cancellationToken)
    {
        var orderId = GetOrderId(orderEvent);
        if (!orderEvent.Data.TryGetProperty("items", out var itemsElement) || itemsElement.ValueKind != JsonValueKind.Array)
            throw new InvalidDataException("OrderCreated data.items must be an array");

        var items = itemsElement.EnumerateArray().Select(item => new EventItem(
            item.GetProperty("productId").GetGuid(),
            item.GetProperty("quantity").GetInt32())).ToList();
        if (items.Count == 0 || items.Any(item => item.Quantity < 1))
            throw new InvalidDataException("OrderCreated must contain valid items");

        var products = (await connection.QueryAsync<ProductStock>(new CommandDefinition(
            """
            SELECT id, available_quantity
            FROM products
            WHERE id = ANY(@ProductIds)
            ORDER BY id
            FOR UPDATE
            """,
            new { ProductIds = items.Select(item => item.ProductId).ToArray() }, transaction,
            cancellationToken: cancellationToken))).ToDictionary(product => product.Id);
        var rejected = items.FirstOrDefault(item =>
            !products.TryGetValue(item.ProductId, out var product) || product.AvailableQuantity < item.Quantity);

        if (rejected is not null)
        {
            await AddTransition(connection, transaction, orderEvent, "InventoryRejected", "inventory.rejected",
                "INVENTORY_REJECTED", new { reason = "INSUFFICIENT_STOCK", productId = rejected.ProductId }, cancellationToken);
            return;
        }

        foreach (var item in items)
        {
            await connection.ExecuteAsync(new CommandDefinition(
                """
                UPDATE products
                SET available_quantity = available_quantity - @Quantity, updated_at = now()
                WHERE id = @ProductId
                """,
                item, transaction, cancellationToken: cancellationToken));
        }

        await AddTransition(connection, transaction, orderEvent, "InventoryReserved", "inventory.reserved",
            "INVENTORY_RESERVED", new { }, cancellationToken);
    }

    private Task HandleInventoryReserved(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        OrderEvent orderEvent,
        CancellationToken cancellationToken) =>
        Random.Shared.NextDouble() < options.PaymentFailureRate
            ? AddTransition(connection, transaction, orderEvent, "PaymentRejected", "payments.rejected",
                "PAYMENT_REJECTED", new { reason = "SIMULATED_PAYMENT_REJECTION" }, cancellationToken)
            : AddTransition(connection, transaction, orderEvent, "PaymentApproved", "payments.approved",
                "PAYMENT_APPROVED", new { }, cancellationToken);

    private async Task HandlePaymentApproved(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        OrderEvent orderEvent,
        CancellationToken cancellationToken)
    {
        await AddTransition(connection, transaction, orderEvent, "OrderCompleted", "orders.completed",
            "COMPLETED", new { }, cancellationToken);
        await RecordTerminalMetric(connection, transaction, GetOrderId(orderEvent), "completed", cancellationToken);
    }

    private async Task HandleRejected(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        OrderEvent orderEvent,
        CancellationToken cancellationToken)
    {
        var reason = orderEvent.Data.TryGetProperty("reason", out var reasonElement)
            ? reasonElement.GetString() ?? orderEvent.EventType
            : orderEvent.EventType;
        await AddTransition(connection, transaction, orderEvent, "OrderFailed", "orders.failed",
            "FAILED", new { reason }, cancellationToken);
        await RecordTerminalMetric(connection, transaction, GetOrderId(orderEvent), "failed", cancellationToken);
    }

    private static async Task AddTransition(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        OrderEvent sourceEvent,
        string eventType,
        string topic,
        string status,
        object extra,
        CancellationToken cancellationToken)
    {
        var eventId = Guid.NewGuid();
        var orderId = GetOrderId(sourceEvent);
        var eventPayload = JsonSerializer.Serialize(new
        {
            eventId,
            eventType,
            eventVersion = 1,
            occurredAt = DateTimeOffset.UtcNow,
            traceId = Activity.Current?.TraceId.ToHexString() ?? sourceEvent.TraceId,
            causationId = sourceEvent.EventId,
            data = MergeOrderData(orderId, extra)
        }, EventJson);

        await connection.ExecuteAsync(new CommandDefinition(
            """
            UPDATE orders SET status = @Status, updated_at = now() WHERE id = @OrderId;
            INSERT INTO order_events (id, order_id, event_type, event_data)
            VALUES (@EventId, @OrderId, @EventType, CAST(@Payload AS jsonb));
            INSERT INTO outbox_events (id, aggregate_id, event_type, topic, payload, traceparent)
            VALUES (@EventId, @OrderId, @EventType, @Topic, CAST(@Payload AS jsonb), @Traceparent)
            """,
            new
            {
                Status = status,
                OrderId = orderId,
                EventId = eventId,
                EventType = eventType,
                Topic = topic,
                Payload = eventPayload,
                Traceparent = Activity.Current?.Id
            }, transaction, cancellationToken: cancellationToken));
    }

    private static Dictionary<string, object?> MergeOrderData(Guid orderId, object extra)
    {
        var values = new Dictionary<string, object?> { ["orderId"] = orderId };
        var element = JsonSerializer.SerializeToElement(extra, EventJson);
        foreach (var property in element.EnumerateObject())
            values[property.Name] = property.Value.Clone();
        return values;
    }

    private static async Task RecordTerminalMetric(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid orderId,
        string outcome,
        CancellationToken cancellationToken)
    {
        var seconds = await connection.ExecuteScalarAsync<double>(new CommandDefinition(
            "SELECT extract(epoch FROM (now() - created_at)) FROM orders WHERE id = @OrderId",
            new { OrderId = orderId }, transaction, cancellationToken: cancellationToken));
        Telemetry.OrdersProcessed.Add(1, new KeyValuePair<string, object?>("outcome", outcome));
        Telemetry.OrderProcessingDuration.Record(seconds, new KeyValuePair<string, object?>("outcome", outcome));
    }

    private static Guid GetOrderId(OrderEvent orderEvent)
    {
        if (!orderEvent.Data.TryGetProperty("orderId", out var orderId) || !orderId.TryGetGuid(out var value))
            throw new InvalidDataException($"Event {orderEvent.EventId} has no valid data.orderId");
        return value;
    }

    private sealed record EventItem(Guid ProductId, int Quantity);
    private sealed record ProductStock(Guid Id, int AvailableQuantity);
}
