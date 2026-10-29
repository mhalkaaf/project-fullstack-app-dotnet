using System.Diagnostics;
using Confluent.Kafka;
using Dapper;
using Npgsql;

namespace OrderFlow.Shared;

public sealed class OutboxPublisher(
    NpgsqlDataSource dataSource,
    KafkaProducer producer,
    KafkaState kafkaState,
    ILogger<OutboxPublisher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                producer.EnsureConnected();
                await PublishBatch(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                kafkaState.ProducerReady = false;
                logger.LogError(exception, "Outbox publishing cycle failed");
            }

            await timer.WaitForNextTickAsync(stoppingToken);
        }
    }

    private async Task PublishBatch(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        List<OutboxRow> rows = (await connection.QueryAsync<OutboxRow>(new CommandDefinition(
            """
            SELECT id AS Id, aggregate_id AS AggregateId, topic AS Topic,
                   payload::text AS Payload, traceparent AS Traceparent
            FROM outbox_events
            WHERE published_at IS NULL
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 25
            """,
            transaction: transaction,
            cancellationToken: cancellationToken))).ToList();

        foreach (var row in rows)
        {
            var parent = default(ActivityContext);
            if (row.Traceparent is not null)
                ActivityContext.TryParse(row.Traceparent, null, out parent);

            using var activity = Telemetry.ActivitySource.StartActivity(
                $"publish {row.Topic}",
                ActivityKind.Producer,
                parent);
            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.destination.name", row.Topic);
            activity?.SetTag("messaging.operation.type", "publish");
            activity?.SetTag("orderflow.order.id", row.AggregateId);

            var headers = new Headers();
            if (Activity.Current?.Id is { } traceparent)
                headers.Add("traceparent", System.Text.Encoding.UTF8.GetBytes(traceparent));

            try
            {
                await producer.ProduceAsync(row.Topic, row.AggregateId.ToString(), row.Payload, headers, cancellationToken);
                await connection.ExecuteAsync(new CommandDefinition(
                    "UPDATE outbox_events SET published_at = now(), attempts = attempts + 1 WHERE id = @Id",
                    new { row.Id }, transaction, cancellationToken: cancellationToken));
                Telemetry.KafkaMessages.Add(1,
                    new("service", Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "orderflow"),
                    new("direction", "produced"), new("topic", row.Topic), new("result", "success"));
                logger.LogInformation("Published outbox event {EventId} for order {OrderId} to {Topic}", row.Id, row.AggregateId, row.Topic);
            }
            catch (Exception exception)
            {
                activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
                activity?.AddException(exception);
                Telemetry.KafkaMessages.Add(1,
                    new("service", Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "orderflow"),
                    new("direction", "produced"), new("topic", row.Topic), new("result", "error"));
                throw;
            }
        }

        await transaction.CommitAsync(cancellationToken);

        var pending = await connection.ExecuteScalarAsync<long>(new CommandDefinition(
            "SELECT count(*) FROM outbox_events WHERE published_at IS NULL",
            cancellationToken: cancellationToken));
        Telemetry.SetOutboxPending(pending);
    }

    private sealed record OutboxRow(Guid Id, Guid AggregateId, string Topic, string Payload, string? Traceparent);
}
