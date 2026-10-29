using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Dapper;
using Npgsql;
using OrderFlow.Shared;

namespace OrderFlow.Worker;

public sealed class KafkaConsumerService(
    OrderFlowOptions options,
    KafkaState kafkaState,
    KafkaProducer producer,
    OrderEventProcessor processor,
    NpgsqlDataSource dataSource,
    ILogger<KafkaConsumerService> logger) : BackgroundService
{
    private static readonly string[] Topics =
    [
        "orders.created",
        "inventory.reserved",
        "inventory.rejected",
        "payments.approved",
        "payments.rejected"
    ];

    private static readonly JsonSerializerOptions EventJson = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = options.KafkaBrokers,
            GroupId = options.KafkaGroupId,
            ClientId = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "orderflow-worker",
            AllowAutoCreateTopics = false,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
        }).Build();

        try
        {
            producer.EnsureConnected();
            consumer.Subscribe(Topics);
            kafkaState.ConsumerReady = true;
            logger.LogInformation("Kafka consumer group {GroupId} subscribed to {Topics}", options.KafkaGroupId, Topics);

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException exception)
                {
                    logger.LogError(exception, "Kafka consume failed with {Reason}", exception.Error.Reason);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    continue;
                }

                await HandleMessage(result, stoppingToken);
                try
                {
                    consumer.Commit(result);
                }
                catch (KafkaException exception)
                {
                    // The database remains the source of truth. A redelivery is safe because processed_events is idempotent.
                    logger.LogWarning(exception, "Could not commit offset {Offset} for {TopicPartition}",
                        result.Offset, result.TopicPartition);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
        finally
        {
            kafkaState.ConsumerReady = false;
            consumer.Close();
        }
    }

    private async Task HandleMessage(ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        OrderEvent? orderEvent;
        try
        {
            orderEvent = JsonSerializer.Deserialize<OrderEvent>(result.Message.Value, EventJson)
                ?? throw new JsonException("The event payload is null");
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            await DeadLetter(result, null, exception, 1, cancellationToken);
            RecordConsumed(result.Topic, "dead_letter");
            return;
        }

        var parent = ExtractParent(result.Message.Headers);
        using var activity = Telemetry.ActivitySource.StartActivity(
            $"process {result.Topic}", ActivityKind.Consumer, parent);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", result.Topic);
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.kafka.message.offset", result.Offset.Value);

        Exception? lastError = null;
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                var outcome = await processor.Process(result.Topic, orderEvent, cancellationToken);
                activity?.SetTag("orderflow.processing.attempts", attempt);
                RecordConsumed(result.Topic, outcome);
                return;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                lastError = exception;
                logger.LogWarning(exception, "Attempt {Attempt} failed for event {EventId} from {Topic}",
                    attempt, orderEvent.EventId, result.Topic);
                if (attempt < 3)
                    await Task.Delay(TimeSpan.FromMilliseconds(250 * Math.Pow(2, attempt - 1)), cancellationToken);
            }
        }

        var finalError = lastError ?? new InvalidOperationException("Event processing failed without an exception");
        activity?.SetStatus(ActivityStatusCode.Error, finalError.Message);
        activity?.AddException(finalError);
        await DeadLetter(result, orderEvent, finalError, 3, cancellationToken);
        RecordConsumed(result.Topic, "dead_letter");
    }

    private async Task DeadLetter(
        ConsumeResult<string, string> result,
        OrderEvent? orderEvent,
        Exception exception,
        int attempts,
        CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Serialize(new
        {
            sourceTopic = result.Topic,
            key = result.Message.Key,
            attempts,
            failedAt = DateTimeOffset.UtcNow,
            error = exception.Message,
            originalEvent = orderEvent is null ? JsonSerializer.SerializeToElement(result.Message.Value) : JsonSerializer.SerializeToElement(orderEvent, EventJson)
        }, EventJson);

        producer.EnsureConnected();
        await producer.ProduceAsync("order-processing.dlq", result.Message.Key ?? "unknown", payload, null, cancellationToken);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO dead_letter_events (event_id, topic, event_key, payload, error_message, attempts)
            VALUES (@EventId, @Topic, @EventKey, CAST(@Payload AS jsonb), @ErrorMessage, @Attempts)
            """,
            new
            {
                EventId = orderEvent?.EventId,
                Topic = result.Topic,
                EventKey = result.Message.Key,
                Payload = payload,
                ErrorMessage = exception.Message,
                Attempts = attempts
            }));
        Telemetry.DeadLetters.Add(1, new KeyValuePair<string, object?>("source_topic", result.Topic));
        logger.LogError(exception, "Moved event {EventId} from {Topic} to the dead-letter topic", orderEvent?.EventId, result.Topic);
    }

    private static ActivityContext ExtractParent(Headers headers)
    {
        var bytes = headers?.GetLastBytes("traceparent");
        if (bytes is not null && ActivityContext.TryParse(Encoding.UTF8.GetString(bytes), null, out var parent))
            return parent;
        return default;
    }

    private static void RecordConsumed(string topic, string result) =>
        Telemetry.KafkaMessages.Add(1,
            new("service", Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "orderflow-worker"),
            new("direction", "consumed"), new("topic", topic), new("result", result));
}
