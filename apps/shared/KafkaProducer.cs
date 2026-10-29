using Confluent.Kafka;

namespace OrderFlow.Shared;

public sealed class KafkaProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly IAdminClient _adminClient;
    private readonly KafkaState _state;
    private readonly ILogger<KafkaProducer> _logger;
    private readonly object _connectionGate = new();

    public KafkaProducer(OrderFlowOptions options, KafkaState state, ILogger<KafkaProducer> logger)
    {
        _state = state;
        _logger = logger;
        var clientId = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "orderflow";
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.KafkaBrokers,
            ClientId = clientId,
            AllowAutoCreateTopics = false,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
        _adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = options.KafkaBrokers,
            ClientId = $"{clientId}-health"
        }).Build();
    }

    public void EnsureConnected()
    {
        lock (_connectionGate)
        {
            if (_state.ProducerReady) return;
            _adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            _state.ProducerReady = true;
            _logger.LogInformation("Kafka producer connected");
        }
    }

    public Task<DeliveryResult<string, string>> ProduceAsync(
        string topic,
        string key,
        string value,
        Headers? headers,
        CancellationToken cancellationToken) =>
        _producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key,
            Value = value,
            Headers = headers
        }, cancellationToken);

    public void Dispose()
    {
        _state.ProducerReady = false;
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        _adminClient.Dispose();
    }
}
