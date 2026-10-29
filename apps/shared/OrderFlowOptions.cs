namespace OrderFlow.Shared;

public sealed record OrderFlowOptions(
    string DatabaseUrl,
    string KafkaBrokers,
    string KafkaGroupId,
    double PaymentFailureRate)
{
    public static OrderFlowOptions FromConfiguration(IConfiguration configuration)
    {
        var databaseUrl = configuration["DATABASE_URL"]
            ?? "Host=localhost;Port=5438;Database=orderflow;Username=orderflow;Password=orderflow-local-only";
        var kafkaBrokers = configuration["KAFKA_BROKERS"] ?? "localhost:29098";
        var kafkaGroupId = configuration["KAFKA_GROUP_ID"] ?? "orderflow-worker";
        var failureRate = double.TryParse(configuration["PAYMENT_FAILURE_RATE"], out var parsed) ? parsed : 0.10;

        return new(databaseUrl, kafkaBrokers, kafkaGroupId, Math.Clamp(failureRate, 0, 1));
    }
}
