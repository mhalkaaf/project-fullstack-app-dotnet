using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace OrderFlow.Shared;

public static class Telemetry
{
    public const string ActivitySourceName = "OrderFlow";
    public const string MeterName = "OrderFlow";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    public static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> HttpRequests = Meter.CreateCounter<long>("orderflow_http_requests");
    public static readonly Histogram<double> HttpDuration = Meter.CreateHistogram<double>("orderflow_http_request_duration", "s");
    public static readonly Counter<long> OrdersCreated = Meter.CreateCounter<long>("orderflow_orders_created");
    public static readonly Counter<long> KafkaMessages = Meter.CreateCounter<long>("orderflow_kafka_messages");
    public static readonly Counter<long> OrdersProcessed = Meter.CreateCounter<long>("orderflow_orders_processed");
    public static readonly Histogram<double> OrderProcessingDuration = Meter.CreateHistogram<double>("orderflow_order_processing_duration", "s");
    public static readonly Counter<long> DeadLetters = Meter.CreateCounter<long>("orderflow_dead_letter");

    private static long _outboxPending;

    static Telemetry()
    {
        Meter.CreateObservableGauge("orderflow_outbox_pending", () => Volatile.Read(ref _outboxPending));
    }

    public static void SetOutboxPending(long value) => Interlocked.Exchange(ref _outboxPending, value);
}
