using System.Text.Json;

namespace OrderFlow.Worker;

public sealed record OrderEvent(
    Guid EventId,
    string EventType,
    int EventVersion,
    DateTimeOffset OccurredAt,
    string? TraceId,
    JsonElement Data);
