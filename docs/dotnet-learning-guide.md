# .NET 10 Observability and Messaging Learning Guide

This migration intentionally exposes the mechanics instead of hiding them behind a framework. The browser contract, PostgreSQL schema, Kafka topics, and Grafana backends are unchanged, which makes the Node-to-.NET differences easy to compare in Git history.

## Where to start

| Concern | .NET implementation | What to learn |
|---|---|---|
| HTTP API | `apps/api/Program.cs` | Minimal APIs, middleware, dependency injection, JSON contracts |
| Database transactions | `apps/api/OrderRepository.cs` | Npgsql, Dapper, transaction boundaries, parameterized SQL |
| Outbox publishing | `apps/shared/OutboxPublisher.cs` | `BackgroundService`, polling, row locking, at-least-once delivery |
| Kafka producer | `apps/shared/KafkaProducer.cs` | acknowledgements, idempotent producer, headers, broker readiness |
| Kafka consumer | `apps/worker/KafkaConsumerService.cs` | consumer groups, partitions, retry/DLQ, manual offset commits |
| Business workflow | `apps/worker/OrderEventProcessor.cs` | idempotent claims and state changes in one database transaction |
| Telemetry setup | `apps/shared/HostingExtensions.cs` | OpenTelemetry resources, instrumentation, OTLP exporters |
| Custom signals | `apps/shared/Telemetry.cs` | `ActivitySource`, `Meter`, counters, histograms, cardinality |

## Message lifecycle

```text
HTTP request
  -> database transaction: order + OrderCreated + outbox row
  -> outbox publisher locks row and publishes to Kafka
  -> worker receives message and extracts traceparent
  -> database transaction: idempotency claim + state change + next outbox row
  -> worker commits the Kafka offset
  -> either API or worker publisher sends the next transition
```

The important boundary is database commit before Kafka offset commit. If the worker crashes between those operations, Kafka redelivers the message and the `processed_events` primary key turns it into a no-op. This is at-least-once delivery with idempotent effects, not exactly once.

The outbox closes a different failure window: business data and the intent to publish commit together. Publishing and marking a row published cannot be one atomic transaction across PostgreSQL and Kafka, so a crash can still duplicate a message. Consumer idempotency is therefore required even with an idempotent Kafka producer.

## The three telemetry signals

### Traces

ASP.NET Core creates the request activity and Npgsql creates database activities. OrderFlow creates producer and consumer activities around Kafka because Confluent.Kafka does not create the application-specific span chain here. The active W3C `traceparent` is persisted in the outbox, injected into a Kafka header during publication, and extracted by the consumer.

In Tempo, follow one trace from `POST /api/orders` through `publish orders.created`, `process orders.created`, and later transitions. A new producer trace may be created after a long delay, but the stored parent preserves causality.

### Metrics

`System.Diagnostics.Metrics` instruments describe rates and distributions without storing one record per operation. Tags are deliberately bounded: route templates, status codes, topics, directions, and outcomes. Order IDs, event IDs, customer names, and exception messages are forbidden as metric tags because their unbounded cardinality would make Prometheus expensive and unreliable.

The applications export OTLP metrics to the collector. Prometheus scrapes the collector's translated endpoint, and Grafana queries Prometheus. This separates instrumentation from the storage vendor.

### Logs

`ILogger` emits structured JSON to stdout. The .NET logging scope adds trace/span identifiers; message templates keep fields such as `OrderId`, `EventId`, and `Topic` queryable. Alloy discovers the Docker containers and sends stdout to Loki. Logs are useful for discrete details; they should not replace metrics or traces.

## Reliability experiments

1. Create an order and locate the same operation in Grafana metrics, Tempo traces, and Loki logs.
2. Stop the worker, create orders, inspect Kafka lag, then restart it and watch recovery.
3. Stop Kafka, create an order, inspect `outbox_events`, restart Kafka, and observe publication.
4. Scale workers with `docker compose up -d --scale worker=3`; inspect partition assignment and confirm one order remains ordered because its ID is the Kafka key.
5. Publish the same valid event twice and confirm the second delivery logs `duplicate` without repeating inventory changes.
6. Publish invalid JSON to a consumed topic and inspect both `order-processing.dlq` and `dead_letter_events`.
7. Raise `PAYMENT_FAILURE_RATE` and distinguish expected business failure metrics from infrastructure errors.

## Important production gaps

The repository remains an educational single-node environment. A production design needs authenticated/encrypted Kafka and PostgreSQL, schema evolution and compatibility checks, database migrations, secret management, retention/capacity planning, broker and consumer-lag exporters, multi-window SLO burn alerts, tested backups, and a deliberate DLQ replay tool. Avoid adding automatic DLQ replay until event correction, deduplication, and operator controls are defined.
