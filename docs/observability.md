# Observability and Reliability

## Telemetry model

OrderFlow uses the three complementary telemetry signals:

- Metrics answer whether a problem exists and how broad it is.
- Traces show where time or failure occurred across request and event boundaries.
- Logs explain discrete events with structured diagnostic context.

OpenTelemetry resource attributes include `service.name`, `service.version`, and `deployment.environment`. Application logs include `service`, `orderId`, `eventId`, and `traceId` when available.

## Metrics

Application metrics:

| Metric | Type | Purpose |
|---|---|---|
| `orderflow_http_requests_total` | counter | Request traffic and status classes |
| `orderflow_http_request_duration_seconds` | histogram | API latency percentiles |
| `orderflow_orders_created_total` | counter | Accepted customer orders |
| `orderflow_orders_processed_total` | counter | Terminal outcomes |
| `orderflow_order_processing_duration_seconds` | histogram | End-to-end workflow time |
| `orderflow_kafka_messages_total` | counter | Produced/consumed/error events |
| `orderflow_outbox_pending` | gauge | Unpublished database events |
| `orderflow_dead_letter_total` | counter | Poison/exhausted messages |

Runtime/process metrics are provided by the Prometheus client. PostgreSQL, Kafka, and host/container exporters can be added as the next exercise.

Never label metrics with order IDs, event IDs, trace IDs, customer names, or exception messages. Those belong in traces and logs.

## Tracing

The API and worker export OTLP traces to the collector. Instrumentation covers HTTP, PostgreSQL, and supported runtime operations. Kafka trace context is injected into message headers and extracted by consumers so asynchronous spans belong to the originating order trace.

Useful trace searches:

- Service name: `orderflow-api` or `orderflow-worker`
- Span status: error
- HTTP route: `POST /api/orders`
- Duration above the latency SLO threshold

## Logging

Applications write single-line JSON to stdout. Grafana Alloy discovers Compose containers through the Docker socket and forwards logs to Loki. This socket is effectively privileged access to the local Docker daemon; use a hardened collector pattern outside a developer workstation.

Suggested Loki queries:

```logql
{compose_project="orderflow"} | json
{service="orderflow-worker"} | json | level="error"
{compose_project="orderflow"} |= "order-id-here"
```

## SLI definitions

### API availability

Eligible requests exclude health checks and client-caused `4xx` validation responses.

```promql
sum(rate(orderflow_http_requests_total{route!~"GET /health.*",status_code!~"4..|5.."}[5m]))
/
sum(rate(orderflow_http_requests_total{route!~"GET /health.*",status_code!~"4.."}[5m]))
```

### Order API latency

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(orderflow_http_request_duration_seconds_bucket{route="POST /api/orders"}[5m])
  )
)
```

### Workflow success

```promql
sum(rate(orderflow_orders_processed_total{outcome="completed"}[30m]))
/
sum(rate(orderflow_orders_processed_total[30m]))
```

## SLOs and error budgets

| SLO | Target | Approximate 30-day error budget |
|---|---:|---:|
| API availability | 99.9% | 43m 12s unavailable |
| Order-create p95 | under 500 ms | 5% of requests may be slower |
| Workflow p95 | under 10 s | 5% of workflows may be slower |
| Workflow success | 99.0% | 1% eligible workflow failures |

The example SLA is 99.5% monthly API availability. SLAs are external commitments; SLOs guide engineering decisions; SLIs are the underlying measurements.

## Alerts

Provisioned starter alerts cover:

- API target unavailable for two minutes.
- Elevated server-error ratio.
- p95 API latency above the objective.
- Pending outbox backlog.
- Any dead-letter activity.

For production, replace single-window thresholds with multi-window, multi-burn-rate alerts. A fast burn catches severe incidents quickly; a slow burn catches sustained erosion without excessive paging.

## Investigation workflow

1. Start with the Grafana overview and identify the affected service or workflow.
2. Compare traffic, error, latency, and saturation changes.
3. Open an exemplar or matching trace around the incident window.
4. Pivot from trace ID or order ID to Loki logs.
5. Check outbox backlog and Kafka consumer lag.
6. Confirm recovery and note error-budget impact.

## Suggested exercises

- Stop the worker and observe retained events/backlog behavior.
- Increase `PAYMENT_FAILURE_RATE` and compare business failures with platform errors.
- Add artificial worker latency and watch workflow p95.
- Scale to three workers and inspect partition assignment.
- Stop Kafka, create orders, restore Kafka, and observe outbox recovery.
- Add recording rules and a Grafana SLO/error-budget dashboard.
