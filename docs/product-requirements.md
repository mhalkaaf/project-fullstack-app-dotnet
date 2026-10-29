# OrderFlow Product Requirements Document

## 1. Product summary

OrderFlow is an educational event-driven order-processing platform. It gives a DevOps engineer a compact system in which to practise frontend and backend development, relational transactions, Kafka delivery semantics, container operations, and observability.

The application models a miniature commerce workflow: a customer selects products, submits an order, and watches it progress through inventory reservation, simulated payment, and completion.

## 2. Goals

- Demonstrate a complete browser-to-database request path.
- Process business operations asynchronously using Kafka.
- Make duplicate delivery, retries, and failures visible and safe.
- Correlate metrics, traces, and logs across services.
- Define measurable SLIs and operate against explicit SLOs.
- Run the entire learning environment with Docker Compose.

## 3. Non-goals

- Real payment processing or storage of payment credentials.
- Production-grade authentication, authorization, or multi-tenancy.
- A highly available Kafka or PostgreSQL deployment.
- Kubernetes deployment in the first milestone.
- Financially binding SLA enforcement.

## 4. Personas

### Customer

- Browses available products.
- Submits an order.
- Checks the current state and event timeline.

### Operator

- Monitors availability, latency, traffic, saturation, and Kafka lag.
- Finds failed workflows using an order ID, event ID, or trace ID.
- Inspects dead-lettered events and retries eligible failures.
- Injects controlled payment failures for learning exercises.

## 5. User stories

### Catalog and ordering

- As a customer, I can see products, prices, and available quantities.
- As a customer, I can add one or more products to an order.
- As a customer, I receive an order ID immediately after a valid submission.
- As a customer, I can see every state transition in chronological order.
- As a customer, I cannot request zero items or more stock than is available.

### Operations

- As an operator, I can see request rate, errors, and latency.
- As an operator, I can see order-processing duration and outcomes.
- As an operator, I can see Kafka consumer lag, retries, and dead letters.
- As an operator, I can search logs using the order or trace ID.
- As an operator, I can open a trace spanning HTTP, PostgreSQL, Kafka, and worker processing.

## 6. Order state model

Happy path:

```text
PENDING -> INVENTORY_RESERVED -> PAYMENT_APPROVED -> COMPLETED
```

Failure paths:

```text
PENDING -> INVENTORY_REJECTED -> FAILED
INVENTORY_RESERVED -> PAYMENT_REJECTED -> FAILED
```

Every transition creates an immutable `order_events` record. The mutable `orders.status` field is a convenient current-state projection.

## 7. Functional requirements

### Frontend

- Product catalog and quantity selection.
- Customer name and order submission.
- Order summary, current status, and event timeline.
- Clear empty, loading, validation, and failure states.
- Polling for updates in milestone one; Server-Sent Events are optional later.

### API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/products` | List products and inventory |
| `POST` | `/api/orders` | Create an order and its outbox event |
| `GET` | `/api/orders` | List recent orders |
| `GET` | `/api/orders/{id}` | Return an order and timeline |
| `GET` | `/health/live` | Process liveness |
| `GET` | `/health/ready` | PostgreSQL and Kafka readiness |

The API and worker export application/runtime metrics to the OpenTelemetry Collector over OTLP; Prometheus scrapes the collector's translated endpoint.

Future operator endpoints:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/operations/failed-events` | List dead-letter records |
| `POST` | `/api/operations/failed-events/{id}/retry` | Republish an eligible event |

### Event processing

- Creating an order and its first outbox record must be one PostgreSQL transaction.
- The publisher sends unpublished outbox rows to Kafka and marks them published only after broker acknowledgement.
- Consumers must be idempotent by `(event_id, consumer_name)`.
- `order_id` is the Kafka key so one order stays ordered within a partition.
- A transient handler failure is retried with backoff.
- An exhausted or invalid message is written to `order-processing.dlq`.
- Events carry `eventId`, `eventType`, `eventVersion`, `occurredAt`, `traceId`, and `data`.

## 8. Data requirements

Core entities:

- `products`: catalog data and available quantity.
- `orders`: current state, customer, total, and timestamps.
- `order_items`: immutable quantities and purchase-time prices.
- `order_events`: auditable state-transition timeline.
- `outbox_events`: database-backed publish queue.
- `processed_events`: consumer idempotency ledger.
- `dead_letter_events`: failed payload and diagnostic metadata.

Money is stored as fixed-precision decimal, never floating point. IDs are UUIDs. All timestamps are UTC.

## 9. Non-functional requirements

- Every container has a useful health check where the image permits it.
- Services handle termination signals and close broker/database connections.
- Configuration and secrets are supplied through environment variables.
- Application logs are structured JSON and exclude credentials.
- High-cardinality identifiers do not become metric labels.
- Database migrations are repeatable and source-controlled.
- A fresh checkout starts with `docker compose up --build`.
- A duplicate Kafka delivery causes no duplicate stock or payment side effect.

## 10. SLI, SLO, and SLA requirements

The initial service objectives are:

| Concern | SLI | SLO over rolling 30 days |
|---|---|---|
| API availability | successful eligible requests / all eligible requests | 99.9% |
| Order API latency | `POST /api/orders` duration | 95% below 500 ms |
| Workflow latency | create time to terminal state | 95% below 10 seconds |
| Processing success | completed eligible orders / eligible orders | 99.0% |
| Duplicate safety | workflows without duplicate side effects | 99.99% |
| Event freshness | age/lag of the oldest unprocessed event | 99% below 5 seconds |

The example customer-facing SLA is 99.5% monthly order-creation API availability. It is educational only. The stricter internal SLO creates an operating margin.

At 99.9% availability, the approximate 30-day error budget is 43 minutes and 12 seconds.

## 11. Acceptance criteria

Milestone one is complete when:

- Docker Compose starts all containers from a fresh checkout.
- The UI creates a valid order and shows its eventual terminal state.
- PostgreSQL contains the order, items, outbox records, and timeline.
- Kafka UI shows each lifecycle topic.
- Duplicate event processing is harmless.
- A configured payment failure produces a failed order.
- Prometheus scrapes the API, worker, and OpenTelemetry Collector.
- Grafana opens with provisioned Prometheus, Tempo, and Loki sources.
- A trace spans API creation and asynchronous processing where propagation permits.
- Structured application logs are searchable in Loki.
- Alert rules cover API availability, latency, Kafka lag, and dead letters.

Milestone two adds authenticated operator tools, controlled retry from the DLQ, load generation, burn-rate recording rules, and deployment to Kubernetes.

## 12. Delivery phases

1. Synchronous catalog, order API, schema, and browser workflow.
2. Kafka lifecycle topics, consumer groups, and asynchronous state changes.
3. Transactional outbox, idempotency, retry, and dead-letter handling.
4. OpenTelemetry traces and application/business metrics.
5. Grafana dashboards, SLO reporting, and actionable alerts.
6. Load tests, failure exercises, consumer scaling, and runbooks.
