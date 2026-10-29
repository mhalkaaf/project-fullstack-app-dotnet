# Architecture and Event Flow

## System context

```text
Customer browser
      |
      v
Nginx frontend -- /api proxy --> Order API ---> PostgreSQL
                                      |
                                      | transactional outbox publisher
                                      v
                                    Kafka
                                      |
                                      v
                                  Order worker
                                      |
                                      +----------> PostgreSQL

API + worker telemetry ---> OpenTelemetry Collector ---> Tempo
API + worker metrics -------------------------------> Prometheus
Container stdout ----------> Grafana Alloy ----------> Loki
Prometheus + Tempo + Loki --------------------------> Grafana
```

## Runtime components

| Component | Responsibility |
|---|---|
| Frontend | Catalog, order submission, timeline polling, API reverse proxy |
| API (.NET 10) | ASP.NET Core validation/queries, transactional order creation, outbox publishing |
| Worker (.NET 10) | `BackgroundService` Kafka consumption, inventory/payment workflow, idempotency |
| PostgreSQL | Source of truth and transactional outbox |
| Kafka | Durable lifecycle-event distribution |
| OTel Collector | Receives OTLP telemetry and forwards traces |
| Prometheus | Scrapes and evaluates metrics/alerts |
| Tempo | Stores distributed traces |
| Loki | Stores structured container logs |
| Alloy | Discovers Docker containers and ships their logs |
| Grafana | Correlates metrics, traces, and logs |

## Create-order sequence

```text
Browser        API             PostgreSQL       Kafka          Worker
   |            |                   |              |              |
   | POST order |                   |              |              |
   |----------->| BEGIN             |              |              |
   |            | insert order      |              |              |
   |            | insert items      |              |              |
   |            | insert outbox     |              |              |
   |            | COMMIT ---------->|              |              |
   | 202 + id   |                   |              |              |
   |<-----------|                   |              |              |
   |            | read outbox       |              |              |
   |            | publish ------------------------>|              |
   |            | mark published   |              |              |
   |            |                   |              | consume      |
   |            |                   |              |------------->|
   |            |                   | reserve stock, record event |
   |            |                   |<-----------------------------|
```

Later transitions repeat the outbox/publish/consume pattern until the order reaches `COMPLETED` or `FAILED`.

## Kafka topology

| Topic | Key | Consumer group | Meaning |
|---|---|---|---|
| `orders.created` | order ID | `orderflow-worker` | Start inventory processing |
| `inventory.reserved` | order ID | `orderflow-worker` | Start simulated payment |
| `inventory.rejected` | order ID | `orderflow-worker` | Record inventory failure |
| `payments.approved` | order ID | `orderflow-worker` | Complete the order |
| `payments.rejected` | order ID | `orderflow-worker` | Record payment failure |
| `orders.completed` | order ID | future notification/analytics groups | Terminal success |
| `orders.failed` | order ID | future notification/analytics groups | Terminal failure |
| `order-processing.dlq` | order ID | operator tooling | Exhausted/invalid events |

The local environment uses three partitions per business topic to make partition assignment visible when multiple workers are started:

```bash
docker compose up -d --scale worker=3
```

Ordering is guaranteed only inside a partition. Using the order ID as key ensures transitions for one order share a partition.

## Delivery guarantees

The design targets at-least-once delivery:

1. Database changes and an outbox row commit together.
2. API and worker both run an outbox publisher. `FOR UPDATE SKIP LOCKED` lets multiple instances share work without normally selecting the same row.
3. A publisher may still send the same row more than once if it crashes after Kafka acknowledgement but before committing `published_at`.
4. The worker inserts a `processed_events` claim before applying business changes in the same database transaction.
5. It commits the Kafka offset only after the database transaction commits. A redelivery therefore becomes a no-op.

Exactly-once language should be avoided unless the entire business side effect—not only Kafka reads and writes—is covered.

## Failure behavior

- PostgreSQL unavailable: API readiness fails and order writes return `503`.
- Kafka unavailable: existing API reads continue; outbox rows remain pending for later publication.
- Worker unavailable: Kafka retains events and consumer lag grows.
- Invalid inventory: order transitions to `FAILED` through an `inventory.rejected` event.
- Simulated payment rejection: order transitions to `FAILED` through a `payments.rejected` event.
- Poison message: processing attempts are tracked and the payload is copied to the DLQ after the configured limit.

## Security boundaries

The Compose environment binds learning interfaces to localhost. Default credentials are intentionally local-only and must never be reused in a shared environment. A production evolution would add TLS, a secrets manager, Kafka authentication/authorization, API identity, least-privilege database roles, image scanning, and network policies.

## Scaling evolution

- Scale stateless API containers behind a reverse proxy.
- Scale workers up to the number of partitions for useful parallelism.
- Split the single worker into inventory, payment, notification, and analytics consumer groups only when the operational learning benefit justifies it.
- Replace the single-node PostgreSQL/Kafka topology before treating the project as highly available.
