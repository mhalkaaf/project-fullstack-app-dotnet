# OrderFlow

OrderFlow is a small, event-driven order-processing application for learning full-stack development and production operations. It combines a browser UI, an HTTP API, PostgreSQL, Kafka workers, Docker, and an OpenTelemetry/Grafana observability stack.

## What is included

- Static frontend served by Nginx
- Node.js API and background worker
- PostgreSQL schema and seed products
- Kafka running in single-node KRaft mode
- Transactional outbox and idempotent consumer foundations
- OpenTelemetry Collector, Prometheus, Grafana, Tempo, Loki, and Alloy
- Provisioned Grafana data sources, dashboard, and Prometheus alerts
- Product, architecture, operations, and observability documentation

## Quick start

Requirements: Docker Engine with Docker Compose v2 and at least 6 GB of memory available to Docker.

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

Open:

- Application: <http://localhost:8088>
- API health: <http://127.0.0.1:3008/health/ready>
- Grafana: <http://localhost:3009> (`admin` / the value of `GRAFANA_ADMIN_PASSWORD`)
- Prometheus: <http://localhost:9098>
- Kafka UI: <http://localhost:8089>

Create an order from the frontend, then inspect its trace, application metrics, Kafka events, and structured logs.

Stop the environment while keeping data:

```bash
docker compose down
```

Remove the environment and its local Docker volumes:

```bash
docker compose down --volumes
```

## Useful commands

```bash
make help
make up
make status
make logs
make smoke
make config
make down
```

## Repository map

```text
apps/
  api/                 HTTP API and outbox publisher
  frontend/            Browser UI and Nginx configuration
  worker/              Kafka consumer and order workflow
database/              PostgreSQL initialization
docker/kafka/          Kafka topic initialization
docs/                  Product and engineering documentation
observability/         Collector, metrics, traces, logs, dashboards, alerts
compose.yaml           Complete local environment
```

## Documentation

- [Product requirements](docs/product-requirements.md)
- [Architecture and event flow](docs/architecture.md)
- [Observability, SLI, SLO, and SLA](docs/observability.md)
- [Development and operations guide](docs/development.md)

This environment is deliberately educational. The single-node databases and brokers are not a production deployment topology.

Published host ports are configurable in `.env`; internal container ports remain unchanged.
