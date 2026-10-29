# Development and Operations Guide

## Configuration

Copy `.env.example` to `.env`. The defaults are safe only for a local workstation.

Important settings:

| Variable | Meaning |
|---|---|
| `*_PORT` | Localhost-only published ports; change them when another tool owns a default |
| `POSTGRES_*` | Local database name and credentials |
| `KAFKA_CLUSTER_ID` | Stable KRaft cluster ID for the local volume |
| `GRAFANA_ADMIN_*` | Local Grafana login |
| `PAYMENT_FAILURE_RATE` | Simulated rejection probability from `0` to `1` |
| `LOG_LEVEL` | Application structured-log level |

## Start and verify

```bash
cp .env.example .env
make config
make up
make status
make smoke
```

The first start can take several minutes while images are downloaded and built. Kafka topic initialization and PostgreSQL schema initialization are one-shot containers and should exit successfully.

The API and worker target .NET 10. Install a .NET 10 SDK to build them outside Docker; `global.json` accepts the latest installed .NET 10 feature band. The frontend uses Next.js 16 and requires Node.js 20.9 or newer. Docker supplies both runtimes.

Build or run the frontend locally:

```bash
cd apps/frontend
npm ci
API_INTERNAL_URL=http://127.0.0.1:3008 npm run dev
```

The production frontend image uses Next.js standalone output and proxies browser `/api/*` calls to `API_INTERNAL_URL` at runtime.

## Build the .NET backend

```bash
dotnet restore OrderFlow.sln
dotnet build OrderFlow.sln -c Release --no-restore
```

Run services directly while PostgreSQL, Kafka, and the collector are available. Set the OTLP endpoint to the collector's published HTTP port (`4328` by default):

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4328
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
dotnet run --project apps/api
dotnet run --project apps/worker
```

The environment-variable contract is the same in Docker and local execution. The application defaults connect to PostgreSQL on `5438` and Kafka on `29098`; Compose injects the internal container addresses.

## Debugging commands

```bash
docker compose logs -f api worker
docker compose logs kafka kafka-init
docker compose exec postgres psql -U orderflow -d orderflow
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group orderflow-worker --describe
dotnet-counters monitor --name OrderFlow.Api
dotnet-trace collect --name OrderFlow.Worker
```

## Resetting local data

This removes the project containers and Docker volumes, including local orders, Kafka records, and Grafana state:

```bash
make clean
```

Then run `make up` to rebuild a fresh environment.

## Changing database schema

Files under `database/init` run only when PostgreSQL creates an empty data volume. During early development, add a numbered SQL file and recreate the local volume. When the application matures, introduce a dedicated migration tool instead of relying on initialization scripts.

## Adding Kafka events

1. Document the event and ownership in `docs/architecture.md`.
2. Add the topic to `docker/kafka/create-topics.sh`.
3. Use a versioned envelope and retain backward compatibility.
4. Add producer and consumer metrics.
5. Add duplicate-delivery and invalid-payload tests.
6. Define retry and DLQ behavior before deployment.

## Production gaps

Before deploying outside a local learning environment:

- Use managed or clustered PostgreSQL and Kafka.
- Move secrets to a secrets manager and rotate them.
- Enable authentication, authorization, encryption, and network controls.
- Build immutable versioned images in CI and scan/sign them.
- Use a real migration workflow and tested backups.
- Add Kafka/PostgreSQL/container exporters and capacity alerts.
- Define ownership, escalation paths, runbooks, and an incident process.
- Load-test SLOs and size retention/resource limits.
