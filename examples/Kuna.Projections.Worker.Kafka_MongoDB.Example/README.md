# Kuna.Projections.Worker.Kafka_MongoDB.Example

This example runs a projection worker that:

- consumes events from Kafka
- expects native Kafka projection records produced by the shared seeder
- projects into MongoDB using the existing MongoDB sink

This example is intended for the flow:

- `Seeder -> Kafka -> Kuna.Projections -> MongoDB`

## Start Infrastructure

```bash
docker compose up -d
```

## Seed Kafka Directly

```bash
./scripts/seed-kafka-live.sh
```

This script now performs the full upstream demo setup:

- ensures the Kafka topic exists
- writes generated order events directly into Kafka using the shared seeder

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.Kafka_MongoDB.Example.csproj
```

The worker exposes:

```text
GET /
GET /health
GET /diagnostics/orders/status
POST /diagnostics/orders/replay-consistency
```

Default local URL:

```text
http://localhost:5067/
```

## Important Constraint

This worker assumes Kafka ordering is safe for projection replay.
For that to be true, the producer must ensure all events for one model are published to the same partition.

The shared seeder writes Kafka records keyed by model id, so Kafka preserves per-order partitioning.

This worker uses:

- `OrdersProjection:Kafka:Transformer = "Native"`
- `OrdersProjection:Kafka:Topic = "orders-events"`

That means the Kafka topic should contain the repository's native Kafka projection record format.

If you choose to set `OrdersProjection:Kafka:Partitions`, the configured partition ids must already exist on the topic.
The source validates that at startup and the Kafka health check reports missing configured partitions as unhealthy.

## Check Runtime Status

Read the current Kafka projection status:

```bash
curl http://localhost:5067/diagnostics/orders/status | jq
```

This reports:

- topic and projection instance id
- persisted checkpoint
- MongoDB order count
- whether the projection is caught up
- total lag
- per-partition offsets and lag

Health endpoint:

```bash
curl http://localhost:5067/health
```

## Run Replay Consistency Check

Check a small sample first:

```bash
curl -X POST http://localhost:5067/diagnostics/orders/replay-consistency \
  -H "Content-Type: application/json" \
  -d '{"limit": 50, "stopOnFirstMismatch": true, "logEvery": 10}' | jq
```

Check one specific order:

```bash
curl -X POST http://localhost:5067/diagnostics/orders/replay-consistency \
  -H "Content-Type: application/json" \
  -d '{"orderId":"3e893947-5882-4d40-a684-946dd270f8c2"}' | jq
```

The replay consistency endpoint:

- replays matching orders directly from Kafka
- rebuilds projection state in memory
- compares the replayed snapshot with the MongoDB document
- returns mismatch details when a divergence is found

## Script Overrides

- `seed-kafka-live.sh`: `TARGET_EVENTS`, `MIN_COMPLETE_ORDERS`, `STREAM_PREFIX`, `KAFKA_TOPIC`, `KAFKA_PARTITIONS`, `KAFKA_BOOTSTRAP_SERVERS`, `REPORT_PATH`
  Directly seeds Kafka using the shared seeder.
  Defaults to `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092`.
- `reset-projection-state.sh`: `MONGODB_CONTAINER_NAME`, `MONGODB_DATABASE`, `ORDERS_COLLECTION_NAME`, `CHECKPOINTS_COLLECTION_NAME`
