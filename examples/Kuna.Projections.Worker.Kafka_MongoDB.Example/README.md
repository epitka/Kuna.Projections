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

## Inspect Persisted Orders In MongoDB

Connect with the local Mongo shell:

```bash
mongosh mongodb://localhost:27017/orders_projection
```

List the collections:

```javascript
show collections
```

Read a few projected orders:

```javascript
db.orders_order.find().limit(3).pretty()
```

Check the checkpoint document:

```javascript
db.projection_checkpoints.find().pretty()
```

Check persisted failures:

```javascript
db.projection_failures.find().pretty()
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

For the Kafka example, the comparison intentionally ignores `GlobalEventPosition`.
That value is a Kafka transport checkpoint over all assigned partitions, so replaying one order in isolation does not reproduce the exact persisted checkpoint position from the original projection run.

## Restart Projection State Without Reseeding Events

```bash
./scripts/reset-projection-state.sh
```

## Reseed From Scratch

```bash
docker compose down -v
docker compose up -d
./scripts/seed-kafka-live.sh
```

## Script Overrides

- `seed-kafka-live.sh`: `TARGET_EVENTS`, `MIN_COMPLETE_ORDERS`, `STREAM_PREFIX`, `KAFKA_TOPIC`, `KAFKA_PARTITIONS`, `KAFKA_BOOTSTRAP_SERVERS`, `REPORT_PATH`
  Directly seeds Kafka using the shared seeder.
  Defaults to `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092`.
- `reset-projection-state.sh`: `MONGODB_CONTAINER_NAME`, `MONGODB_DATABASE`, `ORDERS_COLLECTION_NAME`, `CHECKPOINTS_COLLECTION_NAME`

## Configuration

- The worker uses `ConnectionStrings:MongoDB` and `OrdersProjection:Kafka:BootstrapServers` from `appsettings.json`.
- The MongoDB sink stores orders in `orders_order`.
- Checkpoints remain in `projection_checkpoints`.
- Projection failures are stored in `projection_failures`.
