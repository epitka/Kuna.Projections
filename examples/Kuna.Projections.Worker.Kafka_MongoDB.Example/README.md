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
http://localhost:5284/
```

## Inspect Persisted Orders In MongoDB

Connect with the local Mongo shell:

```bash
mongosh mongodb://localhost:27017/orders_projection
```

List the collections:

```text
show collections
```

Read a few projected orders:

```text
db.orders_order.find().limit(3).pretty()
```

Check the checkpoint document:

```text
db.projection_checkpoints.find().pretty()
```

Check persisted failures:

```text
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
curl http://localhost:5284/diagnostics/orders/status | jq
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
curl http://localhost:5284/health
```

## Run Replay Consistency Check

Check a small sample first:

```bash
curl -X POST http://localhost:5284/diagnostics/orders/replay-consistency \
  -H "Content-Type: application/json" \
  -d '{"limit": 50, "stopOnFirstMismatch": true, "logEvery": 10}' | jq
```

Check one specific order:

```bash
curl -X POST http://localhost:5284/diagnostics/orders/replay-consistency \
  -H "Content-Type: application/json" \
  -d '{"orderId":"3e893947-5882-4d40-a684-946dd270f8c2"}' | jq
```

The replay consistency endpoint:

- scans Kafka sequentially in batches
- rebuilds matching order state in memory
- compares touched orders with MongoDB and evicts matched orders from the temporary replay cache
- returns mismatch details when a divergence is found

This endpoint is diagnostic work, not the normal projection path.
It is intentionally heavier than `/diagnostics/orders/status`, especially on large topics.

### How Kafka Consistency Works

Kafka does not provide direct per-order stream replay in the same way KurrentDB does.
Because of that, this diagnostic does not ask Kafka for one order at a time.
Instead it uses the shape Kafka is good at:

- assign all configured partitions
- read the topic sequentially from the earliest offset
- rebuild in-memory order state for matching records
- compare those rebuilt orders against MongoDB in batches

That is why the Kafka consistency check is designed as one topic scan rather than many per-order scans.
It is much cheaper and it preserves the real transport order seen by the projection worker.

### Why Matched Orders Can Be Evicted

During the scan, the diagnostic keeps an in-memory cache of active replayed orders.
When a replayed order matches the persisted MongoDB document, that order is evicted from the temporary cache.

That is safe for this projection because later events for the same order would change the persisted MongoDB document too.
If the MongoDB document already matches the replayed state at the current point in the Kafka scan, then the diagnostic has already observed all events needed to explain that persisted row.

In other words:

- if more events for that order still existed later in Kafka, the MongoDB row would not yet match the replayed intermediate state
- because it does match, the diagnostic can drop that order from memory and continue scanning

This assumption is valid for the Orders projection because it is a monotonic event-driven model:

- the order document is derived entirely from the order's event history
- later valid events mutate the persisted document
- there is no separate side channel updating MongoDB behind the projection

### What To Watch For

The consistency check is trustworthy only if the Kafka topic follows the same ordering assumptions as the main projection worker:

- all events for one order must use the same Kafka key
- that key must keep one order on one partition
- per-order ordering must come from Kafka partition order, not timestamps

Things that would break or weaken the diagnostic:

- producers publishing one order's events with different keys
- repartitioning that moves one order across partitions
- out-of-band writes that mutate MongoDB without corresponding Kafka events
- topic retention deleting old events before the diagnostic can replay them

For the Kafka example, the comparison intentionally ignores `GlobalEventPosition`.
That value is a whole-topic Kafka checkpoint across assigned partitions, so replaying one order or evicting matched orders during a diagnostic run does not reproduce the exact persisted checkpoint value from the original projection run.

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

- The worker uses `ConnectionStrings:MongoDB` and `ConnectionStrings:Kafka` for broker/store endpoints.
- Projection-specific Kafka source options live under `OrdersProjection:Kafka`, such as `Topic`, `Partitions`, `Transformer`, and `PollTimeoutMs`.
- The MongoDB sink stores orders in `orders_order`.
- Checkpoints remain in `projection_checkpoints`.
- Projection failures are stored in `projection_failures`.
