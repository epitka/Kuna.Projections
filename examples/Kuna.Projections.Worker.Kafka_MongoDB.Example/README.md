# Kuna.Projections.Worker.Kafka_MongoDB.Example

This example runs a projection worker that:

- consumes events from Kafka
- expects those Kafka records to have the KurrentDB Kafka Sink record shape
- projects into MongoDB using the existing MongoDB sink

This example is intended for the flow:

- `KurrentDB -> Kafka -> Kuna.Projections -> MongoDB`

## Start Infrastructure

```bash
docker compose up -d
```

## Seed KurrentDB And Export To Kafka

```bash
./scripts/seed-kurrent-live.sh
```

This script now performs the full upstream demo setup:

- ensures the KurrentDB Kafka Sink connector is configured
- ensures the Kafka topic exists
- writes generated order events into KurrentDB using the shared seeder
- relies on KurrentDB to export those events into Kafka

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.Kafka_MongoDB.Example.csproj
```

## Important Constraint

This worker assumes Kafka ordering is safe for projection replay.
For that to be true, KurrentDB's Kafka Sink must be configured so that all events for one model are published to the same partition.

The source transformer can adapt Kurrent's exported record shape.
It cannot repair incorrect partitioning after the fact.

## Example Kurrent Kafka Sink Notes

This worker uses:

- `OrdersProjection:Kafka:Transformer = "Kurrent"`
- `OrdersProjection:Kafka:Topic = "orders-events"`

That means the Kafka topic should contain records exported from KurrentDB's Kafka Sink connector, not the repository's native Kafka event format.

If you choose to set `OrdersProjection:Kafka:Partitions`, the configured partition ids must already exist on the topic.
The source validates that at startup and the Kafka health check reports missing configured partitions as unhealthy.

## Script Overrides

- `seed-kurrent-live.sh`: `TARGET_EVENTS`, `MIN_COMPLETE_ORDERS`, `STREAM_PREFIX`, `KURRENT_CONNECTION_STRING`, `REPORT_PATH`
  Also configures the KurrentDB Kafka Sink connector before seeding.
- `configure-kurrent-kafka-sink.sh`: `KURRENT_BASE_URL`, `KURRENT_USERNAME`, `KURRENT_PASSWORD`, `CONNECTOR_ID`, `KAFKA_TOPIC`, `KAFKA_PARTITIONS`, `KAFKA_BOOTSTRAP_SERVERS`, `STREAM_PREFIX`
- `reset-projection-state.sh`: `MONGODB_CONTAINER_NAME`, `MONGODB_DATABASE`, `ORDERS_COLLECTION_NAME`, `CHECKPOINTS_COLLECTION_NAME`
