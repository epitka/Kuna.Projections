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
