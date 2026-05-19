# Kuna.Projections.Worker.Kurrent_MongoDB.Example

This example runs independently with:

- KurrentDB, seeded live with generated events
- MongoDB, used as the projection store

## Start Infrastructure

```bash
docker compose up -d
```

## Seed KurrentDB With Generated Events

```bash
./scripts/seed-kurrent-live.sh
```

The script runs the shared seeder project at `examples/Kuna.Examples.EventsSeeder`.

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.Kurrent_MongoDB.Example.csproj
```

The worker exposes:

```text
GET /
```

Default local URL:

```text
http://localhost:5277/
```

## Restart Projection State Without Reseeding Events

```bash
./scripts/reset-projection-state.sh
```

## Reseed From Scratch

```bash
docker compose down -v
docker compose up -d
./scripts/seed-kurrent-live.sh
```

## Script Overrides

- `seed-kurrent-live.sh`: `TARGET_EVENTS`, `MIN_COMPLETE_ORDERS`, `STREAM_PREFIX`, `KURRENT_CONNECTION_STRING`, `REPORT_PATH`
  Uses `Kuna.Examples.EventsSeeder` to scaffold order streams and write them to KurrentDB.
- `reset-projection-state.sh`: `MONGODB_CONTAINER_NAME`, `MONGODB_DATABASE`, `ORDERS_COLLECTION_NAME`, `CHECKPOINTS_COLLECTION_NAME`
  Clears Mongo projection state and checkpoints without touching Kurrent events.

## Configuration

- The worker uses `ConnectionStrings:MongoDB` and `ConnectionStrings:KurrentDB` from `appsettings.json`.
- The MongoDB sink stores orders in `orders_order`.
- Checkpoints remain in `projection_checkpoints`.
- Projection failures are stored in `projection_failures`.
