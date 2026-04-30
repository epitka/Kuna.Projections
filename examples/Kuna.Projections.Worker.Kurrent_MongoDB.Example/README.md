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

## Reseed From Scratch

```bash
docker compose down -v
docker compose up -d
./scripts/seed-kurrent-live.sh
```

## Script Overrides

- `seed-kurrent-live.sh`: `TARGET_EVENTS`, `MIN_COMPLETE_ORDERS`, `STREAM_PREFIX`, `KURRENT_CONNECTION_STRING`, `REPORT_PATH`
  Uses `Kuna.Examples.EventsSeeder` to scaffold order streams and write them to KurrentDB.

## Configuration

- The worker uses `ConnectionStrings:MongoDB` and `ConnectionStrings:KurrentDB` from `appsettings.json`.
- The MongoDB sink stores orders in `orders_order`, checkpoints in `projection_checkpoints`, and failures in `projection_failures`.
