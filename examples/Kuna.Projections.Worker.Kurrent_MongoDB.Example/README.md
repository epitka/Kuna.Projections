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
POST /diagnostics/orders/replay-consistency
```

Default local URL:

```text
http://localhost:5279/
```

## Run The Live Consistency Flow

With Docker, .NET, `curl`, and `jq` installed:

```bash
./scripts/run-live-consistency-flow.sh
```

The script resets the compose volumes, starts the infrastructure, seeds 10,000 events,
starts the worker, waits for the `Projection pipeline fully drained` message, seeds
another 5,000 events, and waits until the live projection is fully drained and the
replay consistency result reports no mismatch.

Override the event counts or timeouts with `INITIAL_EVENTS`, `SECOND_EVENTS`,
`STARTUP_TIMEOUT_SECONDS`, and `DRAIN_TIMEOUT_SECONDS`.

The repository's `Example Consistency` CI workflow runs this flow for every pull
request, merge queue entry, and push to `master`.

## Run Replay Consistency Check

The replay consistency diagnostic compares:

- the current `Order` documents already persisted in MongoDB
- a fresh per-stream replay from KurrentDB using the same `OrdersProjection`

Run the full check:

```bash
curl -X POST http://localhost:5279/diagnostics/orders/replay-consistency \
  -H "Content-Type: application/json" \
  -d '{}'
```

Run a single-order check:

```bash
curl -X POST http://localhost:5279/diagnostics/orders/replay-consistency \
  -H "Content-Type: application/json" \
  -d '{"orderId":"00000000-0000-0000-0000-000000000000"}'
```

Request fields:

- `orderId`
  Optional. If omitted, every persisted order is checked. If supplied, only that order is checked.
- `stopOnFirstMismatch`
  Optional. Defaults to `true`. When `true`, the diagnostic returns as soon as it finds the first mismatch.
- `logEvery`
  Optional. Defaults to `500`. Controls how often progress is written to the worker logs.

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
