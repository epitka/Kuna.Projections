# Kuna.Projections.Worker.Kurrent_EF.Example

This example runs independently with:

- KurrentDB, seeded live with generated events
- PostgreSQL, used as the projection store

## Start Infrastructure

```bash
docker compose up -d
```

## Seed KurrentDB With Generated Events

```bash
./scripts/seed-kurrent-live.sh
```

The script now runs the shared seeder project at `examples/Kuna.Examples.EventsSeeder`.

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.Kurrent_EF.Example.csproj
```

## Run The On-Demand Replay Consistency Check

The replay consistency diagnostic compares:

- the current `Order` rows already persisted in PostgreSQL
- a fresh per-stream replay from KurrentDB using the same `OrdersProjection`

This lets you verify that the projection output stored in the database matches what
the projection would produce if each order stream were replayed from scratch.

Before running the diagnostic, start the infrastructure, seed events if needed, and run the worker.
The diagnostic uses the same KurrentDB and PostgreSQL connections already configured in `appsettings.json`.

The worker exposes:

```text
POST /diagnostics/orders/replay-consistency
```

Default local URL:

```text
http://localhost:5277/diagnostics/orders/replay-consistency
```

Port note:

- `http://localhost:5277` is the plain HTTP endpoint
- `https://localhost:7277` is the HTTPS endpoint
- if you call the HTTPS endpoint locally with `curl`, use `https://` and usually `-k`

Example HTTPS call:

```bash
curl -k -X POST https://localhost:7277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{"limit":1}'
```

Run the full check:

```bash
curl -X POST http://localhost:5277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{}'
```

Run a single-order check:

```bash
curl -X POST http://localhost:5277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"00000000-0000-0000-0000-000000000000"}'
```

Run a limited sample:

```bash
curl -X POST http://localhost:5277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{"limit":100}'
```

Request fields:

- `orderId`
  Optional. If supplied, only that order is checked.
- `limit`
  Optional. Limits how many persisted orders are checked.
- `stopOnFirstMismatch`
  Optional. Defaults to `true`. When `true`, the diagnostic returns as soon as it finds the first mismatch.
- `logEvery`
  Optional. Defaults to `500`. Controls how often progress is written to the worker logs.

Example:

```bash
curl -X POST http://localhost:5277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{
    "limit": 1000,
    "stopOnFirstMismatch": false,
    "logEvery": 100
  }'
```

Successful responses return HTTP `200 OK` and a JSON body like:

```json
{
  "isConsistent": true,
  "totalOrders": 4002,
  "checkedOrders": 4002,
  "startedAtUtc": "2026-04-09T11:00:00.0000000+00:00",
  "completedAtUtc": "2026-04-09T11:00:12.0000000+00:00",
  "elapsedMilliseconds": 12000.0,
  "mismatch": null
}
```

If a mismatch is found, `mismatch` contains:

- `orderId`
- `reason`
- `databaseSnapshotJson`
- `replaySnapshotJson`

Those two snapshot JSON payloads are intended for direct side-by-side comparison.

While the diagnostic is running, the worker writes progress logs such as:

```text
Replay consistency diagnostics progress for OrdersProjection: checked=500/4002
```

At the end it logs:

```text
Replay consistency diagnostics completed for OrdersProjection: isConsistent=True, checked=4002/4002, elapsedMs=12000
```

If a mismatch is found, the worker also logs the first mismatched `orderId`.

Notes:

- This diagnostic is read-only. It does not modify the projection tables or Kurrent streams.
- The check is against the current contents of PostgreSQL at the moment you call it.
- If the worker is still actively processing new events, results can change while the diagnostic is running.
  For the strongest verification, run it after the projection has fully drained.

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
- `reset-projection-state.sh`: `POSTGRES_CONTAINER_NAME`, `POSTGRES_USER`, `POSTGRES_DB`, `POSTGRES_SCHEMA`

## Configuration

- The worker uses `ConnectionStrings:PostgreSql` and `ConnectionStrings:KurrentDB` from `appsettings.json`.
- `OrdersProjection:InstanceId` is the deployment identity. Change it when the projection logic changes and you want the new deployment to replay from the beginning in parallel before switching reads over.
