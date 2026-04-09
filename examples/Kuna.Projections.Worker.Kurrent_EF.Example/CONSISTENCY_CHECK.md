# Orders Projection Consistency Check

This document explains how to run the on-demand replay consistency diagnostic for the
`orders` projection in `Kuna.Projections.Worker.Kurrent_EF.Example`.

The diagnostic compares:

- the current `Order` rows already persisted in PostgreSQL
- a fresh per-stream replay from KurrentDB using the same `OrdersProjection`

This lets you verify that the projection output stored in the database matches what
the projection would produce if each order stream were replayed from scratch.

## Prerequisites

1. Start the example infrastructure:

```bash
docker compose up -d
```

2. Seed events if needed:

```bash
./scripts/seed-kurrent-live.sh
```

3. Run the worker:

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.Kurrent_EF.Example.csproj
```

The diagnostic runs against the same KurrentDB and PostgreSQL connections already configured
for the worker in `appsettings.json`.

## Endpoint

The worker exposes an HTTP endpoint:

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

## Run The Full Check

This checks all `Order` rows currently in PostgreSQL.

```bash
curl -X POST http://localhost:5277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{}'
```

## Run A Single-Order Check

This is useful when you want to inspect a specific suspected mismatch.

```bash
curl -X POST http://localhost:5277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"00000000-0000-0000-0000-000000000000"}'
```

## Run A Limited Sample

This checks only the first `N` rows ordered by `Id`.

```bash
curl -X POST http://localhost:5277/diagnostics/orders/replay-consistency \
  -H 'Content-Type: application/json' \
  -d '{"limit":100}'
```

## Request Fields

- `orderId`
  Optional. If supplied, only that order is checked.

- `limit`
  Optional. Limits how many persisted orders are checked.

- `stopOnFirstMismatch`
  Optional. Defaults to `true`.
  When `true`, the diagnostic returns as soon as it finds the first mismatch.

- `logEvery`
  Optional. Defaults to `500`.
  Controls how often progress is written to the worker logs.

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

## Response Shape

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

## Worker Logs

While the diagnostic is running, the worker writes progress logs such as:

```text
Replay consistency diagnostics progress for OrdersProjection: checked=500/4002
```

At the end it logs:

```text
Replay consistency diagnostics completed for OrdersProjection: isConsistent=True, checked=4002/4002, elapsedMs=12000
```

If a mismatch is found, the worker also logs the first mismatched `orderId`.

## Notes

- This diagnostic is read-only. It does not modify the projection tables or Kurrent streams.
- The check is against the current contents of PostgreSQL at the moment you call it.
- If the worker is still actively processing new events, results can change while the diagnostic is running.
  For the strongest verification, run it after the projection has fully drained.
