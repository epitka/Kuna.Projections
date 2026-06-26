# Kuna.Projections.Worker.EventSourcingDB_Postgres.Example

A runnable reference worker that projects events from [EventSourcingDB](https://www.eventsourcingdb.io/) into PostgreSQL.

It uses:

- EventSourcingDB as the event source
- PostgreSQL as the projection store (EF Core via `Kuna.Projections.Sink.EF.Npgsql`)
- the shared `OrdersProjection` from `examples/Kuna.Examples.Projections`

For the source-specific semantics (global ordering, version strategy, caught-up signaling, model id resolution), see [docs/eventsourcingdb-source.md](../../docs/eventsourcingdb-source.md).

## Start Infrastructure

```bash
docker compose up -d
```

This starts EventSourcingDB on `http://localhost:3000` (API token `secret`) and PostgreSQL on `localhost:5432`.

The EventSourcingDB service ships a healthcheck against its `/api/v1/ping` endpoint, so you can use `docker compose up -d --wait` to block until it is actually accepting connections. The seeder also waits for readiness on its own, so seeding right after `up` is safe either way.

## Seed Events

This example reuses the shared `Kuna.Examples.EventsSeeder` (the same generator that backs the Kurrent and Kafka examples). It writes generated order events to EventSourcingDB under the `/orders` subject root, one subject per order (`/orders/{guid}`), with the CloudEvent `type` set to `io.kuna.orders.<EventName>`.

```bash
./scripts/seed-eventsourcingdb-live.sh
```

Environment variables override the defaults: `TARGET_EVENTS` (default `50000`), `MIN_COMPLETE_ORDERS` (default `3000`), `ESDB_BASE_URL` (default `http://localhost:3000`), `ESDB_API_TOKEN` (default `secret`), and `ESDB_BATCH_SIZE` (default `500`, the number of events per `WriteEvents` call).

> Note: EventSourcingDB writes are considerably slower than reads, so the time the seeder takes is not a measure of projection (read) throughput. `ESDB_BATCH_SIZE` only affects how fast the seeder fills the store; it does not change what the projection observes.

The order model id is resolved from the `[ModelId]` property on the events (the default `PreferAttribute` strategy). The seeder also encodes the order `Guid` as the last subject segment, so setting `OrdersProjection:ModelIdResolutionStrategy` to `RequireStreamId` keys on the subject instead.

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.EventSourcingDB_Postgres.Example.csproj
```

The worker creates the projection schema on startup (`EnsureCreated`) and then runs the projection pipeline. Browse to `http://localhost:5277/` to confirm it is running.

## Run The Live Consistency Flow

With Docker, .NET, `curl`, and `jq` installed:

```bash
./scripts/run-live-consistency-flow.sh
```

The script resets the compose volumes, starts the infrastructure, seeds 10,000 events,
starts the worker, waits for the `Projection pipeline fully drained` message, seeds
another 5,000 events, and waits until the live projection is fully drained and the
replay consistency result reports no mismatch.

Override the event counts, seeds, or timeouts with `INITIAL_EVENTS`, `SECOND_EVENTS`,
`CONSISTENCY_SEED`, `INITIAL_SEED`, `SECOND_SEED`, `STARTUP_TIMEOUT_SECONDS`,
`DRAIN_TIMEOUT_SECONDS`, `FLUSH_QUIET_SECONDS`, and `CONSISTENCY_TIMEOUT_SECONDS`.

The repository's `Example Consistency` CI workflow runs this flow for every pull
request, merge queue entry, and push to `master`.

## Run The On-Demand Replay Consistency Check

The replay consistency diagnostic compares:

- the current `Order` rows already persisted in PostgreSQL
- a fresh per-subject replay from EventSourcingDB using the same `OrdersProjection`

This lets you verify that the projection output stored in the database matches what the projection would produce if each order's events were replayed from scratch. For each order it reads the events under `/orders/{guid}` and folds them through the projection, then compares the result to the persisted row.

Before running the diagnostic, start the infrastructure, seed events, and run the worker. The diagnostic uses the same EventSourcingDB and PostgreSQL connections already configured in `appsettings.json`. For the strongest verification, run it after the projection has fully drained.

The worker exposes:

```text
POST /diagnostics/orders/replay-consistency
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

Request fields:

- `orderId` — optional. If omitted, every persisted order is checked; if supplied, only that order.
- `stopOnFirstMismatch` — optional, defaults to `true`. When `true`, returns as soon as the first mismatch is found.
- `logEvery` — optional, defaults to `500`. Controls how often progress is written to the worker logs.

Successful responses return HTTP `200 OK` and a JSON body like:

```json
{
  "isConsistent": true,
  "totalOrders": 4002,
  "checkedOrders": 4002,
  "startedAtUtc": "2026-06-23T11:00:00.0000000+00:00",
  "completedAtUtc": "2026-06-23T11:00:12.0000000+00:00",
  "elapsedMilliseconds": 12000.0,
  "mismatch": null
}
```

If a mismatch is found, `mismatch` contains `orderId`, `reason`, `databaseSnapshotJson`, and `replaySnapshotJson` — the last two are intended for direct side-by-side comparison.

Notes:

- This diagnostic is read-only. It does not modify the projection tables or EventSourcingDB.
- The check is against the current contents of PostgreSQL at the moment you call it; if the worker is still processing new events, results can change while it runs.

## Restart Projection State Without Reseeding Events

```bash
./scripts/reset-projection-state.sh
```

## Configuration

- `EventSourcingDb:BaseUrl` and `EventSourcingDb:ApiToken` configure the EventSourcingDB connection.
- `ConnectionStrings:PostgreSql` configures the projection store.
- `OrdersProjection:InstanceId` is the deployment identity. Change it when the projection logic changes and you want the new deployment to replay from the beginning in parallel before switching reads over.
- `OrdersProjection:EventVersionCheckStrategy` must be `Monotonic` (default here) or `Disabled`. `Consecutive` is not supported by the EventSourcingDB source and fails fast at startup.
- `OrdersProjection:EventSourcingDB:Subject` and `:Recursive` select the observed subject scope.
