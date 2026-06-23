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

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.EventSourcingDB_Postgres.Example.csproj
```

The worker creates the projection schema on startup (`EnsureCreated`) and then runs the projection pipeline. Browse to `http://localhost:5277/` to confirm it is running.

## Seed Events

This example reuses the shared `Kuna.Examples.EventsSeeder` (the same generator that backs the Kurrent and Kafka examples). It writes generated order events to EventSourcingDB under the `/orders` subject root, one subject per order (`/orders/{guid}`), with the CloudEvent `type` set to `io.kuna.orders.<EventName>`.

```bash
./scripts/seed-eventsourcingdb-live.sh
```

Environment variables override the defaults: `TARGET_EVENTS` (default `50000`), `MIN_COMPLETE_ORDERS` (default `3000`), `ESDB_BASE_URL` (default `http://localhost:3000`), `ESDB_API_TOKEN` (default `secret`), and `ESDB_BATCH_SIZE` (default `500`, the number of events per `WriteEvents` call).

> Note: EventSourcingDB writes are considerably slower than reads, so the time the seeder takes is not a measure of projection (read) throughput. `ESDB_BATCH_SIZE` only affects how fast the seeder fills the store; it does not change what the projection observes.

The order model id is resolved from the `[ModelId]` property on the events (the default `PreferAttribute` strategy). The seeder also encodes the order `Guid` as the last subject segment, so setting `OrdersProjection:ModelIdResolutionStrategy` to `RequireStreamId` keys on the subject instead.

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
