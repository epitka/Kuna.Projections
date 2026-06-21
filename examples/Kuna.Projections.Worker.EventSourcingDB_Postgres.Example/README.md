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

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.EventSourcingDB_Postgres.Example.csproj
```

The worker creates the projection schema on startup (`EnsureCreated`) and then runs the projection pipeline. Browse to `http://localhost:5277/` to confirm it is running.

## Seed Events

This example does not ship a seeder. Write events to EventSourcingDB under the configured subject (`/orders` by default) with your own producer or the EventSourcingDB API/CLI. Events whose CloudEvent `type` ends in an event name that the `OrdersProjection` knows (for example `io.kuna.orders.OrderCreated`) are projected; other types are ignored.

The order model id is resolved from the `[ModelId]` property on the events (the default `PreferAttribute` strategy). To key on the subject instead, set `OrdersProjection:ModelIdResolutionStrategy` to `RequireStreamId` and use subjects whose last segment is the order `Guid`, for example `/orders/{guid}`.

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
