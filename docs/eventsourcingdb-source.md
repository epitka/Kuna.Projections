# EventSourcingDB Source

`Kuna.Projections.Source.EventSourcingDB` ingests events from [EventSourcingDB](https://www.eventsourcingdb.io/) and emits projection envelopes into the core pipeline. It is built on the official `EventSourcingDb` .NET client and mirrors the design of the other Kuna sources, so the projection programming model is identical regardless of where events come from.

For the shared projection settings (flush, backpressure, caching), see [configuration-reference.md](configuration-reference.md). For the full architecture, see [overview.md](overview.md).

## Install

```bash
dotnet add package Kuna.Projections.Core
dotnet add package Kuna.Projections.Source.EventSourcingDB
```

Add a sink package as well, for example `Kuna.Projections.Sink.EF.Npgsql` or `Kuna.Projections.Sink.MongoDB`.

## Register

The connection is configured through the EventSourcingDB SDK's own `EventSourcingDb` configuration section:

```json
{
  "ConnectionStrings": {
    "PostgreSql": "Host=localhost;Port=5432;Database=orders_projection;Username=postgres;Password=postgres"
  },
  "EventSourcingDb": {
    "BaseUrl": "http://localhost:3000",
    "ApiToken": "secret"
  },
  "OrdersProjection": {
    "InstanceId": "orders-v1",
    "EventVersionCheckStrategy": "Monotonic",
    "EventSourcingDB": {
      "Subject": "/orders",
      "Recursive": true
    }
  }
}
```

Wire the source on the projection registration:

```csharp
using Kuna.Projections.Core;
using Kuna.Projections.Sink.EF.Npgsql;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.EntityFrameworkCore;

services.AddProjectionHost(typeof(Program).Assembly);

services.AddDbContext<OrdersDbContext>(
    options => options.UseNpgsql(configuration.GetConnectionString("PostgreSql")));

services.AddProjection<Order>(configuration, "OrdersProjection")
        .UseEventSourcingDbSource(loggerFactory)
        .UseNpgsqlDataStore<Order, OrdersDbContext>(schema: "orders_projection");
```

The EventSourcingDB client is registered once per service collection. If the application already registered an `IClient` (for example with its own `AddEventSourcingDb(...)` call), the source reuses it.

## Semantics

### One ordered stream per projection

EventSourcingDB delivers events in a single, globally ordered stream; the event `id` is a monotonically increasing integer. The source observes one subject scope (`Subject` + `Recursive`, default `/` recursive) and checkpoints on the event `id`. Use one subject root per projection. Splitting ingestion across several narrow subjects would require an order-preserving merge and is intentionally not supported.

### Event version strategy

EventSourcingDB exposes only the global `id`, not a per-aggregate version. The source maps the global `id` onto `EventNumber` and requires `EventVersionCheckStrategy.Monotonic` (recommended) or `EventVersionCheckStrategy.Disabled`. `Consecutive` is **not supported** — it needs gapless per-aggregate numbers — and `UseEventSourcingDbSource(...)` fails fast at startup if a projection is configured with it.

`Monotonic` also makes at-least-once replay idempotent: after a crash between persisting model state and the checkpoint, redelivered events are skipped because their id is not greater than the model's last applied id.

### Model id resolution

Model ids are resolved through `ModelIdResolutionStrategy` and the `[ModelId]` attribute, exactly as for the other sources. When deriving the id from the subject, the configured segment (the last segment by default, e.g. `{guid}` in `/orders/{guid}`) is parsed as a `Guid`.

> `Kuna.Projections` models read-model keys as `Guid` (`IModel.Id`). This constraint comes from the framework itself, not from the EventSourcingDB source. Subjects whose key segment is not a `Guid` must carry the id through a `[ModelId]` property or map it to a `Guid` with a custom strategy.

### Event type mapping

CloudEvent `type` values are mapped to CLR event types by name. By default the segment after the last dot is used (`io.kuna.orders.OrderCreated` → `OrderCreated`), matched case-insensitively. Pass a custom resolver when your naming differs:

```csharp
.UseEventSourcingDbSource(loggerFactory, eventTypeNameResolver: type => type.Split('.').Last())
```

Unmapped types are delivered as `UnknownEvent`, so a projection can ignore or explicitly handle them via `Apply(UnknownEvent)`.

### Caught-up signaling

EventSourcingDB's observe stream has no explicit caught-up marker. The source reads the head position once on start and emits `ProjectionCaughtUpEvent` either immediately (when the checkpoint is already at or beyond the head) or once an observed event reaches it. The signal only switches the pipeline from catch-up batching to live processing; if it is missing or imprecise it affects live-processing latency, never correctness.

## Example

See [examples/Kuna.Projections.Worker.EventSourcingDB_Postgres.Example](../examples/Kuna.Projections.Worker.EventSourcingDB_Postgres.Example) for a runnable EventSourcingDB to PostgreSQL worker.
