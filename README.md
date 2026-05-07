# Kuna.Projections

<img src="artifacts/logo.png" alt="Kuna.Projections logo" width="320" />

`Kuna.Projections` is a high-throughput .NET projection pipeline that turns event streams into read models aka materialized views aka projections. It separates projection behavior from event ingestion, persistence, checkpointing, batching, and failure tracking so application code can stay focused on read-model logic.

The architecture is built around independent seams with explicit bounds. Event ingestion, projection execution, and persistence do not have to run as one synchronous loop. Each part can move independently within configured limits for buffering, batching, cache size, and checkpoint advancement.

Today, the repository provides:

- `Kuna.Projections.Abstractions` for shared contracts and model types
- `Kuna.Projections.Core` for the projection runtime and pipeline
- `Kuna.Projections.Source.KurrentDB` for KurrentDB-backed event ingestion
- `Kuna.Projections.Sink.EF` for the shared EF Core-backed relational sink
- `Kuna.Projections.Sink.EF.Npgsql`, `Kuna.Projections.Sink.EF.SqlServer`, and `Kuna.Projections.Sink.EF.MySql` for provider-specific relational registration and duplicate-key handling
- `examples/Kuna.Projections.Worker.Kurrent_EF.Example` as the runnable reference worker

If you want the shortest route to a running worker, start with [docs/quickstart.md](docs/quickstart.md). If you want the full architecture and API map, start with [docs/overview.md](docs/overview.md).

## Why this exists

Projection code is usually simple. Projection plumbing is not.

In event-sourced systems, projections are a standard way to turn append-only domain event streams into queryable read models. The write side preserves the event history, while the read side usually needs a materialized view shaped for API queries, reporting, dashboards, search, or operational workflows. That pattern is common, but teams often end up rebuilding the same projection runtime concerns around it in each service.

The library is aimed at that event-sourcing use case first, but it is not limited to it. The runtime is source-driven rather than hard-wired to one storage model, so it can also be used in non-event-sourced systems by implementing a different source that emits envelopes from some other upstream change feed or event stream.

This library moves the repetitive infrastructure concerns out of the application:

- event reading and deserialization
- model id resolution
- projection instance lifecycle
- replay and catch-up handling
- batching and flush control
- checkpoint persistence
- projection failure recording

That leaves projection authors with a smaller, clearer programming model: define a state type, define events, implement `Apply(...)` methods, wire the source and sink, and run the pipeline.

In other words, the library gives you a reusable projection engine: it reads envelopes from a source, routes them to projection instances, tracks replay progress with checkpoints, batches persistence work, records failures, and keeps the projection code itself focused on state transitions instead of pipeline mechanics.

It also gives consumers a better throughput model:

- bounded buffering instead of unbounded memory growth
- separate catch-up and live-processing persistence strategies
- decoupled source, runtime, and sink responsibilities
- checkpoint-driven recovery instead of ad hoc replay loops

## Quickstart

The smallest credible setup is:

1. Add the projection packages.
2. Define a read model.
3. Define domain events.
4. Implement a projection with `Apply(...)` methods.
5. Register the source, sink, and projection runtime.
6. Run `IProjectionPipeline`.

### Install packages

```bash
dotnet add package Kuna.Projections.Core
dotnet add package Kuna.Projections.Source.KurrentDB
dotnet add package Kuna.Projections.Sink.EF
dotnet add package Kuna.Projections.Sink.EF.Npgsql
```

### Define a read model

```csharp
using Kuna.Projections.Abstractions.Models;

public sealed class Account : Model
{
    public string Email { get; set; } = string.Empty;
    public bool Verified { get; set; }
}
```

### Define events

```csharp
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

public sealed class AccountCreated : Event
{
    public Guid AccountId { get; init; }

    public required string Email { get; init; }
}

public sealed class EmailVerified : Event
{
    [ModelId]
    public Guid AcctId { get; init; }
}
```

### Implement a projection

```csharp
using Kuna.Projections.Core;

public sealed class AccountProjection : Projection<Account>
{
    public AccountProjection(Guid modelId)
        : base(modelId)
    {
    }

    public void Apply(AccountCreated @event) => this.My.Email = @event.Email;

    public void Apply(EmailVerified @event) => this.My.Verified = true;
}
```

Delete semantics are terminal. If your domain has a delete event, handle it in the projection and call `DeleteModel()`:

```csharp
public sealed class AccountDeleted : Event
{
    public Guid AccountId { get; init; }
}

public void Apply(AccountDeleted @event)
{
    this.DeleteModel();
}
```

That marks the projection row for physical deletion on the next flush. The runtime does not soft-delete the model and does not keep deleted state in the in-memory handoff cache. The operating assumption is that, after a delete event for a model, later events for that same model are invalid and should fail because the model no longer exists.

### Register the runtime

```csharp
using Kuna.Projections.Core;
using Kuna.Projections.Sink.EF.Npgsql;
using Kuna.Projections.Source.KurrentDB;
using Microsoft.EntityFrameworkCore;

services.AddProjectionHost(typeof(Program).Assembly);

services.AddDbContext<AccountProjectionDbContext>(
    options => options.UseNpgsql(configuration.GetConnectionString("PostgreSql")));

services.AddProjection<Account>(configuration, "AccountProjection")
        .UseKurrentDbSource(loggerFactory)
        .UseNpgsqlDataStore<Account, AccountProjectionDbContext>(schema: "dbo");
```

The projection type declares its own creation event:

```csharp
using Kuna.Projections.Abstractions.Attributes;

[InitialEvent<AccountCreated>]
public sealed class AccountProjection : Projection<Account>
{
}
```

Then run the pipeline from your host:

```csharp
await pipeline.RunAsync(stoppingToken);
```

For a full working example, see [examples/Kuna.Projections.Worker.Kurrent_EF.Example](examples/Kuna.Projections.Worker.Kurrent_EF.Example).

## Architecture at a glance

The important seam lines are:

- source: where events come from
- projection: how the read model changes
- sink: where the read model and operational metadata are stored
- pipeline: how replay, batching, flushes, and recovery are coordinated

Those seam lines are also throughput boundaries. The source can continue reading, the runtime can continue transforming, and the sink can continue flushing within configured operational bounds rather than forcing a per-event end-to-end write cycle.

Current flow:

1. The Kurrent source reads events and emits envelopes.
2. The core pipeline resolves or loads projection state.
3. The projection applies the event to the in-memory model.
4. The EF sink persists model changes.
5. The checkpoint store records the last durable global position.
6. Failures are stored for later inspection and recovery.

That separation is what makes high-throughput projection processing practical: each stage can absorb short-term pressure independently while the pipeline still stays bounded and recoverable.

## Configuration

At minimum, applications typically provide:

- `ConnectionStrings:KurrentDB`
- `ConnectionStrings:PostgreSql`
- one projection settings section per registered projection

Example:

```json
{
  "ConnectionStrings": {
    "PostgreSql": "Host=localhost;Port=5432;Database=orders_projection;Username=postgres;Password=postgres",
    "KurrentDB": "esdb://admin:changeit@localhost:2113?tls=false"
  },
  "OrdersProjection": {
    "InstanceId": "orders-v1",
    "Source": "KurrentDB",
    "KurrentDB": {
      "Filter": {
        "Kind": "StreamPrefix",
        "Prefixes": [ "order" ]
      }
    }
  }
}
```

`InstanceId` is required. It identifies one deployed projection instance, not the `TState` type in the abstract.

That distinction matters when the projection logic changes and you want a blue/green rollout:

1. keep the current deployment running with `InstanceId = "orders-v1"`
2. deploy the new projection with `InstanceId = "orders-v2"`
3. let `orders-v2` replay from the beginning into its own checkpoint and storage boundary
4. validate the rebuilt `orders-v2` read model
5. switch readers or configuration to `orders-v2`
6. retire `orders-v1` later

The point is to make projection changes replay-safe. A new `InstanceId` starts a new checkpoint lineage, so the new deployment can rebuild from the start without overwriting the current deployment's checkpoint or failure records.

`KurrentDB:Filter` maps onto the Kurrent .NET subscription filter model:

- `Kind` selects the filter shape.
- Use `Prefixes` for prefix-based kinds.
- Use `Regex` for regex-based kinds.

Supported `Kind` values are `StreamPrefix`, `StreamRegex`, `EventTypePrefix`, and `EventTypeRegex`.

For the full Kurrent .NET filtering model and semantics, see the official documentation:
https://docs.kurrent.io/clients/dotnet/v1.0/subscriptions.html

Full configuration details live in [docs/configuration-reference.md](docs/configuration-reference.md).
