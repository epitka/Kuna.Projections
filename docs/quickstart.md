# Quickstart

This guide covers the shortest credible path to a running projection: select or create a host project, define a read model, define events, write a projection, register the libraries, and run the pipeline.

The examples use:

- `Kuna.Projections.Core`
- `Kuna.Projections.Source.Kurrent` (source implementation for KurrentDB)
- `Kuna.Projections.Sink.EF` (sink implemenation using Entity Framework)

If you want the broader package map first, see [overview.md](overview.md). If you want every configuration knob documented, see [configuration-reference.md](configuration-reference.md).

## What You Build

You define five things:

1. a host project
2. a read model
3. event types
4. a projection with `Apply(...)` methods
5. host registration that runs the projection pipeline

Optional:

6. a projection startup task for one-time initialization such as ensuring the projection database exists

The libraries provide the rest:

- reading from an `IAsyncEnumerable` source
- deserializing events
- resolving model ids
- loading existing projection state
- batching writes
- persisting checkpoints
- recording projection failures

## 1. Select Or Create A Host Project

Start with a .NET project that will host your projection pipeline.

This can be:

- a worker service
- a console application
- a Web API project
- an existing application where you want the projection runtime to run

## 2. Add Packages

Add the projection packages your worker needs:

```bash
dotnet add package Kuna.Projections.Core
dotnet add package Kuna.Projections.Source.Kurrent
dotnet add package Kuna.Projections.Sink.EF
```

`Kuna.Projections.Abstractions` is brought in transitively by those packages, so you usually do not need to add it explicitly unless you want a direct contracts-only dependency.

## 3. Create A Read Model

Start with a "state" object representing your read model that inherits `Model`.

```csharp
using Kuna.Projections.Abstractions.Models;

public sealed class Account : Model
{
    public string Email { get; set; } = string.Empty;
    public bool Verified { get; set; }
}
```

`Model` already gives you the fields that projection runtime needs:

- `Id`
- `EventNumber`
- `GlobalEventPosition`
- `HasStreamProcessingFaulted`

## 4. Create Event Types

Events must inherit `Event`.

```csharp
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

public sealed class AccountCreated : Event
{
    [ModelId]  // <- not required, see notes
    public Guid AccountId { get; init; }

    public required string Email { get; init; }
}

public sealed class EmailVerified : Event
{
    [ModelId]
    public Guid AccountId { get; init; }
}
```

Important note:
- `[ModelId]` is not required; model ids can also be resolved from the stream id. See [configuration-reference.md](configuration-reference.md#modelidresolutionstrategy).
- the current KurrentDB-backed source implementation discovers event CLR types from the entry assembly by scanning exported types derived from `Event`
- with the default `ModelIdResolutionStrategy` of `PreferAttribute`, the source uses a `[ModelId]` property when present; otherwise it falls back to resolving the model id from the stream id


## 5. Implement The Projection

Create a projection by inheriting `Projection<TState>` and adding `Apply(...)` methods.

```csharp
using Kuna.Projections.Core;

public sealed class AccountProjection : Projection<Account>
{
    internal AccountProjection(Guid modelId)
        : base(modelId)
    {
    }

    public void Apply(AccountCreated @event)
    {
        this.My.Email = @event.Email;
        // Alternative explicit form:
        // this.ModelState.Email = @event.Email;
    }

    public void Apply(EmailVerified @event)
    {
        this.My.Verified = true;
        // Alternative explicit form:
        // this.ModelState.Verified = true;
    }
}
```

Important constraints:

- the projection class must be `public`, because `AddProjection<TState>` discovers projections via `Assembly.GetExportedTypes()`
- `AddProjection<TState>` requires exactly one public `Projection<TState>` for that `TState` in the assembly; the same assembly may also contain projections for other state types
- projections are not auto-registered across the assembly; the application enables a projection explicitly by calling `AddProjection<TState>(...)` for the state type it wants to run
- the projection constructor must accept a single `Guid`, which is the model id for the projection instance and model state; this is the current shape and is expected to expand later to support other id types

At runtime, the base class updates `EventNumber` and `GlobalEventPosition` after each successful apply.

## 6. Create The Projection DbContext

Your relational DbContext should inherit `SqlProjectionsDbContext` so it includes checkpoint and failure tables, then add your model entities.

Any relational database supported by an Entity Framework Core provider can be used here, for example PostgreSQL, SQL Server, or MySQL.

```csharp
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;

public sealed class AccountProjectionDbContext : SqlProjectionsDbContext
{
    public AccountProjectionDbContext(
        DbContextOptions<AccountProjectionDbContext> options,
        ProjectionSchema<AccountProjectionDbContext> projectionSchema)
        : base(options, projectionSchema.Value)
    {
    }

    public DbSet<Account> Accounts { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Account>(
            account =>
            {
                account.Property(x => x.Email).HasMaxLength(250);
            });

        base.OnModelCreating(modelBuilder);
    }
}
```

The sink persists:

- your read model tables
- `CheckPoint`
- `ProjectionFailure`

`SqlProjectionsDbContext` configures the common `IModel` fields for every mapped projection model:

- `Id` as the primary key
- `EventNumber` as a concurrency token
- `GlobalEventPosition` with the shared `bigint` conversion

That means you only configure projection-specific fields such as lengths, indexes, relationships, and owned types in your derived DbContext. The inherited projection fields are mapped by `SqlProjectionsDbContext` when you call `base.OnModelCreating(modelBuilder)`.

## 7. Optionally Add A Projection Startup Task

If your projection needs one-time initialization before pipelines start, implement `IProjectionStartupTask`.

Typical uses:

- ensure the projection database exists
- apply projection-specific initialization
- validate required dependencies before processing starts

```csharp
using Kuna.Projections.Abstractions.Services;

public sealed class AccountProjectionStartupTask : IProjectionStartupTask
{
    private readonly IServiceProvider serviceProvider;

    public AccountProjectionStartupTask(IServiceProvider serviceProvider)
    {
        this.serviceProvider = serviceProvider;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var scope = this.serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<AccountProjectionDbContext>();
        await dbContext.Database.EnsureCreatedAsync(cancellationToken);
    }
}
```

You only need this when your projection has startup work to do.

## 8. Run The Pipeline In A Host

There are two supported hosting patterns:

- recommended: use `AddProjectionHost(...)` and let the library run all registered pipelines
- manual: write your own `BackgroundService` that resolves `IProjectionPipeline` and calls `RunAsync(...)`

## 9. Register services for DI

```csharp
using Kuna.Projections.Core;
using Kuna.Projections.Source.Kurrent;
using Kuna.Projections.Sink.EF;
using Microsoft.EntityFrameworkCore;

services.AddSingleton<IProjectionStartupTask, AccountProjectionStartupTask>();
services.AddProjectionHost(typeof(Program).Assembly);

services.AddDbContext<AccountProjectionDbContext>(
    options => options.UseNpgsql(configuration.GetConnectionString("PostgreSql")));

services.AddKurrentDBSource<Account>(configuration, loggerFactory, "AccountProjection");
services.AddSqlProjectionsDataStore<Account, AccountProjectionDbContext>(schema: "dbo");
services.AddProjection<Account>(configuration, settingsSectionName: "AccountProjection");
```

Note that DbContext requires db schema to be passed in. For more information see [configuration-reference.md](configuration-reference.md#when-to-use-a-projection-specific-schema).

These calls do the heavy lifting:

- `AddKurrentDBSource<TState>(...)` wires the current KurrentDB-backed implementation of `IEventSource<EventEnvelope>`
- `AddSqlProjectionsDataStore<TState, TDataContext>(schema: ...)` wires SQL-backed state loading, persistence, checkpointing, and failure handling
- `AddProjection<TState>(...)` wires projection creation, transformation, caching, and the runtime pipeline
- `AddProjectionHost(...)` runs all registered pipelines and startup tasks
- `AddHostedService<TWorker>()` is the manual alternative if you are not using `AddProjectionHost(...)`

## 10. Add Configuration

At minimum you need connection strings and one projection section that contains both projection runtime settings and a nested `KurrentDB` source section.

```json
{
  "ConnectionStrings": {
    "KurrentDB": "esdb://localhost:2113?tls=false",
    "PostgreSql": "Host=localhost;Port=5432;Database=accounts_projection;Username=postgres;Password=postgres"
  },
  "AccountProjection": {
    "Source": "KurrentDB",
    "KurrentDB": {
      "Filter": {
        "Kind": "StreamPrefix",
        "Prefixes": [ "Account" ]
      }
    }
  }
}
```

Useful settings notes:

- `Source` defaults to `KurrentDB`, but the section is still shown explicitly here because the worker must contain a matching nested `KurrentDB` section
- `KurrentDB.Filter.Prefixes` currently requires exactly one prefix and is used as the prefix filter for the Kurrent subscription
- the default `ModelIdResolutionStrategy` on the root projection section is `PreferAttribute`
- projection settings can be bound from a named section by calling `AddProjection<TState>(configuration, settingsSectionName: "AccountProjection")`
- source settings are bound by calling `AddKurrentDBSource<TState>(configuration, loggerFactory, "AccountProjection")`

For the full set of available settings and defaults, see [configuration-reference.md](configuration-reference.md).

## 10. Run The Pipeline

When the host starts, the registered projection host resolves all registered `IProjectionPipeline` instances and runs them.

## See The Runnable Example

The best real reference in this repository is [examples/Kuna.Projections.Worker.Kurrent_EF.Example](../examples/Kuna.Projections.Worker.Kurrent_EF.Example).

Check ReadMe to run projection and to perform consistency check:

- [README.md](../examples/Kuna.Projections.Worker.Kurrent_EF.Example/README.md)

## Next

After the quickstart, the next useful reads are:

- [configuration-reference.md](configuration-reference.md)
- [overview.md](overview.md)
