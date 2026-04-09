# Configuration Reference

This document describes the library-owned configuration surface used by:

- `Kuna.Projections.Core`
- `Kuna.Projections.Source.Kurrent`

It also calls out one important distinction:

- some applications layer their own projection-specific configuration on top of the shared library settings

For the shortest getting-started path, see [quickstart.md](quickstart.md).

## Overview

The main configuration sections are:

- `ConnectionStrings`
- one projection settings section per registered projection

In the runnable example, that section is:

- `OrdersProjection`

That section is bound to `ProjectionSettings` by application code via `AddProjection<TState>(..., settingsSectionName: "OrdersProjection")`.

## Example

```json
{
  "ConnectionStrings": {
    "PostgreSql": "Host=localhost;Port=5432;Database=orders_projection;Username=postgres;Password=postgres",
    "EventStore": "esdb://admin:changeit@localhost:2113?tls=false"
  },
  "OrdersProjection": {
    "CatchUpPersistenceStrategy": "ModelCountBatching",
    "LiveProcessingPersistenceStrategy": "TimeBasedBatching",
    "MaxPendingProjectionsCount": 100,
    "LiveProcessingFlushDelay": 1000,
    "SkipStateNotFoundFailure": false,
    "InFlightModelCacheMinEntries": 10000,
    "InFlightModelCacheCapacityMultiplier": 3,
    "EventVersionCheckStrategy": "Consecutive",
    "EventStoreSource": {
      "StreamName": "order-",
      "EventsBoundedCapacity": 12000,
      "ModelIdResolutionStrategy": "RequireStreamId"
    }
  }
}
```

This matches the shape used in [appsettings.json](../examples/Kuna.Projections.Worker.Kurrent_EF.Example/appsettings.json).

## `ConnectionStrings`

The libraries expect these connection string names:

- `EventStore`
  Used by `AddEventStoreSource<TState>(...)` to create the current KurrentDB-backed source implementation.
- `PostgreSql`
  Not required by the library directly, but typically used by the application when constructing the EF `DbContext` that will be passed into `AddSqlProjectionsDataStore<TState, TDataContext>(schema: ...)`.

If `EventStore` is missing or empty, source registration fails.

## Projection Settings Section

Section name: application-defined

Bound to: `ProjectionSettings`

This section controls pipeline behavior in `Kuna.Projections.Core`.

In the runnable example, the section name is `OrdersProjection`.

### `CatchUpPersistenceStrategy`

Type: `PersistenceStrategy`

Allowed values:

- `ModelCountBatching`
- `TimeBasedBatching`
- `ImmediateModelFlush`

Meaning:

- controls how persistence behaves while the pipeline is catching up from an older checkpoint

Guidance:

- `ModelCountBatching` is the sensible default for replay-heavy startup
- `ImmediateModelFlush` trades throughput for lower buffering

### `LiveProcessingPersistenceStrategy`

Type: `PersistenceStrategy`

Allowed values:

- `ModelCountBatching`
- `TimeBasedBatching`
- `ImmediateModelFlush`

Meaning:

- controls how persistence behaves after the source has caught up and the pipeline is processing live events

Default: `ImmediateModelFlush`

Guidance:

- `ImmediateModelFlush` is the current default
- `TimeBasedBatching` trades lower write frequency for additional buffering delay

### `MaxPendingProjectionsCount`

Type: `int`

Meaning:

- upper bound used by the runtime for pending projection work
- also influences internal buffering and cache sizing

Guidance:

- set this explicitly
- a non-zero value is expected in practice
- the example uses `100`

### `LiveProcessingFlushDelay`

Type: `int`

Unit: milliseconds

Meaning:

- controls how often live processing requests a time-based flush

Runtime behavior:

- the pipeline uses `Math.Max(1, LiveProcessingFlushDelay)`, so values less than `1` effectively become `1`

Guidance:

- set this explicitly for predictable live behavior
- the example uses `1000`

### `SkipStateNotFoundFailure`

Type: `bool`

Default: `false`

Meaning:

- when `true`, the runtime skips generating a persisted failure if an event for an existing stream cannot find current state in the store
- when `false`, the runtime records a failure and marks the model as failed for the flush window

Use it when:

- `true` if you want the pipeline to be tolerant of missing state during some replay scenarios
- `false` if missing state should be treated as an operational problem

Current recommendation:

- leave this at `false` unless you have a specific replay or migration scenario that requires tolerant missing-state handling

### `InFlightModelCacheMinEntries`

Type: `int`

Meaning:

- minimum in-flight cache capacity retained even when pending batch sizes are small

Guidance:

- adjust if you expect very high concurrency or a large replay window
- for most adopters, the default is a reasonable starting point

### `InFlightModelCacheCapacityMultiplier`

Type: `int`

Meaning:

- dynamic cache sizing multiplier tied to `MaxPendingProjectionsCount`
- effective cache size is `max(InFlightModelCacheMinEntries, MaxPendingProjectionsCount * multiplier)`

Use it when:

- you want cache size to scale with your chosen pending-work capacity

### `EventVersionCheckStrategy`

Type: `EventVersionCheckStrategy`

Allowed values:

- `Disabled`
- `Consecutive`
- `Monotonic`

Meaning:

- controls how the projection base class validates event ordering before `Apply(...)` runs

Behavior:

- `Disabled`: ordering checks are skipped
- `Consecutive`: each next event must be exactly previous event number + 1
- `Monotonic`: event numbers must keep increasing, but gaps are allowed

Guidance:

- `Consecutive` is the safest default when stream order matters strongly
- `Monotonic` is useful when duplicate or stale events are more likely than hard gaps
- `Disabled` should be chosen deliberately

## `EventStoreSource`

Section name: nested under the projection section

Bound to: `EventStoreSourceSettings`

This section controls how the current KurrentDB-backed source implementation reads and shapes events before they enter the core pipeline.

In the runnable example, the full path is `OrdersProjection:EventStoreSource`.

### `StreamName`

Type: `string`

Meaning:

- stream name or prefix the source should read from
- this value is required

Guidance:

- the example uses `order-`

### `EventsBoundedCapacity`

Type: `int`

Default: `12000`

Meaning:

- bounded capacity used by the current source implementation while buffering events into the `IAsyncEnumerable` pipeline

Guidance:

- keep the default unless you have measured pressure that requires tuning
- larger values may improve throughput but also increase memory use

### `ModelIdResolutionStrategy`

Type: `ModelIdResolutionStrategy`

Allowed values:

- `PreferAttribute`
- `RequireStreamId`
- `RequireMatch`

Default: `PreferAttribute`

Meaning:

- controls how the source resolves the model id for an event

Behavior:

- `PreferAttribute`: use `[ModelId]` when present, otherwise try the stream id
- `RequireStreamId`: use the stream id as the authoritative source
- `RequireMatch`: require both stream id and `[ModelId]` to resolve and agree

Guidance:

- `PreferAttribute` is the most flexible default
- `RequireStreamId` is a good fit when stream naming is authoritative
- `RequireMatch` is the strictest and safest option when you want validation across both representations

## Application-Specific Sections

The current recommended shape is one projection settings section per registered projection.

From the example:

```json
{
  "OrdersProjection": {
    "CatchUpPersistenceStrategy": "ModelCountBatching",
    "LiveProcessingPersistenceStrategy": "TimeBasedBatching",
    "MaxPendingProjectionsCount": 100,
    "LiveProcessingFlushDelay": 1000,
    "SkipStateNotFoundFailure": false,
    "InFlightModelCacheMinEntries": 10000,
    "InFlightModelCacheCapacityMultiplier": 3,
    "EventVersionCheckStrategy": "Consecutive",
    "EventStoreSource": {
      "StreamName": "order-",
      "EventsBoundedCapacity": 12000,
      "ModelIdResolutionStrategy": "RequireStreamId"
    }
  }
}
```

Why this matters:

- all projection runtime settings live together in one place
- the application can bind that section directly into `ProjectionSettings`
- relational applications can provide schema directly when constructing `SqlProjectionsDbContext`

That pattern is useful when:

- you have multiple projections and want each one to declare its own runtime settings explicitly
- you do not want to merge top-level defaults with per-projection override sections

### When To Use A Projection-Specific Schema

Use a dedicated schema when:

- multiple projection modules share one physical database
- each projection module has its own `DbContext`
- you want each projection to manage its own migrations without colliding with other projection modules

Why this matters:

- `SqlProjectionsDbContext` includes infrastructure tables such as `CheckPoints` and `ProjectionFailures`
- if multiple projection contexts point at the same database and same schema, they can all try to create or manage those tables
- the same applies to projection model tables if different modules reuse generic names such as `Orders`, `Invoices`, or `Customers`

Schema isolation avoids those collisions by namespacing each projection store, for example:

- `orders_projection.CheckPoints`
- `orders_projection.ProjectionFailures`
- `billing_projection.CheckPoints`
- `billing_projection.ProjectionFailures`

This is most useful when:

- you want one database but independent projection modules
- you do not want to force one shared migrations owner for infrastructure tables
- you want the same deployment model to work whether projections are hosted together or separately

You usually do not need a dedicated schema when:

- each projection uses its own database
- one shared `DbContext` owns all projection tables in the database

Important nuance:

- using separate schemas does not share checkpoint or failure tables across projection modules
- that is usually acceptable, because it is the same operational tradeoff you would already have if each projection used its own database

## Recommended Starting Point

For a first production-like setup, start with:

```json
{
  "OrdersProjection": {
    "CatchUpPersistenceStrategy": "ModelCountBatching",
    "LiveProcessingPersistenceStrategy": "TimeBasedBatching",
    "MaxPendingProjectionsCount": 100,
    "LiveProcessingFlushDelay": 1000,
    "SkipStateNotFoundFailure": false,
    "InFlightModelCacheMinEntries": 10000,
    "InFlightModelCacheCapacityMultiplier": 3,
    "EventVersionCheckStrategy": "Consecutive",
    "EventStoreSource": {
      "ModelIdResolutionStrategy": "RequireStreamId"
    }
  }
}
```

Then tune from observed behavior rather than from guesswork.

## Related Docs

- [quickstart.md](quickstart.md)
- [overview.md](overview.md)
