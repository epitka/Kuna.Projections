# MongoDB Sink

`Kuna.Projections.Sink.MongoDB` provides a native MongoDB-backed implementation of:

- `IModelStateSink<TState>`
- `IModelStateStore<TState>`
- `ICheckpointStore`
- `IProjectionCheckpointStore<TState>`
- `IProjectionFailureHandler<TState>`
- `IProjectionStartupTask`

It persists one collection per projection model type plus shared checkpoint and failure collections.

## Install

```bash
dotnet add package Kuna.Projections.Sink.MongoDB
```

## Register

```csharp
using Kuna.Projections.Sink.MongoDB;

services.AddMongoProjectionsDataStore<Account>(
    options =>
    {
        options.ConnectionString = "mongodb://localhost:27017";
        options.DatabaseName = "account_projection";
    });
```

Optional overrides:

```csharp
services.AddMongoProjectionsDataStore<Account>(
    options =>
    {
        options.ConnectionString = "mongodb://localhost:27017";
        options.DatabaseName = "account_projection";
        options.CollectionPrefix = "accounts";
        options.CheckpointCollectionName = "accounts_checkpoints";
        options.FailureCollectionName = "accounts_failures";
        options.SetModelCollectionName<Account>("account_documents");
    });
```

## Collection Naming

Default names:

- model collection: `{CollectionPrefix}_{model-name}`
- checkpoint collection: `projection_checkpoints`
- failure collection: `projection_failures`

`model-name` is the projection state type name converted to snake case without a trailing `Model` suffix.

Example:

- `OrderModel` with the default prefix becomes `projection_order`
- `AccountState` with `CollectionPrefix = "billing"` becomes `billing_account_state`

## Persisted Shape

- `IModel.Id` is stored as Mongo `_id` using the canonical `Guid` string form
- `GlobalEventPosition` is stored as a string
- projection state is stored directly as the root document, not wrapped in an envelope
- `EventNumber` remains the optimistic concurrency value for updates and deletes

## Replay-Safe Persistence Semantics

The sink preserves the pipeline replay assumptions:

- duplicate inserts are skipped
- stale updates are skipped when `ExpectedEventNumber` no longer matches
- stale deletes are skipped when `ExpectedEventNumber` no longer matches
- items created and deleted within the same flush are not persisted
- model writes can succeed even when sibling writes in the same batch fail

Checkpoints remain separate from model persistence. If model writes succeed and checkpoint persistence fails, replay remains safe because duplicate inserts and stale update/delete retries are tolerated.

## Failure Handling

On persistence failure, the sink:

- marks the model as faulted when the model document still exists
- upserts a failure record keyed by `ModelName + ModelId`
- replaces stored failure text and metadata with the newest failure
- truncates persisted exception text to the latest 500 characters before storage

## Startup Behavior

The package registers an `IProjectionStartupTask` that ensures:

- the model collection exists
- the checkpoint collection exists
- the failure collection exists
- the unique failure index on `ModelName + ModelId` exists

Startup fails if MongoDB collection or index initialization fails.
