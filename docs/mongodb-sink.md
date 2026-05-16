# MongoDB Sink

`Kuna.Projections.Sink.MongoDB` provides a native MongoDB-backed implementation of:

- `IModelStateSink<TState>`
- `IModelStateStore<TState>`
- `ICheckpointStore`
- `IProjectionFailureHandler<TState>`
- `IProjectionStartupTask`

It persists one collection per projection model type plus shared checkpoint and failure collections.

## Install

```bash
dotnet add package Kuna.Projections.Sink.MongoDB
```

## Register

```csharp
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Core;
using Kuna.Projections.Sink.MongoDB;
using Kuna.Projections.Source.KurrentDB;

[InitialEvent<AccountCreated>]
public sealed class AccountProjection : Projection<Account>
{
    public AccountProjection(Guid id)
        : base(id)
    {
    }
}

services.AddProjection<Account>(configuration, settingsSectionName: "AccountProjection")
    .UseKurrentDbSource(loggerFactory)
    .UseMongoDataStore(
        "mongodb://localhost:27017",
        "account_projection",
        options =>
        {
        });
```

Optional overrides:

```csharp
services.AddProjection<Account>(configuration, settingsSectionName: "AccountProjection")
    .UseKurrentDbSource(loggerFactory)
    .UseMongoDataStore(
        "mongodb://localhost:27017",
        "account_projection",
        options =>
        {
            options.CollectionPrefix = "accounts";
            options.SetModelCollectionName<Account>("account_documents");
        });
```

`AddProjection<TState>(...)` resolves the initial event type from the projection class attribute:

- annotate the projection with `[InitialEvent<TEvent>]`
- do not call `WithInitialEvent(...)`; that API is no longer used

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
- Mongo-specific serializers are scoped through the registered BSON class maps for projection models; the package does not replace MongoDB's global `Guid` serializer for the process

## Replay-Safe Persistence Semantics

The sink preserves the pipeline replay assumptions:

- duplicate inserts are skipped
- stale updates are skipped when `ExpectedEventNumber` no longer matches because a newer event already won the race
- stale deletes are skipped when `ExpectedEventNumber` no longer matches
- items created and deleted within the same flush are not persisted
- model writes can succeed even when sibling writes in the same batch fail

For batch updates, the sink verifies matched counts and performs conditional read-back only when Mongo reports that one or more update operations did not match. That read-back distinguishes:

- writes that already applied successfully
- stale replay misses that should be ignored
- missing documents that should be surfaced as real persistence failures

Checkpoints remain separate from model persistence. If model writes succeed and checkpoint persistence fails, replay remains safe because duplicate inserts and stale update/delete retries are tolerated.

## Failure Handling

On persistence failure, the sink:

- inserts one failure record keyed by `ModelName + InstanceId + ModelId`
- marks the model as faulted when the model document still exists
- records failures even when the model document does not exist yet
- preserves the original stored failure payload when later failures occur for the same `ModelName + InstanceId + ModelId`
- truncates persisted exception text to the first 500 characters before storage
- persists the failure record and model fault flag inside a MongoDB transaction

The sink does not create stub model documents just to hold failure state. First-event and missing-state failures live only in `projection_failures`.

Because transactional failure handling spans the model and failure collections, the deployment must support MongoDB multi-document transactions.

## Startup Behavior

The package registers an `IProjectionStartupTask` that ensures:

- the model collection exists
- the checkpoint collection exists
- the failure collection exists
- the unique failure index on `ModelName + InstanceId + ModelId` exists

Startup directly attempts to create the required collections and tolerates `NamespaceExists`, so repeated or concurrent startup remains idempotent. Startup still fails if MongoDB collection or index initialization fails for any other reason.

## Registration Notes

At registration time, the package:

- registers one keyed `IMongoClient` singleton per connection string
- registers one keyed `IMongoDatabase` per projection registration
- registers one keyed `ICollectionNamer` per projection registration
- initializes the Mongo class map for `TState`
- uses the configured projection `InstanceId` for persisted projection failures
