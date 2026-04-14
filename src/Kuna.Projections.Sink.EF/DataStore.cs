using System.Diagnostics;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Kuna.Projections.Sink.EF;

/// <summary>
/// EF Core-backed implementation of model-state persistence, model-state
/// loading, and checkpoint persistence for the projection pipeline.
/// </summary>
public class DataStore<TState, TDataContext>
    : IModelStateSink<TState>,
      IProjectionStoreWriter<TState>,
      IModelStateStore<TState>,
      ICheckpointStore
    where TState : class, IModel, new()
    where TDataContext : DbContext, IProjectionDbContext
{
    private const int DuplicatePkViolationError = 2627;
    private const string PostgresDuplicatePkViolationError = PostgresErrorCodes.UniqueViolation;

    private readonly IServiceProvider serviceProvider;
    private readonly IProjectionFailureHandler<TState> failureHandler;
    private readonly Stopwatch insertStopWatch;
    private readonly Stopwatch updateStopWatch;
    private readonly ILogger logger;
    private readonly List<ProjectionFailure> pendingUpdateFailures;
    private readonly bool hasChildEntities;
    private readonly string modelName;
    private readonly Stopwatch modelWriteMetricsLogStopwatch;
    private double cumulativeModelWriteSaveChangesMs;
    private long cumulativeModelWriteSaveChangesCalls;

    private TDataContext? dbContext;
    private IServiceScope? scope;

    /// <summary>
    /// Initializes the EF-backed model store and its long-lived DbContext scope.
    /// </summary>
    public DataStore(
        IServiceProvider serviceProvider,
        IProjectionFailureHandler<TState> failureHandler,
        ILogger<DataStore<TState, TDataContext>> logger)
    {
        this.serviceProvider = serviceProvider;
        this.failureHandler = failureHandler;
        this.logger = logger;
        this.modelName = ProjectionModelName.For<TState>();

        this.InitDbContext();
        this.pendingUpdateFailures = new List<ProjectionFailure>();
        this.hasChildEntities = this.ValidateAndDetectChildEntities();

        this.insertStopWatch = new Stopwatch();
        this.updateStopWatch = new Stopwatch();
        this.modelWriteMetricsLogStopwatch = Stopwatch.StartNew();
    }

    private TDataContext DbContext => this.dbContext ?? throw new InvalidOperationException("DbContext has not been initialized.");

    /// <summary>
    /// Loads the persisted model state for the specified model id.
    /// </summary>
    public async Task<TState?> Load(Guid modelId, CancellationToken cancellationToken)
    {
        try
        {
            var model = await this.DbContext.FindAsync<TState>([modelId,], cancellationToken);

            if (model != null
                && this.hasChildEntities)
            {
                await this.LoadNavigationGraphAsync(model, cancellationToken);
            }

            if (model != null
                && this.hasChildEntities)
            {
                this.MarkPersistedGraph(model);
            }

            return model;
        }
        catch (Exception ex)
        {
            this.logger.LogError(
                "LoadModel failed for modelName: {Model} id: {ModelId}, with {Exception}",
                this.modelName,
                modelId,
                ex.ToString());

            throw;
        }
    }

    /// <summary>
    /// Persists one batch of model-state changes and then advances the
    /// checkpoint to the batch's global event position.
    /// </summary>
    public async Task PersistBatch(ModelStatesBatch<TState> batch, CancellationToken cancellationToken)
    {
        // let's remove items that are both IsNew and ShouldDelete
        // no reason to insert them and immediately delete them
        var toExclude = batch.Changes
                             .Where(x => x is { IsNew: true, ShouldDelete: true, });

        var toPersist = batch.Changes.Except(toExclude).ToArray();

        await this.PersistBatchInternal(toPersist, cancellationToken);
        this.LogCumulativeModelWriteMetricsIfDue();

        foreach (var modelState in toPersist.Where(x => !x.ShouldDelete))
        {
            if (this.hasChildEntities)
            {
                this.MarkPersistedGraph(modelState.Model);
            }
        }

        await this.PersistCheckpoint(
            new CheckPoint
            {
                ModelName = this.modelName,
                GlobalEventPosition = batch.GlobalEventPosition,
            },
            cancellationToken);

        this.DisposeScope();
        this.InitDbContext();
    }

    public async Task<IReadOnlyList<PersistenceItemOutcome>> WriteBatch(
        PersistenceWriteBatch<TState> batch,
        CancellationToken cancellationToken)
    {
        var toExclude = batch.Items
                             .Where(x => x is { IsNew: true, ShouldDelete: true, })
                             .ToArray();

        var modelStates = batch.Items
                               .Except(toExclude)
                               .Select(
                                   item => new ModelState<TState>(
                                       item.Model,
                                       item.IsNew,
                                       item.ShouldDelete,
                                       item.GlobalEventPosition,
                                       item.ExpectedEventNumber))
                               .ToArray();

        var outcomeStatuses = await this.PersistBatchInternal(modelStates, cancellationToken);
        this.LogCumulativeModelWriteMetricsIfDue();

        foreach (var modelState in modelStates.Where(x => !x.ShouldDelete))
        {
            if (this.hasChildEntities)
            {
                this.MarkPersistedGraph(modelState.Model);
            }
        }

        this.DisposeScope();
        this.InitDbContext();

        return batch.Items
                    .Select(
                        item =>
                        {
                            var status = outcomeStatuses.TryGetStatus(item.Model.Id, out var value)
                                ? value
                                : PersistenceItemOutcomeStatus.Persisted;

                            return new PersistenceItemOutcome(
                                item.Model.Id,
                                item.StagedVersionToken,
                                item.GlobalEventPosition,
                                status,
                                null);
                        })
                    .ToArray();
    }

    /// <summary>
    /// Loads the persisted checkpoint for the specified model name.
    /// </summary>

    // TODO: getcheckpoint.md
    public async Task<CheckPoint> GetCheckpoint(CancellationToken cancellationToken)
    {
        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        var checkPoint = await transientContext!.CheckPoint.FindAsync([this.modelName,], cancellationToken);

        return checkPoint
               ?? new CheckPoint
               {
                   ModelName = this.modelName,
                   GlobalEventPosition = new GlobalEventPosition(0),
               };
    }

    /// <summary>
    /// Persists the checkpoint for the specified model name.
    /// </summary>
    public async Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        var currentCheckpoint = await transientContext!
                                      .CheckPoint
                                      .Where(x => x.ModelName == checkPoint.ModelName)
                                      .SingleOrDefaultAsync(cancellationToken);

        if (currentCheckpoint == null)
        {
            transientContext.Add(checkPoint);
        }
        else
        {
            currentCheckpoint.GlobalEventPosition = checkPoint.GlobalEventPosition;
        }

        await transientContext.SaveChangesAsync(cancellationToken);
    }

    private static bool IsNpgsqlProvider(DbContext dbContext)
    {
        var providerName = dbContext.Database.ProviderName;
        return providerName?.Contains("Npgsql", StringComparison.OrdinalIgnoreCase) == true;
    }

    private static bool IsDuplicateKeyViolation(Exception ex)
    {
        if (ex is DbUpdateException { InnerException: { } innerException, })
        {
            return IsDuplicateKeyViolation(innerException);
        }

        return ex switch
               {
                   SqlException { Number: DuplicatePkViolationError, }                => true,
                   PostgresException { SqlState: PostgresDuplicatePkViolationError, } => true,
                   _                                                                  => false,
               };
    }

    private async Task<PersistenceOutcomeCollector> PersistBatchInternal(ModelState<TState>[] modelStates, CancellationToken cancellationToken)
    {
        var outcomes = new PersistenceOutcomeCollector(modelStates.Length);
        var toInsert = new List<ModelState<TState>>(modelStates.Length);
        var toUpdate = new List<ModelState<TState>>();

        foreach (var modelState in modelStates)
        {
            if (modelState.IsNew)
            {
                toInsert.Add(modelState);
                continue;
            }

            toUpdate.Add(modelState);
        }

        await this.DoBulkInserts(toInsert, outcomes, cancellationToken);
        await this.SaveUpdates(toUpdate, outcomes, cancellationToken);

        return outcomes;
    }

    private async Task DoBulkInserts(
        List<ModelState<TState>> modelStates,
        PersistenceOutcomeCollector outcomes,
        CancellationToken cancellationToken)
    {
        if (modelStates.Count == 0)
        {
            return;
        }

        var models = modelStates.Select(x => x.Model).ToList();
        var shouldFallBackToSingleInserts = false;
        this.insertStopWatch.Restart();

        using var transientScope = this.serviceProvider.CreateScope();

        await using (var transientContext = transientScope
                                            .ServiceProvider
                                            .GetRequiredService<TDataContext>())
        {
            var strategy = transientContext!.Database.CreateExecutionStrategy();

            await strategy.ExecuteAsync(
                async () =>
                {
                    await using var transaction = await transientContext!.Database.BeginTransactionAsync(cancellationToken);

                    try
                    {
                        await transientContext.AddRangeAsync(models, cancellationToken);
                        await this.SaveModelChangesAsync(transientContext, cancellationToken);
                        await transaction.CommitAsync(cancellationToken);
                        this.insertStopWatch.Stop();

                        this.logger.LogDebug(
                            "Inserted {ModelCount} models in {SqlExecutionTime} milliseconds for {Model}",
                            models.Count,
                            this.insertStopWatch.ElapsedMilliseconds.ToString("N0"),
                            this.modelName);

                        foreach (var model in models)
                        {
                            outcomes.Record(model.Id, PersistenceItemOutcomeStatus.Persisted);
                        }

                        this.insertStopWatch.Reset();
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync(cancellationToken);

                        shouldFallBackToSingleInserts = true;
                        this.logger.LogWarning(ex, "Batch insert failed, falling back to single inserts...");
                    }
                });
        }

        if (shouldFallBackToSingleInserts)
        {
            await this.InsertOneAtTheTime(modelStates, outcomes, cancellationToken);
        }
    }

    private async Task InsertOneAtTheTime(
        IEnumerable<ModelState<TState>> modelStates,
        PersistenceOutcomeCollector outcomes,
        CancellationToken cancellationToken)
    {
        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        foreach (var modelState in modelStates)
        {
            var model = modelState.Model;

            try
            {
                transientContext!.Attach(model);
                transientContext!.Entry(model).State = EntityState.Added;
                await this.SaveModelChangesAsync(transientContext, cancellationToken);
                outcomes.Record(model.Id, PersistenceItemOutcomeStatus.Persisted);
            }
            catch (Exception ex) when (IsDuplicateKeyViolation(ex))
            {
                // Replay may try to insert an already-persisted model when checkpoint lags behind sink writes.
                transientContext!.Entry(model).State = EntityState.Detached;
                this.logger.LogDebug(
                    ex,
                    "Skipping duplicate insert for {Model} {ModelId}",
                    this.modelName,
                    model.Id);
                outcomes.Record(model.Id, PersistenceItemOutcomeStatus.SkippedAsStale);
            }
            catch (Exception ex)
            {
                this.logger.LogWarning(ex, "model {Model} failed to insert", this.modelName);

                var failure = new ProjectionFailure(
                    modelId: model.Id,
                    exception: ex.ToString(),
                    failureType: nameof(FailureType.Persistence),
                    eventNumber: model.EventNumber!.Value,
                    streamPosition: model.GlobalEventPosition,
                    modelName: this.modelName,
                    failureCreatedOn: DateTime.Now.ToUniversalTime());

                await this.failureHandler.Handle(failure, cancellationToken);

                transientContext!.Entry(model).State = EntityState.Detached;
                outcomes.Record(model.Id, PersistenceItemOutcomeStatus.Failed);
            }
        }
    }

    private async Task SaveUpdates(
        List<ModelState<TState>> projections,
        PersistenceOutcomeCollector outcomes,
        CancellationToken cancellationToken)
    {
        if (projections.Count == 0)
        {
            return;
        }

        var pending = projections.ToList();
        this.ApplyPendingEntries(pending);
        this.updateStopWatch.Restart();

        var strategy = this.DbContext.Database.CreateExecutionStrategy();

        await strategy.ExecuteAsync(
            async () =>
            {
                await using var transaction = await this.DbContext.Database.BeginTransactionAsync(cancellationToken);

                try
                {
                    await this.SaveModelChangesAsync(this.DbContext, cancellationToken);

                    await transaction.CommitAsync(cancellationToken);
                    this.updateStopWatch.Stop();

                    this.logger.LogDebug(
                        "Updated {ModelCount} models in {SqlExecutionTime} milliseconds for {Model}",
                        pending.Count,
                        this.updateStopWatch.ElapsedMilliseconds.ToString("N0"),
                        this.modelName);

                    foreach (var projection in pending)
                    {
                        outcomes.Record(projection.Model.Id, PersistenceItemOutcomeStatus.Persisted);
                    }
                }
                catch (DbUpdateConcurrencyException cex)
                {
                    await transaction.RollbackAsync(cancellationToken);

                    // Treat optimistic concurrency misses as benign replay/stale updates.
                    var skipped = 0;

                    foreach (var failedEntry in cex.Entries)
                    {
                        var model = failedEntry.Entity as TState;

                        if (model is null)
                        {
                            continue;
                        }

                        skipped++;
                        pending.RemoveAll(x => x.Model.Id == model.Id);
                        failedEntry.State = EntityState.Detached;
                        outcomes.Record(model.Id, PersistenceItemOutcomeStatus.SkippedAsStale);
                    }

                    this.logger.LogDebug(
                        cex,
                        "Skipped {SkippedModelCount} stale projection changes for {Model}",
                        skipped,
                        this.modelName);

                    if (pending.Count > 0)
                    {
                        this.ApplyPendingEntries(pending);
                        await this.SaveUpdates(pending, outcomes, cancellationToken);
                    }
                }
                catch (DbUpdateException dbex)
                {
                    await transaction.RollbackAsync(cancellationToken);

                    if (dbex.Entries.Count > 0)
                    {
                        foreach (var failedEntry in dbex.Entries)
                        {
                            var model = failedEntry.Entity as TState;

                            if (model is null)
                            {
                                continue;
                            }

                            pending.RemoveAll(x => x.Model.Id == model.Id);

                            this.logger.LogWarning(
                                dbex,
                                "Failed to update stream projection {ModelName} {@Model}",
                                this.modelName,
                                (TState)failedEntry.Entity);

                            failedEntry.State = EntityState.Detached;

                            var failure = new ProjectionFailure(
                                modelId: model.Id,
                                exception: dbex.ToString(),
                                failureType: nameof(FailureType.Persistence),
                                eventNumber: model.EventNumber!.Value,
                                streamPosition: model.GlobalEventPosition,
                                modelName: this.modelName,
                                failureCreatedOn: DateTime.Now.ToUniversalTime());

                            this.pendingUpdateFailures.Add(failure);
                            outcomes.Record(model.Id, PersistenceItemOutcomeStatus.Failed);
                        }
                    }
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync(cancellationToken);

                    this.logger.LogError(
                        ex,
                        "Failed to save updates for stream projection {Model}, number Of Models impacted {Modelcount}",
                        this.modelName,
                        pending.Count);

                    this.DisposeScope();

                    foreach (var model in pending.Select(x => x.Model))
                    {
                        this.logger.LogWarning(
                            ex,
                            "Failed to update stream projection {Model} {Modelid}",
                            this.modelName,
                            model.Id);

                        var failure = new ProjectionFailure(
                            modelId: model.Id,
                            exception: ex.ToString(),
                            failureType: nameof(FailureType.Persistence),
                            eventNumber: model.EventNumber!.Value,
                            streamPosition: model.GlobalEventPosition,
                            modelName: this.modelName,
                            failureCreatedOn: DateTime.Now.ToUniversalTime());

                        this.pendingUpdateFailures.Add(failure);
                        outcomes.Record(model.Id, PersistenceItemOutcomeStatus.Failed);
                    }

                    pending.Clear();
                }
                finally
                {
                    this.updateStopWatch.Reset();
                }

                if (this.pendingUpdateFailures.Count > 0)
                {
                    foreach (var failure in this.pendingUpdateFailures)
                    {
                        await this.failureHandler.Handle(failure, cancellationToken);
                    }

                    this.pendingUpdateFailures.Clear();

                    if (pending.Count > 0)
                    {
                        this.ApplyPendingEntries(pending);
                        await this.SaveUpdates(pending, outcomes, cancellationToken);
                    }
                }
            });
    }

    private void ApplyPendingEntries(IReadOnlyCollection<ModelState<TState>> pending)
    {
        foreach (var (model, _, shouldDelete, _, expectedEventNumber) in pending)
        {
            var entry = this.DbContext.Entry(model);

            if (entry.State == EntityState.Detached)
            {
                this.DbContext.Attach(model);
                entry = this.DbContext.Entry(model);
            }

            entry.Property(nameof(IModel.EventNumber)).OriginalValue = expectedEventNumber;

            if (shouldDelete)
            {
                entry.State = EntityState.Deleted;
                continue;
            }

            entry.State = EntityState.Modified;

            if (this.hasChildEntities)
            {
                this.ApplyChildEntityStates(entry, new HashSet<object>(ReferenceEqualityComparer.Instance));
            }
        }
    }

    private async Task SaveModelChangesAsync(DbContext dbContext, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        await dbContext.SaveChangesAsync(cancellationToken);
        stopwatch.Stop();

        this.cumulativeModelWriteSaveChangesMs += stopwatch.Elapsed.TotalMilliseconds;
        this.cumulativeModelWriteSaveChangesCalls++;
    }

    private void LogCumulativeModelWriteMetricsIfDue()
    {
        if (this.modelWriteMetricsLogStopwatch.Elapsed < TimeSpan.FromSeconds(10))
        {
            return;
        }

        this.logger.LogInformation(
            "Cumulative model write SaveChanges metrics for {ModelName}: saveChangesCalls={SaveChangesCalls}, cumulativeSaveChangesMs={CumulativeSaveChangesMs}",
            this.modelName,
            this.cumulativeModelWriteSaveChangesCalls,
            this.cumulativeModelWriteSaveChangesMs);

        this.modelWriteMetricsLogStopwatch.Restart();
    }

    private sealed class PersistenceOutcomeCollector
    {
        private readonly Dictionary<Guid, PersistenceItemOutcomeStatus> statuses;

        public PersistenceOutcomeCollector(int capacity)
        {
            this.statuses = new Dictionary<Guid, PersistenceItemOutcomeStatus>(capacity);
        }

        public void Record(Guid modelId, PersistenceItemOutcomeStatus status)
        {
            this.statuses[modelId] = status;
        }

        public bool TryGetStatus(Guid modelId, out PersistenceItemOutcomeStatus status)
        {
            return this.statuses.TryGetValue(modelId, out status);
        }
    }

    private bool ValidateAndDetectChildEntities()
    {
        var entityType = this.DbContext.Model.FindEntityType(typeof(TState))
                         ?? throw new InvalidOperationException($"EF model metadata for root model type '{typeof(TState).FullName}' was not found.");

        var visited = new HashSet<Type>();
        return this.ValidateAndDetectChildEntities(entityType, visited);
    }

    private bool ValidateAndDetectChildEntities(Microsoft.EntityFrameworkCore.Metadata.IEntityType entityType, HashSet<Type> visited)
    {
        if (!visited.Add(entityType.ClrType))
        {
            return false;
        }

        var hasParticipatingHierarchy = false;

        foreach (var navigation in entityType.GetNavigations())
        {
            var targetEntityType = navigation.TargetEntityType;

            if (targetEntityType.IsOwned())
            {
                hasParticipatingHierarchy |= this.ValidateAndDetectChildEntities(targetEntityType, visited);
                continue;
            }

            if (!typeof(ChildEntity).IsAssignableFrom(targetEntityType.ClrType))
            {
                throw new InvalidOperationException(
                    $"Model '{typeof(TState).FullName}' has persisted child navigation '{entityType.ClrType.Name}.{navigation.Name}' "
                    + $"targeting '{targetEntityType.ClrType.FullName}', but that child type does not inherit from '{typeof(ChildEntity).FullName}'. "
                    + $"Fix by making '{targetEntityType.ClrType.Name}' inherit from '{nameof(ChildEntity)}', or remove the persisted child navigation from this model graph.");
            }

            hasParticipatingHierarchy = true;
            hasParticipatingHierarchy |= this.ValidateAndDetectChildEntities(targetEntityType, visited);
        }

        return hasParticipatingHierarchy;
    }

    private async Task LoadNavigationGraphAsync(object root, CancellationToken cancellationToken)
    {
        await this.LoadNavigationGraphAsync(root, new HashSet<object>(ReferenceEqualityComparer.Instance), cancellationToken);
    }

    private async Task LoadNavigationGraphAsync(object node, HashSet<object> visited, CancellationToken cancellationToken)
    {
        if (!visited.Add(node))
        {
            return;
        }

        var entry = this.DbContext.Entry(node);

        foreach (var navigationEntry in entry.Navigations)
        {
            if (!navigationEntry.IsLoaded)
            {
                await navigationEntry.LoadAsync(cancellationToken);
            }

            switch (navigationEntry.CurrentValue)
            {
                case null:
                    continue;
                case System.Collections.IEnumerable children when navigationEntry.Metadata.IsCollection:
                    foreach (var child in children)
                    {
                        if (child != null)
                        {
                            await this.LoadNavigationGraphAsync(child, visited, cancellationToken);
                        }
                    }

                    break;
                default:
                    await this.LoadNavigationGraphAsync(navigationEntry.CurrentValue, visited, cancellationToken);
                    break;
            }
        }
    }

    private void MarkPersistedGraph(object root)
    {
        this.MarkPersistedGraph(root, new HashSet<object>(ReferenceEqualityComparer.Instance));
    }

    private void MarkPersistedGraph(object node, HashSet<object> visited)
    {
        if (!visited.Add(node))
        {
            return;
        }

        if (node is ChildEntity entity)
        {
            entity.PersistenceState = EntityPersistenceState.Persisted;
        }

        if (!this.hasChildEntities)
        {
            return;
        }

        var entityType = this.DbContext.Model.FindEntityType(node.GetType());

        if (entityType == null)
        {
            return;
        }

        foreach (var navigation in entityType.GetNavigations())
        {
            var value = navigation.PropertyInfo?.GetValue(node);

            switch (value)
            {
                case null:
                    continue;
                case System.Collections.IEnumerable children when navigation.IsCollection:
                    foreach (var child in children)
                    {
                        if (child != null)
                        {
                            this.MarkPersistedGraph(child, visited);
                        }
                    }

                    break;
                default:
                    this.MarkPersistedGraph(value, visited);
                    break;
            }
        }
    }

    private void ApplyChildEntityStates(EntityEntry entry, HashSet<object> visited)
    {
        if (!visited.Add(entry.Entity))
        {
            return;
        }

        foreach (var navigationEntry in entry.Navigations)
        {
            switch (navigationEntry.CurrentValue)
            {
                case null:
                    continue;
                case System.Collections.IEnumerable children when navigationEntry.Metadata.IsCollection:
                    foreach (var child in children)
                    {
                        if (child != null)
                        {
                            this.ApplyEntityState(child, visited);
                        }
                    }

                    break;
                default:
                    this.ApplyEntityState(navigationEntry.CurrentValue, visited);
                    break;
            }
        }
    }

    private void ApplyEntityState(object entity, HashSet<object> visited)
    {
        if (!visited.Add(entity))
        {
            return;
        }

        var entry = this.DbContext.Entry(entity);

        if (entry.State == EntityState.Detached)
        {
            this.DbContext.Attach(entity);
            entry = this.DbContext.Entry(entity);
        }

        if (entity is ChildEntity persistedEntity)
        {
            entry.State = persistedEntity.PersistenceState switch
                          {
                              EntityPersistenceState.New     => EntityState.Added,
                              EntityPersistenceState.Deleted => EntityState.Deleted,
                              _                              => EntityState.Modified,
                          };
        }
        else
        {
            entry.State = EntityState.Modified;
        }

        this.ApplyChildEntityStates(entry, visited);
    }

    private void InitDbContext()
    {
        this.scope = this.serviceProvider.CreateScope();
        this.dbContext = this.scope.ServiceProvider.GetRequiredService<TDataContext>();
    }

    private void DisposeScope()
    {
        this.scope?.Dispose();
        this.scope = null;
    }
}
