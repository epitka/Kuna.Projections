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

    /// <summary>
    /// Initializes the EF-backed model store.
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

        this.pendingUpdateFailures = new List<ProjectionFailure>();
        this.hasChildEntities = this.ValidateAndDetectChildEntities();

        this.insertStopWatch = new Stopwatch();
        this.updateStopWatch = new Stopwatch();
    }

    /// <summary>
    /// Loads the persisted model state for the specified model id.
    /// </summary>
    public async Task<TState?> Load(Guid modelId, CancellationToken cancellationToken)
    {
        try
        {
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = transientScope
                                               .ServiceProvider
                                               .GetRequiredService<TDataContext>();

            var model = await transientContext.FindAsync<TState>([modelId,], cancellationToken);

            if (model != null
                && this.hasChildEntities)
            {
                await this.LoadNavigationGraphAsync(transientContext, model, cancellationToken);
            }

            if (model != null
                && this.hasChildEntities)
            {
                this.MarkPersistedGraph(transientContext, model);
            }

            return model;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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
        var persistStopwatch = Stopwatch.StartNew();
        var phaseStopwatch = Stopwatch.StartNew();

        // let's remove items that are both IsNew and ShouldDelete
        // no reason to insert them and immediately delete them
        var toExclude = batch.Changes
                             .Where(x => x is { IsNew: true, ShouldDelete: true, });

        var toPersist = batch.Changes.Except(toExclude).ToArray();
        var excludedCount = batch.Changes.Count - toPersist.Length;
        var insertCount = toPersist.Count(x => x.IsNew);
        var updateCount = toPersist.Count(x => !x.IsNew);

        var checkpoint = new CheckPoint
        {
            ModelName = this.modelName,
            GlobalEventPosition = batch.GlobalEventPosition,
        };

        PersistBatchTimings sinkTimings;

        try
        {
            sinkTimings = await this.PersistBatchInternal(toPersist, checkpoint, cancellationToken);
        }
        catch (Exception ex) when (ex is DbUpdateException or DbUpdateConcurrencyException)
        {
            this.logger.LogWarning(
                ex,
                "Unified batch persist failed for {Model}, falling back to isolated persistence handling...",
                this.modelName);

            await this.PersistBatchInternal(toPersist, cancellationToken);
            await this.PersistCheckpoint(checkpoint, cancellationToken);
            sinkTimings = default;
        }

        var modelPersistMs = phaseStopwatch.Elapsed.TotalMilliseconds;
        var markPersistedGraphMs = 0d;

        if (this.hasChildEntities)
        {
            phaseStopwatch.Restart();
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = transientScope
                                               .ServiceProvider
                                               .GetRequiredService<TDataContext>();

            foreach (var modelState in toPersist.Where(x => !x.ShouldDelete))
            {
                this.MarkPersistedGraph(transientContext, modelState.Model);
            }

            markPersistedGraphMs = phaseStopwatch.Elapsed.TotalMilliseconds;
        }

        persistStopwatch.Stop();

        if (this.logger.IsEnabled(LogLevel.Debug))
        {
            this.logger.LogDebug(
                "Projection EF sink persisted for {Model}: batchChanges={BatchChanges}, persisted={Persisted}, inserted={Inserted}, updated={Updated}, excluded={Excluded}, modelPersistMs={ModelPersistMs:F0}, insertMs={InsertMs:F0}, updateMs={UpdateMs:F0}, checkpointMs={CheckpointMs:F0}, commitMs={CommitMs:F0}, markPersistedGraphMs={MarkPersistedGraphMs:F0}, totalMs={TotalMs:F0}",
                this.modelName,
                batch.Changes.Count,
                toPersist.Length,
                insertCount,
                updateCount,
                excludedCount,
                modelPersistMs,
                sinkTimings.InsertMs,
                sinkTimings.UpdateMs,
                sinkTimings.CheckpointMs,
                sinkTimings.CommitMs,
                markPersistedGraphMs,
                persistStopwatch.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Loads the persisted checkpoint for the specified model name.
    /// </summary>
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

    private async Task<PersistBatchTimings> PersistBatchInternal(
        ModelState<TState>[] modelStates,
        CheckPoint checkPoint,
        CancellationToken cancellationToken)
    {
        var timings = new PersistBatchTimings();
        var phaseStopwatch = new Stopwatch();
        var toInsert = modelStates
                       .Where(x => x.IsNew)
                       .Select(x => x.Model)
                       .ToArray();

        var toUpdate = modelStates
                       .Where(x => !x.IsNew)
                       .ToArray();

        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        var originalAutoDetectChanges = transientContext.ChangeTracker.AutoDetectChangesEnabled;
        transientContext.ChangeTracker.AutoDetectChangesEnabled = false;

        var strategy = transientContext!.Database.CreateExecutionStrategy();

        try
        {
            await strategy.ExecuteAsync(
                async () =>
                {
                    await using var transaction = await transientContext.Database.BeginTransactionAsync(cancellationToken);

                    try
                    {
                        if (toInsert.Length > 0)
                        {
                            phaseStopwatch.Restart();
                            await transientContext.AddRangeAsync(toInsert, cancellationToken);
                            await transientContext.SaveChangesAsync(cancellationToken);
                            transientContext.ChangeTracker.Clear();
                            timings.InsertMs = phaseStopwatch.Elapsed.TotalMilliseconds;
                        }

                        if (toUpdate.Length > 0)
                        {
                            phaseStopwatch.Restart();
                            this.ApplyPendingEntries(transientContext, toUpdate);
                            await transientContext.SaveChangesAsync(cancellationToken);
                            transientContext.ChangeTracker.Clear();
                            timings.UpdateMs = phaseStopwatch.Elapsed.TotalMilliseconds;
                        }

                        phaseStopwatch.Restart();
                        await this.ApplyCheckpoint(transientContext, checkPoint, cancellationToken);
                        await transientContext.SaveChangesAsync(cancellationToken);
                        timings.CheckpointMs = phaseStopwatch.Elapsed.TotalMilliseconds;

                        phaseStopwatch.Restart();
                        await transaction.CommitAsync(cancellationToken);
                        timings.CommitMs = phaseStopwatch.Elapsed.TotalMilliseconds;
                    }
                    catch
                    {
                        await transaction.RollbackAsync(cancellationToken);
                        throw;
                    }
                });
        }
        finally
        {
            transientContext.ChangeTracker.AutoDetectChangesEnabled = originalAutoDetectChanges;
        }

        return timings;
    }

    private async Task PersistBatchInternal(ModelState<TState>[] modelStates, CancellationToken cancellationToken)
    {
        var toInsert = new List<TState>(modelStates.Length);
        var toUpdate = new List<ModelState<TState>>();

        foreach (var modelState in modelStates)
        {
            if (modelState.IsNew)
            {
                toInsert.Add(modelState.Model);
                continue;
            }

            toUpdate.Add(modelState);
        }

        await this.DoBulkInserts(toInsert, cancellationToken);
        await this.SaveUpdates(toUpdate, cancellationToken);
    }

    private async Task DoBulkInserts(List<TState> models, CancellationToken cancellationToken)
    {
        if (models.Count == 0)
        {
            return;
        }

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
                        await transientContext.SaveChangesAsync(cancellationToken);
                        await transaction.CommitAsync(cancellationToken);

                        this.insertStopWatch.Stop();

                        if (this.logger.IsEnabled(LogLevel.Trace))
                        {
                            this.logger.LogTrace(
                                "Inserted {ModelCount} models in {SqlExecutionTime} milliseconds for {Model}",
                                models.Count,
                                this.insertStopWatch.ElapsedMilliseconds.ToString("N0"),
                                this.modelName);
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
            await this.InsertOneAtTheTime(models, cancellationToken);
        }
    }

    private async Task InsertOneAtTheTime(IEnumerable<TState> models, CancellationToken cancellationToken)
    {
        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        foreach (var model in models)
        {
            try
            {
                transientContext!.Attach(model);
                transientContext!.Entry(model).State = EntityState.Added;
                await transientContext.SaveChangesAsync(cancellationToken);
            }
            catch (Exception ex) when (IsDuplicateKeyViolation(ex))
            {
                // Replay may try to insert an already-persisted model when checkpoint lags behind sink writes.
                transientContext!.Entry(model).State = EntityState.Detached;

                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug(
                        ex,
                        "Skipping duplicate insert for {Model} {ModelId}",
                        this.modelName,
                        model.Id);
                }
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
            }
        }
    }

    private async Task SaveUpdates(List<ModelState<TState>> projections, CancellationToken cancellationToken)
    {
        if (projections.Count == 0)
        {
            return;
        }

        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        await this.SaveUpdates(transientContext, projections, cancellationToken);
    }

    private async Task SaveUpdates(TDataContext dbContext, List<ModelState<TState>> projections, CancellationToken cancellationToken)
    {
        var pending = projections.ToList();
        this.ApplyPendingEntries(dbContext, pending);
        this.updateStopWatch.Restart();

        var strategy = dbContext.Database.CreateExecutionStrategy();

        await strategy.ExecuteAsync(
            async () =>
            {
                await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);

                try
                {
                    await dbContext.SaveChangesAsync(cancellationToken);

                    await transaction.CommitAsync(cancellationToken);
                    this.updateStopWatch.Stop();

                    if (this.logger.IsEnabled(LogLevel.Trace))
                    {
                        this.logger.LogTrace(
                            "Updated {ModelCount} models in {SqlExecutionTime} milliseconds for {Model}",
                            pending.Count,
                            this.updateStopWatch.ElapsedMilliseconds.ToString("N0"),
                            this.modelName);
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
                    }

                    if (this.logger.IsEnabled(LogLevel.Debug))
                    {
                        this.logger.LogDebug(
                            cex,
                            "Skipped {SkippedModelCount} stale projection changes for {Model}",
                            skipped,
                            this.modelName);
                    }

                    if (pending.Count > 0)
                    {
                        this.ApplyPendingEntries(dbContext, pending);
                        await this.SaveUpdates(dbContext, pending, cancellationToken);
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
                        this.ApplyPendingEntries(dbContext, pending);
                        await this.SaveUpdates(dbContext, pending, cancellationToken);
                    }
                }
            });
    }

    private void ApplyPendingEntries(DbContext dbContext, IReadOnlyCollection<ModelState<TState>> pending)
    {
        foreach (var (model, isNew, shouldDelete, _, expectedEventNumber) in pending)
        {
            if (isNew
                && !shouldDelete)
            {
                dbContext.Add(model);
                continue;
            }

            var entry = dbContext.Entry(model);

            if (entry.State == EntityState.Detached)
            {
                dbContext.Attach(model);
                entry = dbContext.Entry(model);
            }

            if (shouldDelete)
            {
                entry.Property(nameof(IModel.EventNumber)).OriginalValue = expectedEventNumber;
                entry.State = EntityState.Deleted;
                continue;
            }

            entry.Property(nameof(IModel.EventNumber)).OriginalValue = expectedEventNumber;
            entry.State = EntityState.Modified;

            if (this.hasChildEntities)
            {
                this.ApplyChildEntityStates(dbContext, entry, new HashSet<object>(ReferenceEqualityComparer.Instance));
            }
        }
    }

    private async Task ApplyCheckpoint(
        DbContext dbContext,
        CheckPoint checkPoint,
        CancellationToken cancellationToken)
    {
        var currentCheckpoint = await dbContext.Set<CheckPoint>()
                                               .Where(x => x.ModelName == checkPoint.ModelName)
                                               .SingleOrDefaultAsync(cancellationToken);

        if (currentCheckpoint == null)
        {
            dbContext.Add(checkPoint);
            return;
        }

        currentCheckpoint.GlobalEventPosition = checkPoint.GlobalEventPosition;
        dbContext.Entry(currentCheckpoint)
                 .Property(nameof(CheckPoint.GlobalEventPosition))
                 .IsModified = true;
    }

    private bool ValidateAndDetectChildEntities()
    {
        using var transientScope = this.serviceProvider.CreateScope();

        using var transientContext = transientScope
                                     .ServiceProvider
                                     .GetRequiredService<TDataContext>();

        var entityType = transientContext.Model.FindEntityType(typeof(TState))
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

    private async Task LoadNavigationGraphAsync(DbContext dbContext, object root, CancellationToken cancellationToken)
    {
        await this.LoadNavigationGraphAsync(dbContext, root, new HashSet<object>(ReferenceEqualityComparer.Instance), cancellationToken);
    }

    private async Task LoadNavigationGraphAsync(
        DbContext dbContext,
        object node,
        HashSet<object> visited,
        CancellationToken cancellationToken)
    {
        if (!visited.Add(node))
        {
            return;
        }

        var entry = dbContext.Entry(node);

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
                            await this.LoadNavigationGraphAsync(dbContext, child, visited, cancellationToken);
                        }
                    }

                    break;
                default:
                    await this.LoadNavigationGraphAsync(dbContext, navigationEntry.CurrentValue, visited, cancellationToken);
                    break;
            }
        }
    }

    private void MarkPersistedGraph(DbContext dbContext, object root)
    {
        this.MarkPersistedGraph(dbContext, root, new HashSet<object>(ReferenceEqualityComparer.Instance));
    }

    private void MarkPersistedGraph(DbContext dbContext, object node, HashSet<object> visited)
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

        var entityType = dbContext.Model.FindEntityType(node.GetType());

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
                            this.MarkPersistedGraph(dbContext, child, visited);
                        }
                    }

                    break;
                default:
                    this.MarkPersistedGraph(dbContext, value, visited);
                    break;
            }
        }
    }

    private void ApplyChildEntityStates(DbContext dbContext, EntityEntry entry, HashSet<object> visited)
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
                            this.ApplyEntityState(dbContext, child, visited);
                        }
                    }

                    break;
                default:
                    this.ApplyEntityState(dbContext, navigationEntry.CurrentValue, visited);
                    break;
            }
        }
    }

    private void ApplyEntityState(DbContext dbContext, object entity, HashSet<object> visited)
    {
        if (!visited.Add(entity))
        {
            return;
        }

        var entry = dbContext.Entry(entity);

        if (entry.State == EntityState.Detached)
        {
            dbContext.Attach(entity);
            entry = dbContext.Entry(entity);
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

        this.ApplyChildEntityStates(dbContext, entry, visited);
    }

    private struct PersistBatchTimings
    {
        public double InsertMs { get; set; }

        public double UpdateMs { get; set; }

        public double CheckpointMs { get; set; }

        public double CommitMs { get; set; }
    }
}
