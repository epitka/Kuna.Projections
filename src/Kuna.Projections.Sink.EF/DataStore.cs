using System.Diagnostics;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Sink.EF;

/// <summary>
/// EF Core-backed implementation of model-state persistence, model-state
/// loading, and explicit checkpoint persistence for the projection pipeline.
/// </summary>
public class DataStore<TState, TDataContext>
    : IModelStateSink<TState>,
      IModelStateStore<TState>,
      ICheckpointStore
    where TState : class, IModel, new()
    where TDataContext : DbContext, IProjectionDbContext
{
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
    /// Persists one batch of model-state changes.
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

        PersistBatchTimings sinkTimings;

        try
        {
            sinkTimings = await this.PersistBatchInternal(toPersist, cancellationToken);
        }
        catch (DbUpdateException ex) when (this.IsDuplicateKeyViolation(ex))
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogDebug(
                    "Unified batch persist hit duplicate keys for {Model}, falling back to isolated persistence handling...",
                    this.modelName);
            }

            await this.PersistBatchIsolated(toPersist, cancellationToken);
            sinkTimings = default;
        }
        catch (Exception ex) when (ex is DbUpdateException or DbUpdateConcurrencyException)
        {
            this.logger.LogWarning(
                ex,
                "Unified batch persist failed for {Model}, falling back to isolated persistence handling...",
                this.modelName);

            await this.PersistBatchIsolated(toPersist, cancellationToken);
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
                "Projection EF sink persisted for {Model}: batchChanges={BatchChanges}, persisted={Persisted}, inserted={Inserted}, updated={Updated}, excluded={Excluded}, modelPersistMs={ModelPersistMs:F0}, insertMs={InsertMs:F0}, updateMs={UpdateMs:F0}, commitMs={CommitMs:F0}, markPersistedGraphMs={MarkPersistedGraphMs:F0}, totalMs={TotalMs:F0}",
                this.modelName,
                batch.Changes.Count,
                toPersist.Length,
                insertCount,
                updateCount,
                excludedCount,
                modelPersistMs,
                sinkTimings.InsertMs,
                sinkTimings.UpdateMs,
                sinkTimings.CommitMs,
                markPersistedGraphMs,
                persistStopwatch.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Loads the persisted checkpoint for the specified model name.
    /// </summary>
    public async Task<CheckPoint> GetCheckpoint(string modelName, CancellationToken cancellationToken)
    {
        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        var checkPoint = await transientContext!.CheckPoint.FindAsync([modelName,], cancellationToken);

        return checkPoint
               ?? new CheckPoint
               {
                   ModelName = modelName,
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

    private bool IsDuplicateKeyViolation(Exception ex)
    {
        return this.serviceProvider.GetServices<IDuplicateKeyExceptionDetector>()
                   .Any(detector => detector.IsDuplicateKeyViolation(ex));
    }

    private async Task<PersistBatchTimings> PersistBatchInternal(
        ModelState<TState>[] modelStates,
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

    private async Task PersistBatchIsolated(
        ModelState<TState>[] modelStates,
        CancellationToken cancellationToken)
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

        await this.InsertOneAtTheTime(toInsert, cancellationToken);
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
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        await transaction.RollbackAsync(CancellationToken.None);
                        throw;
                    }
                    catch (DbUpdateException ex) when (IsDuplicateKeyViolation(ex))
                    {
                        await transaction.RollbackAsync(CancellationToken.None);

                        cancellationToken.ThrowIfCancellationRequested();

                        shouldFallBackToSingleInserts = true;

                        if (this.logger.IsEnabled(LogLevel.Debug))
                        {
                            this.logger.LogDebug(
                                "Batch insert hit duplicate keys for {Model}; falling back to single inserts.",
                                this.modelName);
                        }
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync(CancellationToken.None);

                        cancellationToken.ThrowIfCancellationRequested();

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
        foreach (var model in models)
        {
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = transientScope
                                               .ServiceProvider
                                               .GetRequiredService<TDataContext>();

            try
            {
                transientContext!.Attach(model);
                transientContext!.Entry(model).State = EntityState.Added;
                await transientContext.SaveChangesAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (IsDuplicateKeyViolation(ex))
            {
                await this.HandleDuplicateInsertOneAtTheTime(model, transientContext, cancellationToken);
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

    private async Task HandleDuplicateInsertOneAtTheTime(
        TState model,
        TDataContext transientContext,
        CancellationToken cancellationToken)
    {
        transientContext.Entry(model).State = EntityState.Detached;

        if (this.logger.IsEnabled(LogLevel.Debug))
        {
            this.logger.LogDebug(
                "Replacing stale persisted graph for {Model} {ModelId} after duplicate insert during replay.",
                this.modelName,
                model.Id);
        }

        await this.ReplaceOneAtTheTime(model, cancellationToken);
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

        try
        {
            await this.SaveUpdates(transientContext, projections, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is DbUpdateException or DbUpdateConcurrencyException)
        {
            this.logger.LogWarning(
                ex,
                "Batch update failed for {Model}, falling back to isolated updates...",
                this.modelName);

            await this.UpdateOneAtTheTime(projections, cancellationToken);
        }
    }

    private async Task SaveUpdates(TDataContext dbContext, List<ModelState<TState>> projections, CancellationToken cancellationToken)
    {
        this.updateStopWatch.Restart();

        var strategy = dbContext.Database.CreateExecutionStrategy();

        await strategy.ExecuteAsync(
            async () =>
            {
                await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);

                try
                {
                    this.ApplyPendingEntries(dbContext, projections);
                    await dbContext.SaveChangesAsync(cancellationToken);

                    await transaction.CommitAsync(cancellationToken);
                    this.updateStopWatch.Stop();

                    if (this.logger.IsEnabled(LogLevel.Trace))
                    {
                        this.logger.LogTrace(
                            "Updated {ModelCount} models in {SqlExecutionTime} milliseconds for {Model}",
                            projections.Count,
                            this.updateStopWatch.ElapsedMilliseconds.ToString("N0"),
                            this.modelName);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    await transaction.RollbackAsync(CancellationToken.None);
                    throw;
                }
                finally
                {
                    this.updateStopWatch.Reset();
                }
            });
    }

    private async Task UpdateOneAtTheTime(IEnumerable<ModelState<TState>> projections, CancellationToken cancellationToken)
    {
        foreach (var projection in projections)
        {
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = transientScope
                                               .ServiceProvider
                                               .GetRequiredService<TDataContext>();

            try
            {
                await this.SaveUpdates(transientContext, [projection], cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (DbUpdateConcurrencyException)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug(
                        "Skipped stale projection change for {Model} {ModelId}",
                        this.modelName,
                        projection.Model.Id);
                }
            }
            catch (DbUpdateException dbex) when (this.IsDuplicateKeyViolation(dbex))
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug(
                        "Replacing persisted graph for {Model} {ModelId} after duplicate-key update failure.",
                        this.modelName,
                        projection.Model.Id);
                }

                await this.ReplaceOneAtTheTime(projection, cancellationToken);
            }
            catch (Exception ex)
            {
                await this.HandleUpdateFailure(projection, ex, cancellationToken);
            }
        }
    }

    private async Task ReplaceOneAtTheTime(ModelState<TState> projection, CancellationToken cancellationToken)
    {
        await this.ReplaceOneAtTheTime(projection.Model, cancellationToken);
    }

    private async Task ReplaceOneAtTheTime(TState model, CancellationToken cancellationToken)
    {
        using var transientScope = this.serviceProvider.CreateScope();

        await using var transientContext = transientScope
                                           .ServiceProvider
                                           .GetRequiredService<TDataContext>();

        var strategy = transientContext.Database.CreateExecutionStrategy();

        await strategy.ExecuteAsync(
            async () =>
            {
                await using var transaction = await transientContext.Database.BeginTransactionAsync(cancellationToken);

                try
                {
                    var persisted = await transientContext.FindAsync<TState>([model.Id,], cancellationToken);

                    if (persisted != null)
                    {
                        if (this.hasChildEntities)
                        {
                            await this.LoadNavigationGraphAsync(transientContext, persisted, cancellationToken);
                        }

                        transientContext.Remove(persisted);
                        await transientContext.SaveChangesAsync(cancellationToken);
                        transientContext.ChangeTracker.Clear();
                    }

                    transientContext.Add(model);
                    await transientContext.SaveChangesAsync(cancellationToken);

                    await transaction.CommitAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    await transaction.RollbackAsync(CancellationToken.None);
                    throw;
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync(CancellationToken.None);
                    await this.HandleModelPersistenceFailure(model, ex, cancellationToken);
                }
            });
    }

    private async Task HandleUpdateFailure(ModelState<TState> projection, Exception ex, CancellationToken cancellationToken)
    {
        await this.HandleModelPersistenceFailure(projection.Model, ex, cancellationToken);
    }

    private async Task HandleModelPersistenceFailure(TState model, Exception ex, CancellationToken cancellationToken)
    {
        this.logger.LogWarning(
            ex,
            "Failed to persist stream projection {ModelName} {@Model}",
            this.modelName,
            model);

        var failure = new ProjectionFailure(
            modelId: model.Id,
            exception: ex.ToString(),
            failureType: nameof(FailureType.Persistence),
            eventNumber: model.EventNumber!.Value,
            streamPosition: model.GlobalEventPosition,
            modelName: this.modelName,
            failureCreatedOn: DateTime.Now.ToUniversalTime());

        await this.failureHandler.Handle(failure, cancellationToken);
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

        public double CommitMs { get; set; }
    }
}
