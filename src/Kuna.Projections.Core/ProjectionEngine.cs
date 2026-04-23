using System.Collections.Concurrent;
using Kuna.Projections.Abstractions.Exceptions;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Core;

/// <summary>
/// Core stateful runtime engine that owns live projection instances, recreates
/// them from cache or store as needed, applies incoming envelopes, and tracks
/// failed models across a flush window.
/// </summary>
internal sealed class ProjectionEngine<TState>
    : IModelStateTransformer<EventEnvelope, TState>,
      IProjectionRuntimeStats,
      IProjectionLifecycle
    where TState : class, IModel, new()
{
    private readonly IProjectionFactory<TState> projectionFactory;
    private readonly IProjectionFailureHandler<TState> failureHandler;
    private readonly IModelStateCache<TState> modelStateCache;
    private readonly IProjectionSettings<TState> settings;
    private readonly ILogger logger;
    private readonly string modelName;
    private readonly ConcurrentDictionary<Guid, Projection<TState>> projections;
    private readonly ConcurrentDictionary<Guid, byte> failedProjections;
    private long runtimeProjectionHits;
    private long cacheProjectionRestores;
    private long storeProjectionLoads;
    private long storeProjectionMisses;
    private long newProjectionCreates;

    public ProjectionEngine(
        IProjectionFactory<TState> projectionFactory,
        IProjectionFailureHandler<TState> failureHandler,
        IModelStateCache<TState> modelStateCache,
        IProjectionSettings<TState> settings,
        ILogger<ProjectionEngine<TState>> logger)
    {
        this.projectionFactory = projectionFactory;
        this.failureHandler = failureHandler;
        this.modelStateCache = modelStateCache;
        this.settings = settings;
        this.logger = logger;
        this.modelName = ProjectionModelName.For<TState>();
        this.projections = new ConcurrentDictionary<Guid, Projection<TState>>(Environment.ProcessorCount, settings.MaxPendingProjectionsCount);
        this.failedProjections = new ConcurrentDictionary<Guid, byte>(Environment.ProcessorCount, settings.MaxPendingProjectionsCount);
    }

    /// <summary>
    /// Transforms one event envelope into the next model state for its target
    /// model by reusing or recreating the live projection instance, applying the
    /// event, and returning the resulting state snapshot for batching and
    /// persistence. Returns <see langword="null"/> when processing for the model
    /// should be skipped for this envelope.
    /// </summary>
    public async ValueTask<ModelState<TState>?> Transform(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        if (!this.failedProjections.IsEmpty
            && this.failedProjections.ContainsKey(envelope.ModelId))
        {
            return null;
        }

        var projection = await this.GetOrCreateProjection(envelope, cancellationToken);

        if (projection == null)
        {
            return null;
        }

        try
        {
            var expectedEventNumber = projection.ModelState.EventNumber;

            if (!projection.Process(envelope, this.settings.EventVersionCheckStrategy))
            {
                return null;
            }

            this.projections[projection.ModelState.Id] = projection;

            return new ModelState<TState>(
                projection.ModelState,
                projection.IsNew,
                projection.ShouldDelete,
                envelope.GlobalEventPosition,
                expectedEventNumber);
        }
        catch (EventOutOfOrderException ex)
        {
            if (this.failedProjections.TryAdd(envelope.ModelId, 0))
            {
                var failure = new ProjectionFailure(
                    modelId: envelope.ModelId,
                    eventNumber: envelope.EventNumber,
                    streamPosition: envelope.GlobalEventPosition,
                    failureCreatedOn: DateTime.Now.ToUniversalTime(),
                    exception: ex.Message,
                    failureType: FailureType.EventOutOfOrder.ToString(),
                    modelName: this.modelName);

                await this.failureHandler.Handle(failure, cancellationToken);

                this.logger.LogError(
                    ex,
                    "Projection failed to process, event out of order in {StreamId}, event {EventNumber} for model {ModelState}",
                    envelope.StreamId,
                    envelope.EventNumber,
                    this.modelName);
            }

            throw;
        }
        catch (Exception ex)
        {
            this.logger.LogError(
                ex,
                "Projection failed to process in {StreamId}, event {EventNumber} for model {ModelState}",
                envelope.StreamId,
                envelope.EventNumber,
                this.modelName);

            throw;
        }
    }

    /// <summary>
    /// Clears live in-memory runtime state for all models that participated in a
    /// successfully completed flush window, including failed-model markers.
    /// </summary>
    public void OnFlushSucceeded(
        IReadOnlyCollection<Guid> flushedModelIds,
        IReadOnlyCollection<Guid> clearModelIds,
        IReadOnlyDictionary<Guid, long?> flushedEventNumbers)
    {
        foreach (var modelId in flushedModelIds)
        {
            if (this.projections.TryGetValue(modelId, out var projection))
            {
                projection.IsNew = false;
            }
        }

        foreach (var modelId in clearModelIds)
        {
            if (this.projections.TryGetValue(modelId, out var projection)
                && flushedEventNumbers.TryGetValue(modelId, out var flushedEventNumber)
                && projection.ModelState.EventNumber > flushedEventNumber)
            {
                continue;
            }

            this.projections.TryRemove(modelId, out _);
            this.failedProjections.TryRemove(modelId, out _);
        }
    }

    /// <summary>
    /// Resets all live projection instances and failed-model tracking held by
    /// this engine. Intended for tests or full runtime reset scenarios.
    /// </summary>
    public void ClearAll()
    {
        this.projections.Clear();
        this.failedProjections.Clear();
    }

    public ProjectionRuntimeStats ReadAndResetRuntimeStats()
    {
        return new ProjectionRuntimeStats(
            Interlocked.Exchange(ref this.runtimeProjectionHits, 0),
            Interlocked.Exchange(ref this.cacheProjectionRestores, 0),
            Interlocked.Exchange(ref this.storeProjectionLoads, 0),
            Interlocked.Exchange(ref this.storeProjectionMisses, 0),
            Interlocked.Exchange(ref this.newProjectionCreates, 0));
    }

    private async ValueTask<Projection<TState>?> GetOrCreateProjection(
        EventEnvelope envelope,
        CancellationToken cancellationToken)
    {
        if (this.projections.TryGetValue(envelope.ModelId, out var projection))
        {
            Interlocked.Increment(ref this.runtimeProjectionHits);
            return projection;
        }

        if (this.modelStateCache.TryGet(envelope.ModelId, out var cached)
            && cached != null)
        {
            Interlocked.Increment(ref this.cacheProjectionRestores);
            projection = this.projectionFactory.CreateFromModel(cached.Model, cached.IsNew);
            this.projections[envelope.ModelId] = projection;
            return projection;
        }

        var loadModelFromStore = envelope.EventNumber > 0;

        if (loadModelFromStore)
        {
            Interlocked.Increment(ref this.storeProjectionLoads);
        }
        else
        {
            Interlocked.Increment(ref this.newProjectionCreates);
        }

        projection = await this.projectionFactory.Create(envelope.ModelId, loadModelFromStore, cancellationToken);

        if (projection != null)
        {
            return projection;
        }

        if (loadModelFromStore)
        {
            Interlocked.Increment(ref this.storeProjectionMisses);
        }

        this.logger.LogWarning(
            "Projection state not found in data store for {StreamId}, event {EventNumber} for model {ModelState}",
            envelope.StreamId,
            envelope.EventNumber,
            this.modelName);

        if (this.settings.SkipStateNotFoundFailure)
        {
            return null;
        }

        var ex = $"Projection state not found in data store for {envelope.StreamId}, event {envelope.EventNumber}";

        var failure = new ProjectionFailure(
            modelId: envelope.ModelId,
            eventNumber: envelope.EventNumber,
            streamPosition: envelope.GlobalEventPosition,
            failureCreatedOn: DateTime.Now.ToUniversalTime(),
            exception: ex,
            failureType: FailureType.EventProcessing.ToString(),
            modelName: this.modelName);

        await this.failureHandler.Handle(failure, cancellationToken);

        this.failedProjections.TryAdd(envelope.ModelId, 0);

        return null;
    }
}
