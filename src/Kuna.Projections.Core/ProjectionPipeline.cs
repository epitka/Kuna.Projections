using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MessagePack;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Kuna.Projections.Core;

/// <summary>
/// Orchestrates end-to-end projection processing for a model state type. It reads
/// envelopes from the event source, transforms them into model-state changes,
/// batches and persists those changes through the sink, advances the checkpoint,
/// publishes flushed model states into the in-memory cache, and clears live
/// runtime projection state for the flushed model ids.
/// It also implements bounded back-pressure so source reads can continue ahead of
/// persistence until the configured handoff limits are reached.
/// </summary>
public class ProjectionPipeline<TEnvelope, TState> : IProjectionPipeline<TState>
    where TEnvelope : IEventEnvelope
    where TState : class, IModel, new()
{
    private readonly string modelName;
    private readonly IEventSource<TEnvelope> source;
    private readonly IModelStateTransformer<TEnvelope, TState> transformer;
    private readonly IProjectionLifecycle lifecycle;
    private readonly IProjectionCache<TState> projectionCache;
    private readonly IProjectionStoreWriter<TState> storeWriter;
    private readonly ICheckpointStore checkpointStore;
    private readonly IProjectionSettings<TState> settings;
    private readonly ILogger logger;

    internal ProjectionPipeline(
        IEventSource<TEnvelope> source,
        IModelStateTransformer<TEnvelope, TState> transformer,
        IProjectionLifecycle lifecycle,
        IProjectionCache<TState> projectionCache,
        IProjectionStoreWriter<TState> storeWriter,
        ICheckpointStore checkpointStore,
        IProjectionSettings<TState> settings,
        ILogger<ProjectionPipeline<TEnvelope, TState>> logger)
    {
        this.source = source;
        this.transformer = transformer;
        this.lifecycle = lifecycle;
        this.projectionCache = projectionCache;
        this.storeWriter = storeWriter;
        this.checkpointStore = checkpointStore;
        this.settings = settings;
        this.logger = logger;
        this.modelName = ProjectionModelName.For<TState>();
    }

    private enum RawSignalKind
    {
        Event = 0,
        Tick = 1,
        CaughtUp = 2,
        Complete = 3,
    }

    /// <summary>
    /// Runs the projection pipeline from the last persisted checkpoint until the
    /// source completes or cancellation is requested. While running, it buffers
    /// source envelopes, flushes them according to the configured persistence
    /// strategy, persists resulting model states and checkpoints, updates the
    /// in-memory model-state cache, and notifies projection lifecycle listeners
    /// when each flush succeeds.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var system = ActorSystem.Create($"projection-pipeline-{typeof(TState).Name}-{Guid.NewGuid():N}");
        var materializer = ActorMaterializer.Create(system);

        var seenEvents = 0L;
        var transformedEvents = 0L;
        var lastFlushInsertedModels = 0L;
        var lastFlushUpdatedModels = 0L;
        var lastFlushDeletedModels = 0L;
        var lastFlushInFlightCacheHits = 0L;
        var lastFlushInFlightCacheMisses = 0L;
        var flushCount = 0L;
        var cumulativeFlushMs = 0d;
        var runtimeStopwatch = Stopwatch.StartNew();

        var pendingChangesByModel = new Dictionary<Guid, ModelState<TState>>();
        var pendingModelIds = new HashSet<Guid>();
        var checkPoint = await this.checkpointStore.GetCheckpoint(cancellationToken);
        var start = checkPoint.GlobalEventPosition;
        var lastObservedPosition = start;
        var lastFlushedPosition = start;
        var pendingLastObservedPosition = start;
        var hasPendingFlushWindow = false;
        var liveProcessingStarted = false;
        var queueDrainedLogged = false;
        var fullyDrainedLogged = false;
        var periodicFlushRequested = 0;
        var shutdownRequested = 0;
        var nextStagedVersionToken = 0L;
        Task<FlushResult>? inFlightFlushTask = null;
        Task? flushSignalTask = null;
        Task? progressLogTask = null;
        CancellationTokenSource? timerCts = null;
        PeriodicTimer? flushTimer = null;
        PeriodicTimer? progressTimer = null;

        try
        {
            this.logger.LogInformation(
                "Projection pipeline starting for {ModelName}: startPosition={StartPosition}, catchUpStrategy={CatchUpStrategy}, liveStrategy={LiveStrategy}, maxPendingProjections={MaxPendingProjections}, liveFlushDelayMs={LiveFlushDelayMs}",
                this.modelName,
                start,
                this.settings.CatchUpPersistenceStrategy,
                this.settings.LiveProcessingPersistenceStrategy,
                this.settings.MaxPendingProjectionsCount,
                this.settings.LiveProcessingFlushDelay);

            var flushDelay = TimeSpan.FromMilliseconds(Math.Max(1, this.settings.LiveProcessingFlushDelay));
            var progressLogInterval = TimeSpan.FromSeconds(10);

            var sourceBufferSize = Math.Max(1, this.settings.SourceBufferCapacity);
            timerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            flushTimer = new PeriodicTimer(flushDelay);
            progressTimer = new PeriodicTimer(progressLogInterval);

            flushSignalTask = Task.Run(
                async () =>
                {
                    while (await flushTimer.WaitForNextTickAsync(timerCts.Token))
                    {
                        Interlocked.Exchange(ref periodicFlushRequested, 1);
                    }
                },
                timerCts.Token);

            progressLogTask = Task.Run(
                async () =>
                {
                    while (await progressTimer.WaitForNextTickAsync(timerCts.Token))
                    {
                        this.logger.LogInformation(
                            "Projection pipeline progress for {ModelName}: seenEvents={SeenEvents}, lastObservedPosition={LastObservedPosition}",
                            this.modelName,
                            seenEvents,
                            lastObservedPosition);
                    }
                },
                timerCts.Token);

            var stream = Source
                         .From(() => this.source.ReadAll(start, cancellationToken))
                         .Select(RawSignal.ForEnvelope)
                         .KeepAlive(flushDelay, static () => RawSignal.Tick())
                         .Concat(Source.Single(RawSignal.Complete()))

                         // Pull from source aggressively while transform and sink advance independently.
                         .Buffer(sourceBufferSize, OverflowStrategy.Backpressure)
                         .Async()
                         .SelectAsync(
                             parallelism: 1,
                             async signal =>
                             {
                                 if (inFlightFlushTask is { IsCompleted: true, })
                                 {
                                     await ObserveFlushCompletionAsync(inFlightFlushTask, cancellationToken);
                                     inFlightFlushTask = null;
                                 }

                                 switch (signal.Kind)
                                 {
                                     case RawSignalKind.Event:
                                     {
                                         var envelope = signal.Envelope!;
                                         seenEvents++;
                                         lastObservedPosition = envelope.GlobalEventPosition;
                                         pendingLastObservedPosition = envelope.GlobalEventPosition;
                                         hasPendingFlushWindow = true;
                                         pendingModelIds.Add(envelope.ModelId);
                                         fullyDrainedLogged = false;

                                         var change = await this.transformer.Transform(envelope, cancellationToken);
                                         transformedEvents++;

                                         if (change != null)
                                         {
                                             if (pendingChangesByModel.TryGetValue(change.Model.Id, out var existing))
                                             {
                                                 pendingChangesByModel[change.Model.Id] = change with
                                                 {
                                                     ExpectedEventNumber = existing.ExpectedEventNumber,
                                                 };
                                             }
                                             else
                                             {
                                                 pendingChangesByModel[change.Model.Id] = change;
                                             }
                                         }

                                         break;
                                     }
                                     case RawSignalKind.CaughtUp:
                                         liveProcessingStarted = true;
                                         break;
                                     case RawSignalKind.Tick:
                                         break;
                                     case RawSignalKind.Complete:
                                         break;
                                     default:
                                         throw new ArgumentOutOfRangeException();
                                 }

                                 if (inFlightFlushTask == null
                                     && ShouldFlush(signal.Kind))
                                 {
                                     inFlightFlushTask = StartFlush(cancellationToken);
                                 }

                                 return NotUsed.Instance;
                             });

            var runnable = stream
                           .ViaMaterialized(KillSwitches.Single<NotUsed>(), Keep.Right)
                           .ToMaterialized(Sink.Ignore<NotUsed>(), Keep.Both);

            var (killSwitch, completion) = runnable.Run(materializer);

            await using var cancellationRegistration = cancellationToken.Register(
                () =>
                {
                    Interlocked.Exchange(ref shutdownRequested, 1);
                    killSwitch.Shutdown();
                });

            await completion.ConfigureAwait(false);

            if (Volatile.Read(ref shutdownRequested) == 1
                || cancellationToken.IsCancellationRequested)
            {
                this.logger.LogInformation("Projection pipeline cancellation requested");
                await FlushPendingOnShutdownAsync();
            }
            else
            {
                await DrainPendingFlushesAsync(cancellationToken);

                await StopTimersAsync().ConfigureAwait(false);

                this.logger.LogInformation(
                    "Projection pipeline completed for {ModelName}: seen={SeenEvents}, transformed={TransformedEvents}, flushCount={FlushCount}, cumulativeFlushMs={CumulativeFlushMs}, lastFlushInsertedModels={LastFlushInsertedModels}, lastFlushUpdatedModels={LastFlushUpdatedModels}, lastFlushDeletedModels={LastFlushDeletedModels}, lastFlushInFlightCacheHits={LastFlushInFlightCacheHits}, lastFlushInFlightCacheMisses={LastFlushInFlightCacheMisses}, lastObservedPosition={LastObservedPosition}, lastFlushedPosition={LastFlushedPosition}",
                    this.modelName,
                    seenEvents,
                    transformedEvents,
                    flushCount,
                    cumulativeFlushMs,
                    lastFlushInsertedModels,
                    lastFlushUpdatedModels,
                    lastFlushDeletedModels,
                    lastFlushInFlightCacheHits,
                    lastFlushInFlightCacheMisses,
                    lastObservedPosition,
                    lastFlushedPosition);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            this.logger.LogInformation("Projection pipeline cancellation requested");
            await FlushPendingOnShutdownAsync();
        }
        finally
        {
            await StopTimersAsync().ConfigureAwait(false);
            materializer.Shutdown();
            await system.Terminate();
            flushTimer?.Dispose();
            progressTimer?.Dispose();
            timerCts?.Dispose();
        }

        return;

        Task<FlushResult> StartFlush(CancellationToken flushToken)
        {
            var flushedModelIds = pendingModelIds.ToArray();
            var flushPosition = pendingLastObservedPosition;
            var changes = pendingChangesByModel.Values
                                               .Select(CloneModelState)
                                               .ToList();

            pendingChangesByModel.Clear();
            pendingModelIds.Clear();
            hasPendingFlushWindow = false;

            return PersistSnapshotAsync(changes, flushedModelIds, flushPosition, flushToken);
        }

        async Task<FlushResult> PersistSnapshotAsync(
            IReadOnlyList<ModelState<TState>> changes,
            IReadOnlyCollection<Guid> flushedModelIds,
            GlobalEventPosition flushPosition,
            CancellationToken flushToken)
        {
            var flushStopwatch = Stopwatch.StartNew();
            var batchInserted = 0;
            var batchUpdated = 0;
            var batchDeleted = 0;

            if (changes.Count > 0)
            {
                var stagedItems = new List<ProjectedStateEnvelope<TState>>(changes.Count);

                foreach (var change in changes)
                {
                    if (change.ShouldDelete)
                    {
                        batchDeleted++;
                        continue;
                    }

                    if (change.IsNew)
                    {
                        batchInserted++;
                    }
                    else
                    {
                        batchUpdated++;
                    }

                    stagedItems.Add(
                        new ProjectedStateEnvelope<TState>(
                            Model: change.Model,
                            IsNew: change.IsNew,
                            ShouldDelete: change.ShouldDelete,
                            GlobalEventPosition: change.GlobalEventPosition,
                            ExpectedEventNumber: change.ExpectedEventNumber,
                            StagedVersionToken: Interlocked.Increment(ref nextStagedVersionToken),
                            PersistenceStatus: ProjectionPersistenceStatus.Dirty));
                }

                foreach (var stagedItem in stagedItems)
                {
                    await this.projectionCache.Stage(stagedItem, flushToken);
                }

                var pullBatch = await this.projectionCache.PullNextBatch(
                                    new PersistencePullRequest
                                    {
                                        MaxBatchSize = stagedItems.Count,
                                    },
                                    flushToken);

                if (pullBatch != null)
                {
                    var outcomes = await this.storeWriter.WriteBatch(
                                       new PersistenceWriteBatch<TState>
                                       {
                                           Items = pullBatch.Items,
                                       },
                                       flushToken);

                    await this.projectionCache.CompletePull(pullBatch, outcomes, flushToken);
                }
            }

            await this.checkpointStore.PersistCheckpoint(
                new CheckPoint
                {
                    ModelName = this.modelName,
                    GlobalEventPosition = flushPosition,
                },
                flushToken);

            var stats = ReadCacheStats();
            flushStopwatch.Stop();

            return new FlushResult(
                flushedModelIds,
                flushPosition,
                batchInserted,
                batchUpdated,
                batchDeleted,
                stats.Hits,
                stats.Misses,
                flushStopwatch.Elapsed.TotalMilliseconds,
                changes.Count);
        }

        async Task ObserveFlushCompletionAsync(Task<FlushResult> flushTask, CancellationToken flushToken)
        {
            var flushResult = await flushTask.WaitAsync(flushToken);
            var idsToClear = flushResult.FlushedModelIds
                                        .Where(modelId => !pendingChangesByModel.ContainsKey(modelId))
                                        .ToArray();

            var retainedIds = flushResult.FlushedModelIds
                                         .Where(modelId => pendingChangesByModel.ContainsKey(modelId))
                                         .ToArray();

            foreach (var modelId in retainedIds)
            {
                if (pendingChangesByModel.TryGetValue(modelId, out var pending))
                {
                    pendingChangesByModel[modelId] = pending with
                    {
                        IsNew = false,
                    };
                }
            }

            this.lifecycle.OnFlushSucceeded(flushResult.FlushedModelIds, idsToClear);

            lastFlushInsertedModels = flushResult.InsertedModels;
            lastFlushUpdatedModels = flushResult.UpdatedModels;
            lastFlushDeletedModels = flushResult.DeletedModels;
            lastFlushInFlightCacheHits = flushResult.InFlightCacheHits;
            lastFlushInFlightCacheMisses = flushResult.InFlightCacheMisses;
            lastFlushedPosition = flushResult.FlushedPosition;
            flushCount++;
            cumulativeFlushMs += flushResult.ElapsedMilliseconds;

            if (flushResult.BatchModels > 0)
            {
                var activeStrategy = liveProcessingStarted
                                         ? this.settings.LiveProcessingPersistenceStrategy
                                         : this.settings.CatchUpPersistenceStrategy;

                this.logger.LogDebug(
                    "Projection pipeline flush persisted for {ModelName}: batchModels={BatchModels}, inserted={Inserted}, updated={Updated}, deleted={Deleted}, inFlightCacheHits={InFlightCacheHits}, inFlightCacheMisses={InFlightCacheMisses}, flushedPosition={FlushedPosition}, phase={Phase}, strategy={Strategy}",
                    this.modelName,
                    flushResult.BatchModels,
                    lastFlushInsertedModels,
                    lastFlushUpdatedModels,
                    lastFlushDeletedModels,
                    lastFlushInFlightCacheHits,
                    lastFlushInFlightCacheMisses,
                    lastFlushedPosition,
                    liveProcessingStarted ? "Live" : "CatchUp",
                    activeStrategy);
            }

            if (liveProcessingStarted && !queueDrainedLogged)
            {
                var elapsedSeconds = Math.Max(runtimeStopwatch.Elapsed.TotalSeconds, 0.001d);
                var seenEventsPerSecond = seenEvents / elapsedSeconds;
                var transformedEventsPerSecond = transformedEvents / elapsedSeconds;
                var pendingModels = pendingChangesByModel.Count;
                var flushActive = inFlightFlushTask != null;

                this.logger.LogInformation(
                    "Projection source/transform drained for {ModelName}: seen={SeenEvents}, transformed={TransformedEvents}, flushCount={FlushCount}, cumulativeFlushMs={CumulativeFlushMs}, elapsedSeconds={ElapsedSeconds}, seenEventsPerSecond={SeenEventsPerSecond}, transformedEventsPerSecond={TransformedEventsPerSecond}, pendingModels={PendingModels}, flushActive={FlushActive}, lastObservedPosition={LastObservedPosition}, lastFlushedPosition={LastFlushedPosition}",
                    this.modelName,
                    seenEvents,
                    transformedEvents,
                    flushCount,
                    cumulativeFlushMs,
                    elapsedSeconds,
                    seenEventsPerSecond,
                    transformedEventsPerSecond,
                    pendingModels,
                    flushActive,
                    lastObservedPosition,
                    lastFlushedPosition);

                queueDrainedLogged = true;
            }

            if (liveProcessingStarted
                && !fullyDrainedLogged
                && !hasPendingFlushWindow
                && lastFlushedPosition == lastObservedPosition)
            {
                var elapsedSeconds = Math.Max(runtimeStopwatch.Elapsed.TotalSeconds, 0.001d);
                var seenEventsPerSecond = seenEvents / elapsedSeconds;
                var transformedEventsPerSecond = transformedEvents / elapsedSeconds;

                this.logger.LogInformation(
                    "Projection pipeline fully drained for {ModelName}: seen={SeenEvents}, transformed={TransformedEvents}, elapsedSeconds={ElapsedSeconds}, seenEventsPerSecond={SeenEventsPerSecond}, transformedEventsPerSecond={TransformedEventsPerSecond}, lastObservedPosition={LastObservedPosition}, lastFlushedPosition={LastFlushedPosition}, flushCount={FlushCount}, cumulativeFlushMs={CumulativeFlushMs}",
                    this.modelName,
                    seenEvents,
                    transformedEvents,
                    elapsedSeconds,
                    seenEventsPerSecond,
                    transformedEventsPerSecond,
                    lastObservedPosition,
                    lastFlushedPosition,
                    flushCount,
                    cumulativeFlushMs);

                fullyDrainedLogged = true;
            }
        }

        async Task DrainPendingFlushesAsync(CancellationToken flushToken)
        {
            if (inFlightFlushTask != null)
            {
                await ObserveFlushCompletionAsync(inFlightFlushTask, flushToken);
                inFlightFlushTask = null;
            }

            if (hasPendingFlushWindow)
            {
                inFlightFlushTask = StartFlush(flushToken);
                await ObserveFlushCompletionAsync(inFlightFlushTask, flushToken);
                inFlightFlushTask = null;
            }
        }

        bool ShouldFlush(RawSignalKind signalKind)
        {
            if (!hasPendingFlushWindow)
            {
                return false;
            }

            if (signalKind is RawSignalKind.CaughtUp or RawSignalKind.Complete)
            {
                return true;
            }

            var strategy = liveProcessingStarted
                               ? this.settings.LiveProcessingPersistenceStrategy
                               : this.settings.CatchUpPersistenceStrategy;

            var maxPending = Math.Max(1, this.settings.MaxPendingProjectionsCount);
            var timerFlushDue = Interlocked.Exchange(ref periodicFlushRequested, 0) == 1;

            return strategy switch
                   {
                       PersistenceStrategy.ImmediateModelFlush => signalKind == RawSignalKind.Event,
                       PersistenceStrategy.TimeBasedBatching =>
                           pendingModelIds.Count >= maxPending || timerFlushDue,
                       _ => pendingModelIds.Count >= maxPending,
                   };
        }

        async Task FlushPendingOnShutdownAsync()
        {
            try
            {
                await DrainPendingFlushesAsync(CancellationToken.None);

                if (lastFlushedPosition != start)
                {
                    this.logger.LogInformation(
                        "Projection pipeline cancellation flush completed for {ModelName} at position {Position}",
                        this.modelName,
                        lastFlushedPosition);
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Projection pipeline cancellation flush failed for {ModelName}", this.modelName);
            }
        }

        async Task StopTimersAsync()
        {
            if (timerCts == null)
            {
                return;
            }

            try
            {
                await timerCts.CancelAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            await AwaitTimerTask(flushSignalTask).ConfigureAwait(false);
            await AwaitTimerTask(progressLogTask).ConfigureAwait(false);

            flushSignalTask = null;
            progressLogTask = null;
            timerCts = null;
        }

        static async Task AwaitTimerTask(Task? task)
        {
            if (task == null)
            {
                return;
            }

            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on timer cancellation.
            }
        }

        static ModelState<TState> CloneModelState(ModelState<TState> modelState)
        {
            var bytes = MessagePackSerializer.Serialize(modelState.Model, ProjectionCacheMessagePackDefaults.SerializerOptions);
            var clone = MessagePackSerializer.Deserialize<TState>(bytes, ProjectionCacheMessagePackDefaults.SerializerOptions)
                        ?? throw new InvalidOperationException($"Failed to clone model {typeof(TState).Name}");

            return modelState with
            {
                Model = clone,
            };
        }

        (long Hits, long Misses) ReadCacheStats()
        {
            return this.projectionCache.ReadAndResetLookupStats();
        }
    }

    private static bool IsCaughtUpSignal(TEnvelope envelope)
    {
        return envelope is EventEnvelope { Event: ProjectionCaughtUpEvent, };
    }

    private readonly record struct RawSignal(RawSignalKind Kind, TEnvelope? Envelope)
    {
        public static RawSignal Tick()
        {
            return new RawSignal(RawSignalKind.Tick, default);
        }

        public static RawSignal Complete()
        {
            return new RawSignal(RawSignalKind.Complete, default);
        }

        public static RawSignal ForEnvelope(TEnvelope envelope)
        {
            if (IsCaughtUpSignal(envelope))
            {
                return new RawSignal(RawSignalKind.CaughtUp, default);
            }

            return new RawSignal(RawSignalKind.Event, envelope);
        }
    }

    private readonly record struct FlushResult(
        IReadOnlyCollection<Guid> FlushedModelIds,
        GlobalEventPosition FlushedPosition,
        int InsertedModels,
        int UpdatedModels,
        int DeletedModels,
        long InFlightCacheHits,
        long InFlightCacheMisses,
        double ElapsedMilliseconds,
        int BatchModels);
}
