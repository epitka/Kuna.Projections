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
    private readonly IModelStateCache<TState> modelStateCache;
    private readonly IModelStateSink<TState> sink;
    private readonly ICheckpointStore checkpointStore;
    private readonly IProjectionSettings<TState> settings;
    private readonly ILogger logger;

    internal ProjectionPipeline(
        IEventSource<TEnvelope> source,
        IModelStateTransformer<TEnvelope, TState> transformer,
        IProjectionLifecycle lifecycle,
        IModelStateCache<TState> modelStateCache,
        IModelStateSink<TState> sink,
        ICheckpointStore checkpointStore,
        IProjectionSettings<TState> settings,
        ILogger<ProjectionPipeline<TEnvelope, TState>> logger)
    {
        this.source = source;
        this.transformer = transformer;
        this.lifecycle = lifecycle;
        this.modelStateCache = modelStateCache;
        this.sink = sink;
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

    private enum PipelineSignalKind
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
        var processedEvents = 0L;
        var lastFlushInsertedModels = 0L;
        var lastFlushUpdatedModels = 0L;
        var lastFlushDeletedModels = 0L;
        var lastFlushModelStateCacheHits = 0L;
        var lastFlushModelStateCacheMisses = 0L;
        var flushCount = 0L;
        var cumulativeFlushMs = 0d;
        var runtimeStopwatch = Stopwatch.StartNew();

        var checkPoint = await this.checkpointStore.GetCheckpoint(cancellationToken);
        var start = checkPoint.GlobalEventPosition;
        var lastObservedPosition = start;
        var lastFlushedPosition = start;
        var liveProcessingStarted = false;
        var sourceTransformDrainObserved = 0;
        var sourceTransformDrainedLogged = 0;
        var fullyDrainedLogged = 0;
        var pendingBatchCount = 0L;
        var pendingModelCountSnapshot = 0;
        var periodicFlushRequested = 0;
        var shutdownRequested = 0;
        var bufferedModelEventCounts = new Dictionary<Guid, int>();
        var bufferedModelEventCountsLock = new object();
        CancellationTokenSource? timerCts = null;
        PeriodicTimer? flushTimer = null;
        Task? flushSignalTask = null;

        try
        {
            this.logger.LogInformation(
                "Projection pipeline starting for {ModelName}: startPosition={StartPosition}, catchUpStrategy={CatchUpStrategy}, liveStrategy={LiveStrategy}, catchUpModelCountThreshold={CatchUpModelCountThreshold}, liveModelCountThreshold={LiveModelCountThreshold}, catchUpFlushDelay={CatchUpFlushDelay}, liveFlushDelay={LiveFlushDelay}",
                this.modelName,
                start,
                this.settings.CatchUpFlush.Strategy,
                this.settings.LiveProcessingFlush.Strategy,
                this.settings.CatchUpFlush.ModelCountThreshold,
                this.settings.LiveProcessingFlush.ModelCountThreshold,
                NormalizeFlushDelay(this.settings.CatchUpFlush.Delay),
                NormalizeFlushDelay(this.settings.LiveProcessingFlush.Delay));

            var catchUpFlushDelay = NormalizeFlushDelay(this.settings.CatchUpFlush.Delay);
            var liveProcessingFlushDelay = NormalizeFlushDelay(this.settings.LiveProcessingFlush.Delay);
            var flushDelay = catchUpFlushDelay <= liveProcessingFlushDelay ? catchUpFlushDelay : liveProcessingFlushDelay;
            var sourceBufferSize = Math.Max(1, this.settings.Backpressure.SourceToTransformBufferCapacity);
            var transformSinkBufferSize = Math.Max(1, this.settings.Backpressure.TransformToSinkBufferCapacity);
            timerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            flushTimer = new PeriodicTimer(flushDelay);

            flushSignalTask = Task.Run(
                async () =>
                {
                    while (await flushTimer.WaitForNextTickAsync(timerCts.Token))
                    {
                        Interlocked.Exchange(ref periodicFlushRequested, 1);
                    }
                },
                timerCts.Token);

            var stream = Source
                         .From(ReadSourceUntilCancellation)
                         .Select(
                             envelope =>
                             {
                                 var signal = RawSignal.ForEnvelope(envelope);

                                 if (signal.Kind == RawSignalKind.Event)
                                 {
                                     TrackBufferedModel(envelope.ModelId);
                                 }

                                 return signal;
                             })
                         .KeepAlive(flushDelay, static () => RawSignal.Tick())
                         .Concat(Source.Single(RawSignal.Complete()))

                         // Pull from source aggressively while transform and sink advance independently.
                         .Buffer(sourceBufferSize, OverflowStrategy.Backpressure)
                         .Async()
                         .SelectAsync(
                             parallelism: 1,
                             async signal =>
                             {
                                 try
                                 {
                                     switch (signal.Kind)
                                     {
                                         case RawSignalKind.Event:
                                         {
                                             var envelope = signal.Envelope!;
                                             seenEvents++;

                                             var change = await this.transformer.Transform(envelope, cancellationToken);
                                             transformedEvents++;
                                             lastObservedPosition = envelope.GlobalEventPosition;

                                             return PipelineSignal<TState>.Event(
                                                 envelope.ModelId,
                                                 envelope.GlobalEventPosition,
                                                 change == null ? null : CloneModelState(change));
                                         }
                                         case RawSignalKind.CaughtUp:
                                             return PipelineSignal<TState>.CaughtUp();
                                         case RawSignalKind.Tick:
                                             return PipelineSignal<TState>.Tick();
                                         case RawSignalKind.Complete:
                                             return PipelineSignal<TState>.Complete();
                                         default:
                                             throw new ArgumentOutOfRangeException();
                                     }
                                 }
                                 catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                                 {
                                     Interlocked.Exchange(ref shutdownRequested, 1);
                                     return PipelineSignal<TState>.Complete();
                                 }
                             })
                         .Buffer(transformSinkBufferSize, OverflowStrategy.Backpressure)
                         .Async()
                         .Via(CreateBatchingFlow())
                         .SelectAsync(1, PersistFlushAsync);

            var runnable = stream
                           .ViaMaterialized(KillSwitches.Single<FlushResult>(), Keep.Right)
                           .ToMaterialized(Sink.Ignore<FlushResult>(), Keep.Both);

            var (_, completion) = runnable.Run(materializer);

            await using var cancellationRegistration = cancellationToken.Register(
                () =>
                {
                    Interlocked.Exchange(ref shutdownRequested, 1);
                });

            await completion.ConfigureAwait(false);

            if (Volatile.Read(ref shutdownRequested) == 1
                || cancellationToken.IsCancellationRequested)
            {
                this.logger.LogInformation("Projection pipeline cancellation requested");
            }
            else
            {
                this.logger.LogInformation(
                    "Projection pipeline completed for {ModelName}: seen={SeenEvents}, transformed={TransformedEvents}, flushCount={FlushCount}, cumulativeFlushMs={CumulativeFlushMs:F0}, lastFlushInsertedModels={LastFlushInsertedModels}, lastFlushUpdatedModels={LastFlushUpdatedModels}, lastFlushDeletedModels={LastFlushDeletedModels}, lastFlushModelStateCacheHits={LastFlushModelStateCacheHits}, lastFlushModelStateCacheMisses={LastFlushModelStateCacheMisses}, lastObservedPosition={LastObservedPosition}, lastFlushedPosition={LastFlushedPosition}",
                    this.modelName,
                    seenEvents,
                    transformedEvents,
                    flushCount,
                    cumulativeFlushMs,
                    lastFlushInsertedModels,
                    lastFlushUpdatedModels,
                    lastFlushDeletedModels,
                    lastFlushModelStateCacheHits,
                    lastFlushModelStateCacheMisses,
                    lastObservedPosition,
                    lastFlushedPosition);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            this.logger.LogInformation("Projection pipeline cancellation requested");
        }
        finally
        {
            await StopTimersAsync().ConfigureAwait(false);
            materializer.Shutdown();
            await system.Terminate();
            flushTimer?.Dispose();
            timerCts?.Dispose();
        }

        return;

        async IAsyncEnumerable<TEnvelope> ReadSourceUntilCancellation()
        {
            var enumerator = this.source.ReadAll(start, cancellationToken).GetAsyncEnumerator(cancellationToken);

            try
            {
                while (true)
                {
                    TEnvelope envelope;

                    try
                    {
                        if (!await enumerator.MoveNextAsync())
                        {
                            yield break;
                        }

                        envelope = enumerator.Current;
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        yield break;
                    }

                    yield return envelope;
                }
            }
            finally
            {
                await enumerator.DisposeAsync();
            }
        }

        Flow<PipelineSignal<TState>, PipelineFlush<TState>, NotUsed> CreateBatchingFlow()
        {
            return Flow.Create<PipelineSignal<TState>>()
                       .StatefulSelectMany<PipelineSignal<TState>, PipelineSignal<TState>, PipelineFlush<TState>, NotUsed>(
                           () =>
                           {
                               var pendingChangesByModel = new Dictionary<Guid, ModelState<TState>>();
                               var pendingModelIds = new HashSet<Guid>();
                               var pendingModelEventCounts = new Dictionary<Guid, int>();
                               var pendingLastObservedPosition = start;
                               var pendingEventCount = 0L;
                               var hasPendingFlushWindow = false;
                               var pendingFlushWindowStartedAt = 0L;

                               return signal =>
                               {
                                   var output = new List<PipelineFlush<TState>>(1);

                                   switch (signal.Kind)
                                   {
                                       case PipelineSignalKind.Event:
                                           pendingEventCount++;
                                           pendingLastObservedPosition = signal.Position;

                                           if (!hasPendingFlushWindow)
                                           {
                                               pendingFlushWindowStartedAt = Stopwatch.GetTimestamp();
                                           }

                                           hasPendingFlushWindow = true;
                                           pendingModelIds.Add(signal.ModelId);
                                           Volatile.Write(ref pendingModelCountSnapshot, pendingModelIds.Count);
                                           pendingModelEventCounts.TryGetValue(signal.ModelId, out var pendingModelEventCount);
                                           pendingModelEventCounts[signal.ModelId] = pendingModelEventCount + 1;

                                           if (signal.Change != null
                                               && signal.Change is not { IsNew: true, ShouldDelete: true, })
                                           {
                                               if (pendingChangesByModel.TryGetValue(signal.Change.Model.Id, out var existing))
                                               {
                                                   pendingChangesByModel[signal.Change.Model.Id] = signal.Change with
                                                   {
                                                       ExpectedEventNumber = existing.ExpectedEventNumber,
                                                   };
                                               }
                                               else
                                               {
                                                   pendingChangesByModel[signal.Change.Model.Id] = signal.Change;
                                               }
                                           }

                                           if (ShouldFlush(signal.Kind))
                                           {
                                               output.Add(StartFlush());
                                           }

                                           break;

                                       case PipelineSignalKind.CaughtUp:
                                           if (hasPendingFlushWindow)
                                           {
                                               output.Add(StartFlush());
                                           }

                                           liveProcessingStarted = true;
                                           MarkSourceTransformDrainObserved();

                                           if (output.Count == 0)
                                           {
                                               TryLogSourceTransformDrained();
                                               TryLogFullyDrained();
                                           }

                                           break;

                                       case PipelineSignalKind.Tick:
                                           if (ShouldFlush(signal.Kind))
                                           {
                                               output.Add(StartFlush());
                                           }

                                           break;

                                       case PipelineSignalKind.Complete:
                                           if (hasPendingFlushWindow)
                                           {
                                               output.Add(StartFlush());
                                           }

                                           MarkSourceTransformDrainObserved();

                                           if (output.Count == 0)
                                           {
                                               TryLogSourceTransformDrained();
                                               TryLogFullyDrained();
                                           }

                                           break;

                                       default:
                                           throw new ArgumentOutOfRangeException();
                                   }

                                   return output;

                                   PipelineFlush<TState> StartFlush()
                                   {
                                       var flush = new PipelineFlush<TState>(
                                           pendingChangesByModel.Values.ToList(),
                                           pendingModelIds.ToArray(),
                                           new Dictionary<Guid, int>(pendingModelEventCounts),
                                           pendingLastObservedPosition,
                                           pendingEventCount);

                                       pendingChangesByModel.Clear();
                                       pendingModelIds.Clear();
                                       pendingModelEventCounts.Clear();
                                       pendingEventCount = 0;
                                       hasPendingFlushWindow = false;
                                       pendingFlushWindowStartedAt = 0;
                                       Volatile.Write(ref pendingModelCountSnapshot, 0);
                                       Interlocked.Increment(ref pendingBatchCount);

                                       return flush;
                                   }

                                   bool ShouldFlush(PipelineSignalKind signalKind)
                                   {
                                       if (!hasPendingFlushWindow)
                                       {
                                           return false;
                                       }

                                       var flushSettings = liveProcessingStarted
                                                               ? this.settings.LiveProcessingFlush
                                                               : this.settings.CatchUpFlush;

                                       var modelCountFlushThreshold = Math.Max(
                                           1,
                                           flushSettings.ModelCountThreshold);

                                       var timerFlushDue = Interlocked.Exchange(ref periodicFlushRequested, 0) == 1;

                                       return flushSettings.Strategy switch
                                              {
                                                  PersistenceStrategy.ImmediateModelFlush => signalKind == PipelineSignalKind.Event,
                                                  PersistenceStrategy.TimeBasedBatching =>
                                                      IsFlushDelayElapsed(flushSettings.Delay)
                                                      && (signalKind == PipelineSignalKind.Event || signalKind == PipelineSignalKind.Tick || timerFlushDue),
                                                  _ => pendingModelIds.Count >= modelCountFlushThreshold,
                                              };

                                       bool IsFlushDelayElapsed(int delayMilliseconds)
                                       {
                                           return pendingFlushWindowStartedAt > 0
                                                  && Stopwatch.GetElapsedTime(pendingFlushWindowStartedAt) >= NormalizeFlushDelay(delayMilliseconds);
                                       }
                                   }
                               };
                           });
        }

        async Task<FlushResult> PersistFlushAsync(PipelineFlush<TState> flush)
        {
            var flushStopwatch = Stopwatch.StartNew();
            var phaseStopwatch = Stopwatch.StartNew();
            var batchInserted = 0;
            var batchUpdated = 0;
            var batchDeleted = 0;
            var changes = flush.Changes.Select(NormalizePersistedModelState).ToList();
            var sinkPersistMs = 0d;
            var cachePublishMs = 0d;
            var checkpointPersistMs = 0d;
            var lifecycleMs = 0d;

            if (changes.Count > 0)
            {
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
                }

                var batch = new ModelStatesBatch<TState>
                {
                    Changes = changes,
                    GlobalEventPosition = flush.Position,
                };

                await this.sink.PersistBatch(batch, CancellationToken.None);
                sinkPersistMs = phaseStopwatch.Elapsed.TotalMilliseconds;

                phaseStopwatch.Restart();

                foreach (var change in changes)
                {
                    this.modelStateCache.Set(change);
                }

                cachePublishMs = phaseStopwatch.Elapsed.TotalMilliseconds;
            }

            phaseStopwatch.Restart();
            await this.checkpointStore.PersistCheckpoint(
                new CheckPoint
                {
                    ModelName = this.modelName,
                    GlobalEventPosition = flush.Position,
                },
                CancellationToken.None);

            checkpointPersistMs = phaseStopwatch.Elapsed.TotalMilliseconds;

            var stats = this.modelStateCache.ReadAndResetLookupStats();
            var runtimeStats = ReadRuntimeStats();
            flushStopwatch.Stop();

            phaseStopwatch.Restart();
            var idsToClear = ReleaseFlushedModels(flush.ModelEventCounts);
            this.lifecycle.OnFlushSucceeded(flush.ModelIds, idsToClear, GetFlushedEventNumbers(changes));
            lifecycleMs = phaseStopwatch.Elapsed.TotalMilliseconds;

            var flushResult = new FlushResult(
                flush.ModelIds,
                flush.Position,
                batchInserted,
                batchUpdated,
                batchDeleted,
                stats.Hits,
                stats.Misses,
                flushStopwatch.Elapsed.TotalMilliseconds,
                flush.Events,
                changes.Count);

            lastFlushInsertedModels = flushResult.InsertedModels;
            lastFlushUpdatedModels = flushResult.UpdatedModels;
            lastFlushDeletedModels = flushResult.DeletedModels;
            lastFlushModelStateCacheHits = flushResult.ModelStateCacheHits;
            lastFlushModelStateCacheMisses = flushResult.ModelStateCacheMisses;
            lastFlushedPosition = flushResult.FlushedPosition;
            processedEvents += flushResult.Events;
            flushCount++;
            cumulativeFlushMs += flushResult.ElapsedMilliseconds;
            Interlocked.Decrement(ref pendingBatchCount);

            if (flushResult.BatchModels > 0)
            {
                var activeStrategy = liveProcessingStarted
                                         ? this.settings.LiveProcessingFlush.Strategy
                                         : this.settings.CatchUpFlush.Strategy;

                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug(
                        "Projection pipeline flush persisted for {ModelName}: batchModels={BatchModels}, inserted={Inserted}, updated={Updated}, deleted={Deleted}, sinkPersistMs={SinkPersistMs:F0}, cachePublishMs={CachePublishMs:F0}, checkpointPersistMs={CheckpointPersistMs:F0}, lifecycleMs={LifecycleMs:F0}, runtimeProjectionHits={RuntimeProjectionHits}, modelStateCacheRestores={ModelStateCacheRestores}, storeProjectionLoads={StoreProjectionLoads}, storeProjectionMisses={StoreProjectionMisses}, newProjectionCreates={NewProjectionCreates}, modelStateCacheHits={ModelStateCacheHits}, modelStateCacheMisses={ModelStateCacheMisses}, flushedPosition={FlushedPosition}, phase={Phase}, strategy={Strategy}",
                        this.modelName,
                        flushResult.BatchModels,
                        lastFlushInsertedModels,
                        lastFlushUpdatedModels,
                        lastFlushDeletedModels,
                        sinkPersistMs,
                        cachePublishMs,
                        checkpointPersistMs,
                        lifecycleMs,
                        runtimeStats.RuntimeProjectionHits,
                        runtimeStats.ModelStateCacheRestores,
                        runtimeStats.StoreProjectionLoads,
                        runtimeStats.StoreProjectionMisses,
                        runtimeStats.NewProjectionCreates,
                        lastFlushModelStateCacheHits,
                        lastFlushModelStateCacheMisses,
                        lastFlushedPosition,
                        liveProcessingStarted ? "Live" : "CatchUp",
                        activeStrategy);
                }
            }

            TryLogSourceTransformDrained();
            TryLogFullyDrained();

            return flushResult;
        }

        ProjectionRuntimeStats ReadRuntimeStats()
        {
            return this.transformer is IProjectionRuntimeStats runtimeStats
                       ? runtimeStats.ReadAndResetRuntimeStats()
                       : default;
        }

        static IReadOnlyDictionary<Guid, long?> GetFlushedEventNumbers(IReadOnlyCollection<ModelState<TState>> changes)
        {
            if (changes.Count == 0)
            {
                return new Dictionary<Guid, long?>(0);
            }

            var eventNumbers = new Dictionary<Guid, long?>(changes.Count);

            foreach (var change in changes)
            {
                if (!eventNumbers.TryGetValue(change.Model.Id, out var current)
                    || current is null
                    || change.Model.EventNumber > current)
                {
                    eventNumbers[change.Model.Id] = change.Model.EventNumber;
                }
            }

            return eventNumbers;
        }

        void MarkSourceTransformDrainObserved()
        {
            Interlocked.Exchange(ref sourceTransformDrainObserved, 1);
        }

        void TryLogSourceTransformDrained()
        {
            if (Volatile.Read(ref sourceTransformDrainObserved) == 0
                || Interlocked.CompareExchange(ref sourceTransformDrainedLogged, 1, 0) != 0)
            {
                return;
            }

            var elapsedSeconds = Math.Max(runtimeStopwatch.Elapsed.TotalSeconds, 0.001d);
            var flushActive = Volatile.Read(ref pendingBatchCount) > 0;

            this.logger.LogInformation(
                "Projection source/transform drained for {ModelName}: seen={SeenEvents}, transformed={TransformedEvents}, processed={ProcessedEvents}, flushCount={FlushCount}, cumulativeFlushMs={CumulativeFlushMs:F0}, elapsedSeconds={ElapsedSeconds:F0}, seenEventsPerSecond={SeenEventsPerSecond:F0}, transformedEventsPerSecond={TransformedEventsPerSecond:F0}, processedEventsPerSecond={ProcessedEventsPerSecond:F0}, pendingModels={PendingModels}, flushActive={FlushActive}, lastObservedPosition={LastObservedPosition}, lastFlushedPosition={LastFlushedPosition}",
                this.modelName,
                seenEvents,
                transformedEvents,
                processedEvents,
                flushCount,
                cumulativeFlushMs,
                elapsedSeconds,
                seenEvents / elapsedSeconds,
                transformedEvents / elapsedSeconds,
                processedEvents / elapsedSeconds,
                Volatile.Read(ref pendingModelCountSnapshot),
                flushActive,
                lastObservedPosition,
                lastFlushedPosition);
        }

        void TryLogFullyDrained()
        {
            if (Volatile.Read(ref sourceTransformDrainObserved) == 0
                || Volatile.Read(ref pendingBatchCount) != 0
                || Volatile.Read(ref pendingModelCountSnapshot) != 0
                || lastFlushedPosition != lastObservedPosition
                || Interlocked.CompareExchange(ref fullyDrainedLogged, 1, 0) != 0)
            {
                return;
            }

            var elapsedSeconds = Math.Max(runtimeStopwatch.Elapsed.TotalSeconds, 0.001d);

            this.logger.LogInformation(
                "Projection pipeline fully drained for {ModelName}: seen={SeenEvents}, transformed={TransformedEvents}, processed={ProcessedEvents}, elapsedSeconds={ElapsedSeconds:F0}, seenEventsPerSecond={SeenEventsPerSecond:F0}, transformedEventsPerSecond={TransformedEventsPerSecond:F0}, processedEventsPerSecond={ProcessedEventsPerSecond:F0}, lastObservedPosition={LastObservedPosition}, lastFlushedPosition={LastFlushedPosition}, flushCount={FlushCount}, cumulativeFlushMs={CumulativeFlushMs:F0}",
                this.modelName,
                seenEvents,
                transformedEvents,
                processedEvents,
                elapsedSeconds,
                seenEvents / elapsedSeconds,
                transformedEvents / elapsedSeconds,
                processedEvents / elapsedSeconds,
                lastObservedPosition,
                lastFlushedPosition,
                flushCount,
                cumulativeFlushMs);
        }

        ModelState<TState> NormalizePersistedModelState(ModelState<TState> change)
        {
            if (!change.IsNew
                || change.ExpectedEventNumber is null or < 0)
            {
                return change;
            }

            return change with
            {
                IsNew = false,
            };
        }

        void TrackBufferedModel(Guid modelId)
        {
            lock (bufferedModelEventCountsLock)
            {
                bufferedModelEventCounts.TryGetValue(modelId, out var currentCount);
                bufferedModelEventCounts[modelId] = currentCount + 1;
            }
        }

        IReadOnlyCollection<Guid> ReleaseFlushedModels(IReadOnlyDictionary<Guid, int> flushedModelEventCounts)
        {
            var idsToClear = new List<Guid>(flushedModelEventCounts.Count);

            lock (bufferedModelEventCountsLock)
            {
                foreach (var (modelId, flushedEventCount) in flushedModelEventCounts)
                {
                    if (!bufferedModelEventCounts.TryGetValue(modelId, out var bufferedEventCount))
                    {
                        continue;
                    }

                    var remainingCount = bufferedEventCount - flushedEventCount;

                    if (remainingCount > 0)
                    {
                        bufferedModelEventCounts[modelId] = remainingCount;
                        continue;
                    }

                    bufferedModelEventCounts.Remove(modelId);
                    idsToClear.Add(modelId);
                }
            }

            return idsToClear;
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

            if (flushSignalTask != null)
            {
                try
                {
                    await flushSignalTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Expected on timer cancellation.
                }
            }

            flushSignalTask = null;
            timerCts = null;
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
    }

    private static bool IsCaughtUpSignal(TEnvelope envelope)
    {
        return envelope is EventEnvelope { Event: ProjectionCaughtUpEvent, };
    }

    private static TimeSpan NormalizeFlushDelay(int delayMilliseconds)
    {
        return TimeSpan.FromMilliseconds(Math.Max(1, delayMilliseconds));
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

    private readonly record struct PipelineSignal<TModel>(
        PipelineSignalKind Kind,
        Guid ModelId,
        GlobalEventPosition Position,
        ModelState<TModel>? Change)
        where TModel : class, IModel, new()
    {
        public static PipelineSignal<TModel> Event(
            Guid modelId,
            GlobalEventPosition position,
            ModelState<TModel>? change)
        {
            return new PipelineSignal<TModel>(PipelineSignalKind.Event, modelId, position, change);
        }

        public static PipelineSignal<TModel> Tick()
        {
            return new PipelineSignal<TModel>(PipelineSignalKind.Tick, Guid.Empty, default, default);
        }

        public static PipelineSignal<TModel> CaughtUp()
        {
            return new PipelineSignal<TModel>(PipelineSignalKind.CaughtUp, Guid.Empty, default, default);
        }

        public static PipelineSignal<TModel> Complete()
        {
            return new PipelineSignal<TModel>(PipelineSignalKind.Complete, Guid.Empty, default, default);
        }
    }

    private readonly record struct PipelineFlush<TModel>(
        IReadOnlyList<ModelState<TModel>> Changes,
        IReadOnlyCollection<Guid> ModelIds,
        IReadOnlyDictionary<Guid, int> ModelEventCounts,
        GlobalEventPosition Position,
        long Events)
        where TModel : class, IModel, new();

    private readonly record struct FlushResult(
        IReadOnlyCollection<Guid> FlushedModelIds,
        GlobalEventPosition FlushedPosition,
        int InsertedModels,
        int UpdatedModels,
        int DeletedModels,
        long ModelStateCacheHits,
        long ModelStateCacheMisses,
        double ElapsedMilliseconds,
        long Events,
        int BatchModels);
}
