using System.Collections.Concurrent;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MessagePack;
using MessagePack.Resolvers;

namespace Kuna.Projections.Core;

internal static class ProjectionCacheMessagePackDefaults
{
    internal static readonly MessagePackSerializerOptions SerializerOptions =
        MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
}

/// <summary>
/// Stores recently flushed model states as a handoff between the pipeline and the
/// projection engine. After a successful sink flush, the pipeline publishes the
/// flushed model states into this cache. When the next event for a model arrives,
/// the engine checks this cache before loading from the persistent state store.
/// Primary purpose: correctness across flush boundaries, not just performance.
/// Without this cache, the pipeline can persist model state for event N, clear
/// the live in-memory projection instance, and then immediately process event N+1
/// for the same model. If the engine reloads from the state store before the
/// state written for event N is reliably visible there, the projection may be
/// recreated from stale state or appear missing entirely. That can lead to false
/// "state not found" cases or out-of-order/version-check failures even though the
/// previous flush already completed in-process.
/// This cache closes that timing gap by keeping the last flushed model state
/// available in memory until the engine can safely recreate the projection from
/// that state instead of reloading from the store immediately.
/// </summary>
public interface IModelStateCache<TState>
    where TState : class, IModel, new()
{
    bool TryGet(Guid modelId, out ModelState<TState>? change);

    /// <summary>
    /// Adds/updates cache entry only in pipeline flush flow, immediately after
    /// <c>sink.PersistBatch(...)</c> returns in-process.
    /// This is an ordering contract in pipeline code (transform -&gt; persist call returns -&gt; cache set),
    /// not an independent durability/visibility check against the target data store.
    /// Must never be called from transform phase.
    /// </summary>
    void Set(ModelState<TState> modelState);

    (long Hits, long Misses) ReadAndResetLookupStats();
}

/// <summary>
/// In-memory implementation of <see cref="IModelStateCache{TState}"/> that keeps
/// a bounded set of recently flushed model states for reuse by the projection
/// engine. Entries intentionally survive normal flushes so they can bridge store
/// visibility lag after persistence. The cache is bounded by settings and evicts
/// older entries when capacity is exceeded.
/// </summary>
public sealed class InMemoryModelStateCache<TState>
    : IModelStateCache<TState>,
      IProjectionCache<TState>
    where TState : class, IModel, new()
{
    private readonly ConcurrentDictionary<Guid, CacheEntry> inFlightCache;
    private readonly ConcurrentQueue<EvictionEntry> evictionQueue;
    private readonly int inFlightCacheCapacity;
    private int approxInFlightCacheCount;
    private long inFlightLookupHits;
    private long inFlightLookupMisses;
    private long nextToken;

    public InMemoryModelStateCache(IProjectionSettings<TState> settings)
    {
        this.inFlightCacheCapacity = Math.Max(
            1,
            Math.Max(
                settings.InFlightModelCacheMinEntries,
                Math.Max(1, settings.MaxPendingProjectionsCount) * Math.Max(1, settings.InFlightModelCacheCapacityMultiplier)));

        this.inFlightCache = new ConcurrentDictionary<Guid, CacheEntry>(Environment.ProcessorCount, this.inFlightCacheCapacity);
        this.evictionQueue = new ConcurrentQueue<EvictionEntry>();
    }

    public bool TryGet(Guid modelId, out ModelState<TState>? state)
    {
        if (!this.inFlightCache.TryGetValue(modelId, out var cached))
        {
            Interlocked.Increment(ref this.inFlightLookupMisses);
            state = null;
            return false;
        }

        Interlocked.Increment(ref this.inFlightLookupHits);
        state = ToModelState(cached.State);
        return true;
    }

    public void Set(ModelState<TState> modelState)
    {
        var state = new ProjectedStateEnvelope<TState>(
            Model: CloneModel(modelState.Model),
            IsNew: false,
            ShouldDelete: modelState.ShouldDelete,
            GlobalEventPosition: modelState.GlobalEventPosition,
            ExpectedEventNumber: modelState.ExpectedEventNumber,
            StageToken: Interlocked.Increment(ref this.nextToken),
            PersistenceStatus: ProjectionPersistenceStatus.Persisted);

        var isNewKey = this.inFlightCache.TryAdd(state.Model.Id, new CacheEntry(state));

        if (isNewKey)
        {
            Interlocked.Increment(ref this.approxInFlightCacheCount);
        }
        else
        {
            this.inFlightCache[state.Model.Id] = new CacheEntry(state);
        }

        this.evictionQueue.Enqueue(new EvictionEntry(state.Model.Id, state.StageToken));

        if (Volatile.Read(ref this.approxInFlightCacheCount) <= this.inFlightCacheCapacity
            || !this.evictionQueue.TryDequeue(out var candidate)) return;

        // Example:
        // 1) Set(A) -> token=10, queue has (A,10), map[A]=(modelState@10,10)
        // 2) Set(A) again -> token=11, queue has (A,10),(A,11), map[A]=(modelState@11,11)
        // 3) Eviction dequeues candidate (A,10)
        // 4) If we removed by key only, we'd delete modelState@11 (newest) by mistake.
        //    Therefore, we remove only when current token == candidate token.
        if (this.inFlightCache.TryGetValue(candidate.ModelId, out var current)
            && current.State.StageToken == candidate.Token
            && this.inFlightCache.TryRemove(candidate.ModelId, out _))
        {
            Interlocked.Decrement(ref this.approxInFlightCacheCount);
        }
    }

    public ValueTask<ProjectedStateEnvelope<TState>?> Get(Guid modelId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!this.inFlightCache.TryGetValue(modelId, out var cached))
        {
            Interlocked.Increment(ref this.inFlightLookupMisses);
            return ValueTask.FromResult<ProjectedStateEnvelope<TState>?>(null);
        }

        Interlocked.Increment(ref this.inFlightLookupHits);
        return ValueTask.FromResult<ProjectedStateEnvelope<TState>?>(CloneProjectedState(cached.State));
    }

    public ValueTask Stage(ProjectedStateEnvelope<TState> state, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var stagedState = state with
        {
            Model = CloneModel(state.Model),
        };

        var isNewKey = this.inFlightCache.TryAdd(stagedState.Model.Id, new CacheEntry(stagedState));

        if (isNewKey)
        {
            Interlocked.Increment(ref this.approxInFlightCacheCount);
        }
        else
        {
            this.inFlightCache[stagedState.Model.Id] = new CacheEntry(stagedState);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<PersistencePullBatch<TState>?> PullNextBatch(
        PersistencePullRequest request,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException(
            "Pull-based persistence is not supported by the in-memory projection cache until the explicit cache stage is wired into the pipeline.");
    }

    public ValueTask CompletePull(
        PersistencePullBatch<TState> batch,
        IReadOnlyList<PersistenceItemOutcome> outcomes,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException(
            "Pull completion is not supported by the in-memory projection cache until the explicit cache stage is wired into the pipeline.");
    }

    public (long Hits, long Misses) ReadAndResetLookupStats()
    {
        var hits = Interlocked.Exchange(ref this.inFlightLookupHits, 0);
        var misses = Interlocked.Exchange(ref this.inFlightLookupMisses, 0);
        return (hits, misses);
    }

    private static ModelState<TState> ToModelState(ProjectedStateEnvelope<TState> state)
    {
        return new ModelState<TState>(
            CloneModel(state.Model),
            state.IsNew,
            state.ShouldDelete,
            state.GlobalEventPosition,
            state.ExpectedEventNumber);
    }

    private static TState CloneModel(TState model)
    {
        var bytes = MessagePackSerializer.Serialize(model, ProjectionCacheMessagePackDefaults.SerializerOptions);
        var clone = MessagePackSerializer.Deserialize<TState>(bytes, ProjectionCacheMessagePackDefaults.SerializerOptions);

        return clone ?? throw new InvalidOperationException($"Failed to clone model {typeof(TState).Name}");
    }

    private static ProjectedStateEnvelope<TState> CloneProjectedState(ProjectedStateEnvelope<TState> state)
    {
        return state with
        {
            Model = CloneModel(state.Model),
        };
    }

    private readonly record struct EvictionEntry(Guid ModelId, long Token);

    private readonly record struct CacheEntry(ProjectedStateEnvelope<TState> State);
}
