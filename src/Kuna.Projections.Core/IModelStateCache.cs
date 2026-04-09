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
public sealed class InMemoryModelStateCache<TState> : IModelStateCache<TState>
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
        state = CloneProjectionState(cached.ModelState);
        return true;
    }

    public void Set(ModelState<TState> modelState)
    {
        var cachedState =
            modelState with
            {
                IsNew = false,
            };

        var token = Interlocked.Increment(ref this.nextToken);
        var entry = new CacheEntry(cachedState, token);
        var isNewKey = this.inFlightCache.TryAdd(cachedState.Model.Id, entry);

        if (isNewKey)
        {
            Interlocked.Increment(ref this.approxInFlightCacheCount);
        }
        else
        {
            this.inFlightCache[cachedState.Model.Id] = entry;
        }

        this.evictionQueue.Enqueue(new EvictionEntry(cachedState.Model.Id, token));

        if (Volatile.Read(ref this.approxInFlightCacheCount) <= this.inFlightCacheCapacity
            || !this.evictionQueue.TryDequeue(out var candidate)) return;

        // Example:
        // 1) Set(A) -> token=10, queue has (A,10), map[A]=(modelState@10,10)
        // 2) Set(A) again -> token=11, queue has (A,10),(A,11), map[A]=(modelState@11,11)
        // 3) Eviction dequeues candidate (A,10)
        // 4) If we removed by key only, we'd delete modelState@11 (newest) by mistake.
        //    Therefore, we remove only when current token == candidate token.
        if (this.inFlightCache.TryGetValue(candidate.ModelId, out var current)
            && current.Token == candidate.Token
            && this.inFlightCache.TryRemove(candidate.ModelId, out _))
        {
            Interlocked.Decrement(ref this.approxInFlightCacheCount);
        }
    }

    public (long Hits, long Misses) ReadAndResetLookupStats()
    {
        var hits = Interlocked.Exchange(ref this.inFlightLookupHits, 0);
        var misses = Interlocked.Exchange(ref this.inFlightLookupMisses, 0);
        return (hits, misses);
    }

    private static ModelState<TState> CloneProjectionState(ModelState<TState> modelState)
    {
        return modelState with
        {
            Model = CloneModel(modelState.Model),
        };
    }

    private static TState CloneModel(TState model)
    {
        var bytes = MessagePackSerializer.Serialize(model, ProjectionCacheMessagePackDefaults.SerializerOptions);
        var clone = MessagePackSerializer.Deserialize<TState>(bytes, ProjectionCacheMessagePackDefaults.SerializerOptions);

        return clone ?? throw new InvalidOperationException($"Failed to clone model {typeof(TState).Name}");
    }

    private readonly record struct EvictionEntry(Guid ModelId, long Token);

    private readonly record struct CacheEntry(ModelState<TState> ModelState, long Token);
}
