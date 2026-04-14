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
/// In-memory implementation of <see cref="IProjectionCache{TState}"/> that keeps
/// staged and recently persisted model states available for projection reloads.
/// Entries intentionally survive normal flushes so they can bridge store
/// visibility lag after persistence. Persisted entries are bounded by settings
/// and evicted when capacity is exceeded.
/// </summary>
public sealed class InMemoryModelStateCache<TState>
    : IProjectionCache<TState>
    where TState : class, IModel, new()
{
    private readonly ConcurrentDictionary<Guid, CacheEntry> inFlightCache;
    private readonly ConcurrentQueue<EvictionEntry> evictionQueue;
    private readonly int inFlightCacheCapacity;
    private int approxInFlightCacheCount;
    private long inFlightLookupHits;
    private long inFlightLookupMisses;

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

        this.StoreEntry(stagedState);

        return ValueTask.CompletedTask;
    }

    public ValueTask<PersistencePullBatch<TState>?> PullNextBatch(
        PersistencePullRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var maxBatchSize = Math.Max(1, request.MaxBatchSize);
        var selected = new List<ProjectedStateEnvelope<TState>>(maxBatchSize);
        GlobalEventPosition? maxPosition = null;

        foreach (var (modelId, entry) in this.inFlightCache)
        {
            if (selected.Count >= maxBatchSize)
            {
                break;
            }

            if (entry.State.PersistenceStatus != ProjectionPersistenceStatus.Dirty)
            {
                continue;
            }

            var inFlight = entry.State with
            {
                PersistenceStatus = ProjectionPersistenceStatus.InFlight,
            };

            if (!this.inFlightCache.TryUpdate(modelId, new CacheEntry(inFlight), entry))
            {
                continue;
            }

            selected.Add(CloneProjectedState(inFlight));
            maxPosition = maxPosition == null || inFlight.GlobalEventPosition.Value > maxPosition.Value.Value
                ? inFlight.GlobalEventPosition
                : maxPosition;
        }

        if (selected.Count == 0 || maxPosition == null)
        {
            return ValueTask.FromResult<PersistencePullBatch<TState>?>(null);
        }

        return ValueTask.FromResult<PersistencePullBatch<TState>?>(
            new PersistencePullBatch<TState>
            {
                Items = selected,
                MaxPosition = maxPosition.Value,
            });
    }

    public ValueTask CompletePull(
        PersistencePullBatch<TState> batch,
        IReadOnlyList<PersistenceItemOutcome> outcomes,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        foreach (var outcome in outcomes)
        {
            if (!this.inFlightCache.TryGetValue(outcome.ModelId, out var entry)
                || entry.State.StagedVersionToken != outcome.StagedVersionToken)
            {
                continue;
            }

            var updatedState = outcome.Status switch
            {
                PersistenceItemOutcomeStatus.Persisted => entry.State with
                {
                    IsNew = false,
                    PersistenceStatus = ProjectionPersistenceStatus.Persisted,
                },
                PersistenceItemOutcomeStatus.Failed => entry.State with
                {
                    PersistenceStatus = ProjectionPersistenceStatus.Failed,
                },
                PersistenceItemOutcomeStatus.SkippedAsStale => entry.State with
                {
                    PersistenceStatus = ProjectionPersistenceStatus.Dirty,
                },
                _ => throw new ArgumentOutOfRangeException(),
            };

            if (!this.inFlightCache.TryUpdate(outcome.ModelId, new CacheEntry(updatedState), entry))
            {
                continue;
            }

            if (IsEvictable(updatedState.PersistenceStatus))
            {
                this.evictionQueue.Enqueue(new EvictionEntry(outcome.ModelId, updatedState.StagedVersionToken));
                this.TryEvictEligibleEntries();
            }
        }

        return ValueTask.CompletedTask;
    }

    public (long Hits, long Misses) ReadAndResetLookupStats()
    {
        var hits = Interlocked.Exchange(ref this.inFlightLookupHits, 0);
        var misses = Interlocked.Exchange(ref this.inFlightLookupMisses, 0);
        return (hits, misses);
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

    private static bool IsEvictable(ProjectionPersistenceStatus status)
    {
        return status == ProjectionPersistenceStatus.Persisted;
    }

    private void StoreEntry(ProjectedStateEnvelope<TState> state)
    {
        var isNewKey = this.inFlightCache.TryAdd(state.Model.Id, new CacheEntry(state));

        if (isNewKey)
        {
            Interlocked.Increment(ref this.approxInFlightCacheCount);
        }
        else
        {
            this.inFlightCache[state.Model.Id] = new CacheEntry(state);
        }

        if (IsEvictable(state.PersistenceStatus))
        {
            this.evictionQueue.Enqueue(new EvictionEntry(state.Model.Id, state.StagedVersionToken));
            this.TryEvictEligibleEntries();
        }
    }

    private void TryEvictEligibleEntries()
    {
        while (Volatile.Read(ref this.approxInFlightCacheCount) > this.inFlightCacheCapacity
               && this.evictionQueue.TryDequeue(out var candidate))
        {
            // Example:
            // 1) Set(A) -> token=10, queue has (A,10), map[A]=(modelState@10,10)
            // 2) Set(A) again -> token=11, queue has (A,10),(A,11), map[A]=(modelState@11,11)
            // 3) Eviction dequeues candidate (A,10)
            // 4) If we removed by key only, we'd delete modelState@11 (newest) by mistake.
            //    Therefore, we remove only when current token == candidate token.
            if (this.inFlightCache.TryGetValue(candidate.ModelId, out var current)
                && current.State.StagedVersionToken == candidate.Token
                && IsEvictable(current.State.PersistenceStatus)
                && this.inFlightCache.TryRemove(candidate.ModelId, out _))
            {
                Interlocked.Decrement(ref this.approxInFlightCacheCount);
            }
        }
    }

    private readonly record struct EvictionEntry(Guid ModelId, long Token);

    private readonly record struct CacheEntry(ProjectedStateEnvelope<TState> State);
}
