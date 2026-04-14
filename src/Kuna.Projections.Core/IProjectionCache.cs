using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core;

/// <summary>
/// In-memory projection cache contract that supports cache-first lookups for
/// projection recreation and staged pull-based persistence.
/// </summary>
public interface IProjectionCache<TState>
    where TState : class, IModel, new()
{
    /// <summary>
    /// Looks up the latest cached snapshot for a model id.
    /// </summary>
    ValueTask<ProjectedStateEnvelope<TState>?> Get(Guid modelId, CancellationToken cancellationToken);

    /// <summary>
    /// Stages or replaces the latest cached snapshot for a model id.
    /// </summary>
    ValueTask Stage(ProjectedStateEnvelope<TState> state, CancellationToken cancellationToken);

    /// <summary>
    /// Pulls the next cache-backed batch eligible for durable persistence.
    /// </summary>
    ValueTask<PersistencePullBatch<TState>?> PullNextBatch(
        PersistencePullRequest request,
        CancellationToken cancellationToken);

    /// <summary>
    /// Completes a previously pulled batch using per-item persistence outcomes.
    /// </summary>
    ValueTask CompletePull(
        PersistencePullBatch<TState> batch,
        IReadOnlyList<PersistenceItemOutcome> outcomes,
        CancellationToken cancellationToken);

    /// <summary>
    /// Returns and resets cache lookup counters used for pipeline diagnostics.
    /// </summary>
    (long Hits, long Misses) ReadAndResetLookupStats();
}
