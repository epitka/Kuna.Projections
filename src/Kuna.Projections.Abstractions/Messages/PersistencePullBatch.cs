using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Represents one batch pulled from the projection cache for durable
/// persistence.
/// </summary>
public sealed record PersistencePullBatch<TState>
{
    /// <summary>
    /// Cached projection snapshots selected for the current pull.
    /// </summary>
    public required IReadOnlyList<ProjectedStateEnvelope<TState>> Items { get; init; }

    /// <summary>
    /// Highest global event position covered by the pulled batch.
    /// </summary>
    public required GlobalEventPosition MaxPosition { get; init; }
}
