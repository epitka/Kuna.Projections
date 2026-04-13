namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Represents one batch of pulled cached projection snapshots to be written to
/// a durable store.
/// </summary>
public sealed record PersistenceWriteBatch<TState>
{
    /// <summary>
    /// Cached projection snapshots selected for the current durable write.
    /// </summary>
    public required IReadOnlyList<ProjectedStateEnvelope<TState>> Items { get; init; }
}
