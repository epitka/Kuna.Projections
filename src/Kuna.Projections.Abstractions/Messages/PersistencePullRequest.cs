namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Describes how many items the persistence stage wants to pull from the
/// projection cache.
/// </summary>
public sealed record PersistencePullRequest
{
    /// <summary>
    /// Maximum number of cached model snapshots to return in a single pull.
    /// </summary>
    public required int MaxBatchSize { get; init; }
}
