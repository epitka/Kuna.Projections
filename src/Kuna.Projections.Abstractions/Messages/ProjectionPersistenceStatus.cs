namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Tracks where a cached projection snapshot currently sits in the staged
/// persistence lifecycle.
/// </summary>
public enum ProjectionPersistenceStatus
{
    Dirty = 0,
    InFlight = 1,
    Persisted = 2,
    Failed = 3,
}
