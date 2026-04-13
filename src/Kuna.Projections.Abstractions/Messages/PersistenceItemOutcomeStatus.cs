namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Describes the persistence outcome for one pulled cached projection snapshot.
/// </summary>
public enum PersistenceItemOutcomeStatus
{
    Persisted = 0,
    Failed = 1,
    SkippedAsStale = 2,
}
