using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Represents the persistence result for one pulled cached projection snapshot.
/// </summary>
public sealed record PersistenceItemOutcome(
    Guid ModelId,
    long StageToken,
    GlobalEventPosition GlobalEventPosition,
    PersistenceItemOutcomeStatus Status,
    Exception? Exception);
