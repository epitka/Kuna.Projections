using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Represents the latest cached snapshot of one projection model together with
/// the metadata needed for staged pull-based persistence.
/// </summary>
public sealed record ProjectedStateEnvelope<TState>(
    TState Model,
    bool IsNew,
    bool ShouldDelete,
    GlobalEventPosition GlobalEventPosition,
    long? ExpectedEventNumber,
    long StageToken,
    ProjectionPersistenceStatus PersistenceStatus);
