using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Represents the current persisted-state view of a projection model after an
/// event has been applied. It is the data shape passed from transformation to
/// downstream batching, caching, and sink persistence, and includes both the
/// model itself and the metadata needed to decide whether the model should be
/// inserted, updated, deleted, or concurrency-checked.
/// </summary>
public sealed record ModelState<TState>(
    TState Model,
    bool IsNew,
    bool ShouldDelete,
    GlobalEventPosition GlobalEventPosition,
    long? ExpectedEventNumber);
