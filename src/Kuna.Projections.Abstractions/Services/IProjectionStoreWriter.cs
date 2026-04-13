using Kuna.Projections.Abstractions.Messages;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Writes pulled projection-state batches to a durable store and returns
/// per-item persistence outcomes.
/// </summary>
public interface IProjectionStoreWriter<TState>
{
    Task<IReadOnlyList<PersistenceItemOutcome>> WriteBatch(
        PersistenceWriteBatch<TState> batch,
        CancellationToken cancellationToken);
}
