using Kuna.Projections.Abstractions.Messages;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Persists batches of projection model states and their checkpoint position.
/// </summary>
public interface IModelStateSink<TState>
{
    Task PersistBatch(ModelStatesBatch<TState> batch, CancellationToken cancellationToken);
}
