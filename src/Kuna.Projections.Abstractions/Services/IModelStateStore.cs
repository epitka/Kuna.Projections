using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Loads the current persisted state for a projection model by model id.
/// </summary>
public interface IModelStateStore<TState>
    where TState : class, IModel, new()
{
    Task<TState?> Load(Guid modelId, CancellationToken cancellationToken);
}
