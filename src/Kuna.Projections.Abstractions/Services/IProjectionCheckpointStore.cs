using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Provides the checkpoint store instance isolated for one projection model type.
/// </summary>
public interface IProjectionCheckpointStore<TState>
    where TState : class, IModel, new()
{
    ICheckpointStore Value { get; }
}
