using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Handles projection failures that should be persisted or reported outside the pipeline.
/// </summary>
public interface IProjectionFailureHandler<TState>
    where TState : class, IModel, new()
{
    Task Handle(ProjectionFailure failure, CancellationToken cancellationToken);
}
