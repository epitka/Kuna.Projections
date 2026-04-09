namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Runs the end-to-end projection pipeline from source through transformation and persistence.
/// </summary>
public interface IProjectionPipeline
{
    Task RunAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Runs the end-to-end projection pipeline from source through transformation and persistence.
/// </summary>
public interface IProjectionPipeline<TState> : IProjectionPipeline
    where TState : class, Models.IModel, new()
{
}
