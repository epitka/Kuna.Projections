using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionFailureHandler<TState> : IProjectionFailureHandler<TState>
    where TState : class, IModel, new()
{
    public Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
