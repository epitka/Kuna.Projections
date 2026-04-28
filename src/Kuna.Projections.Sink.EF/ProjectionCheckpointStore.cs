using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Sink.EF;

internal sealed class ProjectionCheckpointStore<TState> : IProjectionCheckpointStore<TState>
    where TState : class, IModel, new()
{
    public ProjectionCheckpointStore(ICheckpointStore value)
    {
        this.Value = value;
    }

    public ICheckpointStore Value { get; }
}
