using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionCheckpointStoreAdapter<TState> : IProjectionCheckpointStore<TState>
    where TState : class, IModel, new()
{
    public ProjectionCheckpointStoreAdapter(ICheckpointStore value)
    {
        this.Value = value;
    }

    public ICheckpointStore Value { get; }
}
