using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Source.Kurrent;

internal sealed class ProjectionEventSource<TState> : IProjectionEventSource<TState>
    where TState : class, IModel, new()
{
    public ProjectionEventSource(IEventSource<EventEnvelope> value)
    {
        this.Value = value;
    }

    public IEventSource<EventEnvelope> Value { get; }
}
