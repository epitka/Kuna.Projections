using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Provides the event source instance isolated for one projection model type.
/// </summary>
public interface IProjectionEventSource<TState>
    where TState : class, IModel, new()
{
    IEventSource<EventEnvelope> Value { get; }
}
