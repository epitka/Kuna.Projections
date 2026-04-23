using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core;

public sealed class ProjectionCreationRegistration<TState>
    where TState : class, IModel, new()
{
    public ProjectionCreationRegistration(Type? initialEventType)
    {
        if (initialEventType != null
            && !typeof(Event).IsAssignableFrom(initialEventType))
        {
            throw new ArgumentException($"Initial event type must inherit from {typeof(Event).FullName}.", nameof(initialEventType));
        }

        this.InitialEventType = initialEventType;
    }

    public Type? InitialEventType { get; }
}
