using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Attributes;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public abstract class InitialEventAttribute : Attribute
{
    protected InitialEventAttribute(Type initialEventType)
    {
        ArgumentNullException.ThrowIfNull(initialEventType);

        if (!typeof(Event).IsAssignableFrom(initialEventType))
        {
            throw new ArgumentException(
                $"Initial event type must inherit from {typeof(Event).FullName}.",
                nameof(initialEventType));
        }

        this.InitialEventType = initialEventType;
    }

    public Type InitialEventType { get; }
}

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class InitialEventAttribute<TEvent> : InitialEventAttribute
    where TEvent : Event
{
    public InitialEventAttribute()
        : base(typeof(TEvent))
    {
    }
}
