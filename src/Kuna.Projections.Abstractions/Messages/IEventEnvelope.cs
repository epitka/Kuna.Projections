using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

public interface IEventEnvelope
{
    Event Event { get; }

    Guid ModelId { get; }

    long EventNumber { get; }

    GlobalEventPosition GlobalEventPosition { get; }

    string StreamId { get; }

    DateTime CreatedOn { get; }
}

public readonly struct EventEnvelope : IEventEnvelope
{
    public EventEnvelope(
        long eventNumber,
        GlobalEventPosition streamPosition,
        string streamId,
        Event @event,
        Guid modelId,
        DateTime createdOn)
    {
        this.EventNumber = eventNumber;
        this.GlobalEventPosition = streamPosition;
        this.StreamId = streamId;
        this.Event = @event;
        this.ModelId = modelId;
        this.CreatedOn = createdOn;
    }

    public Event Event { get; }

    public Guid ModelId { get; }

    public long EventNumber { get; }

    public GlobalEventPosition GlobalEventPosition { get; }

    public string StreamId { get; }

    public DateTime CreatedOn { get; }
}
