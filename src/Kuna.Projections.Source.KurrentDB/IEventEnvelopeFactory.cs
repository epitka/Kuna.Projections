using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Source.KurrentDB;

/// <summary>
/// Creates projection <see cref="EventEnvelope"/> instances from raw source
/// event metadata and payload bytes.
/// </summary>
public interface IEventEnvelopeFactory
{
    /// <summary>
    /// Creates an envelope for one source event, or returns
    /// <see langword="null"/> when the event cannot be associated with a model
    /// id for projection processing.
    /// </summary>
    EventEnvelope? Create(
        string streamId,
        byte[] eventData,
        string eventType,
        long eventNumber,
        GlobalEventPosition eventPosition,
        DateTime eventTime);
}

/// <summary>
/// Default <see cref="IEventEnvelopeFactory"/> implementation for Kurrent
/// events. It deserializes the event payload, resolves the projection model id,
/// and combines both into an <see cref="EventEnvelope"/>.
/// </summary>
public class EventEnvelopeFactory : IEventEnvelopeFactory
{
    private readonly IEventDeserializer deserializer;
    private readonly IEventModelIdResolver modelIdResolver;

    /// <summary>
    /// Initializes the envelope factory with the event deserializer and
    /// model-id resolver used for source events.
    /// </summary>
    public EventEnvelopeFactory(
        IEventDeserializer deserializer,
        IEventModelIdResolver modelIdResolver)
    {
        this.deserializer = deserializer;
        this.modelIdResolver = modelIdResolver;
    }

    /// <summary>
    /// Creates an envelope for a Kurrent event. Deserialization failures are
    /// wrapped as <see cref="DeserializationFailed"/> so the projection can
    /// decide how to handle them later.
    /// </summary>
    public EventEnvelope? Create(
        string streamId,
        byte[] eventData,
        string eventType,
        long eventNumber,
        GlobalEventPosition eventPosition,
        DateTime eventTime)
    {
        Event @event;
        Guid modelId;

        try
        {
            @event = this.deserializer.Deserialize(
                eventData,
                eventType,
                eventNumber);
        }
        catch (Exception)
        {
            @event = new DeserializationFailed
            {
                EventNumber = eventNumber,
                GlobalEventPosition = eventPosition,
                CreatedOn = eventTime,
                TypeName = eventType,
            };
        }

        @event.CreatedOn = eventTime;

        if (!this.modelIdResolver.TryResolve(@event, streamId, out modelId))
        {
            return null;
        }

        if (@event is DeserializationFailed failure)
        {
            failure.ModelId = modelId;
        }

        var msg = new EventEnvelope(
            eventNumber: eventNumber,
            streamPosition: eventPosition,
            streamId: streamId,
            modelId: modelId,
            createdOn: eventTime,
            @event: @event);

        return msg;
    }
}
