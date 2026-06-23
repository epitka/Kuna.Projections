using System.Text.Json;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Creates projection <see cref="EventEnvelope"/> instances from raw EventSourcingDB
/// event metadata and payload.
/// </summary>
public interface IEventEnvelopeFactory
{
    /// <summary>
    /// Creates an envelope for one EventSourcingDB event, or returns
    /// <see langword="null"/> when the event cannot be associated with a model id
    /// for projection processing.
    /// </summary>
    EventEnvelope? Create(
        string subject,
        JsonElement data,
        string eventType,
        long eventNumber,
        GlobalEventPosition eventPosition,
        DateTime eventTime);
}

/// <summary>
/// Default <see cref="IEventEnvelopeFactory"/> implementation for EventSourcingDB
/// events. It deserializes the event payload, resolves the projection model id from
/// the subject, and combines both into an <see cref="EventEnvelope"/>.
/// </summary>
public class EventEnvelopeFactory : IEventEnvelopeFactory
{
    private readonly IEventDeserializer deserializer;
    private readonly IEventModelIdResolver modelIdResolver;

    /// <summary>
    /// Initializes the envelope factory with the event deserializer and model-id
    /// resolver used for EventSourcingDB events.
    /// </summary>
    public EventEnvelopeFactory(
        IEventDeserializer deserializer,
        IEventModelIdResolver modelIdResolver)
    {
        this.deserializer = deserializer;
        this.modelIdResolver = modelIdResolver;
    }

    /// <summary>
    /// Creates an envelope for an EventSourcingDB event. Deserialization failures are
    /// wrapped as <see cref="DeserializationFailed"/> so the projection can decide how
    /// to handle them later.
    /// </summary>
    public EventEnvelope? Create(
        string subject,
        JsonElement data,
        string eventType,
        long eventNumber,
        GlobalEventPosition eventPosition,
        DateTime eventTime)
    {
        Event @event;

        try
        {
            @event = this.deserializer.Deserialize(
                data,
                eventType,
                eventPosition.Value);
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

        if (!this.modelIdResolver.TryResolve(@event, subject, out var modelId))
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
            streamId: subject,
            modelId: modelId,
            createdOn: eventTime,
            @event: @event);

        return msg;
    }
}
