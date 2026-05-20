using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Source.Kafka;

public sealed class EventEnvelopeFactory
{
    private readonly IEventDeserializer deserializer;

    public EventEnvelopeFactory(IEventDeserializer deserializer)
    {
        this.deserializer = deserializer;
    }

    public EventEnvelope Create(
        SourceRecord record,
        GlobalEventPosition eventPosition)
    {
        ArgumentNullException.ThrowIfNull(record);

        Event @event;

        try
        {
            @event = this.deserializer.Deserialize(
                record.EventData,
                record.EventType,
                record.EventNumber);
        }
        catch (Exception)
        {
            @event = new DeserializationFailed
            {
                EventNumber = record.EventNumber,
                GlobalEventPosition = eventPosition,
                CreatedOn = record.CreatedOn,
                ModelId = record.ModelId,
                TypeName = record.EventType,
            };
        }

        @event.CreatedOn = record.CreatedOn;

        return new EventEnvelope(
            eventNumber: record.EventNumber,
            streamPosition: eventPosition,
            streamId: record.StreamId,
            @event: @event,
            modelId: record.ModelId,
            createdOn: record.CreatedOn);
    }
}
