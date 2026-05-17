using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Source.Kafka;

public interface IKafkaEventDeserializer
{
    Event Deserialize(byte[] eventData, string eventTypeName, long eventNumber);
}
