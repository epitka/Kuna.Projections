namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaSourceRecord
{
    public required string EventType { get; init; }

    public required long EventNumber { get; init; }

    public required Guid ModelId { get; init; }

    public required DateTime CreatedOn { get; init; }

    public required string StreamId { get; init; }

    public required byte[] EventData { get; init; }
}
