namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaCheckpointDocument
{
    public string Topic { get; init; } = string.Empty;

    public Dictionary<int, long> Partitions { get; init; } = new();
}
