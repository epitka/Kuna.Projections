namespace Kuna.Projections.Source.Kafka;

public sealed class Checkpoint
{
    public string Topic { get; init; } = string.Empty;

    public Dictionary<int, long> Partitions { get; init; } = new();
}
