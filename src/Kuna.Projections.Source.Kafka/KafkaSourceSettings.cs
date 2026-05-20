namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaSourceSettings
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = string.Empty;

    public string Topic { get; init; } = string.Empty;

    public string? ClientId { get; init; }

    public string ConsumerGroupId { get; init; } = string.Empty;

    public KafkaAutoOffsetReset AutoOffsetReset { get; init; } = KafkaAutoOffsetReset.Earliest;

    public KafkaKeyFormat KeyFormat { get; init; } = KafkaKeyFormat.Guid;

    public int[]? Partitions { get; init; }

    public int PollTimeoutMs { get; init; } = 1000;
}
