namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaSourceSettings
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = string.Empty;

    public string Topic { get; init; } = string.Empty;

    public string? ClientId { get; init; }

    public string? ConsumerGroupId { get; init; }

    public KafkaAutoOffsetReset AutoOffsetReset { get; init; } = KafkaAutoOffsetReset.Earliest;

    public KafkaKeyFormat KeyFormat { get; init; } = KafkaKeyFormat.Guid;

    public KafkaSourceTransformerKind Transformer { get; init; } = KafkaSourceTransformerKind.Native;

    public int[]? Partitions { get; init; }

    public int PollTimeoutMs { get; init; } = 1000;
}
