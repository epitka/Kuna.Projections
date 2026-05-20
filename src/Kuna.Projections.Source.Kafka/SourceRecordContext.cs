namespace Kuna.Projections.Source.Kafka;

public sealed class SourceRecordContext
{
    public required string Topic { get; init; }

    public required int Partition { get; init; }

    public required long Offset { get; init; }

    public byte[]? KeyBytes { get; init; }

    public required byte[] ValueBytes { get; init; }

    public IReadOnlyDictionary<string, byte[]> Headers { get; init; } = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);

    public DateTime? TimestampUtc { get; init; }
}
