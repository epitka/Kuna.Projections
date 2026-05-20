using Confluent.Kafka;

namespace Kuna.Projections.Source.Kafka;

internal static class ConsumeResultAdapter
{
    public static ConsumedMessage Adapt(ConsumeResult<byte[], byte[]> result)
    {
        ArgumentNullException.ThrowIfNull(result);

        var headers = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);

        if (result.Message.Headers != null)
        {
            foreach (var header in result.Message.Headers)
            {
                headers[header.Key] = header.GetValueBytes();
            }
        }

        return new ConsumedMessage
        {
            Topic = result.Topic,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value,
            KeyBytes = result.Message.Key,
            ValueBytes = result.Message.Value ?? [],
            Headers = headers,
            TimestampUtc = result.Message.Timestamp.UtcDateTime,
        };
    }
}
