using System.Globalization;
using System.Text;

namespace Kuna.Projections.Source.Kafka;

public sealed class NativeKafkaSourceTransformer : IKafkaSourceTransformer
{
    public KafkaSourceRecord Transform(KafkaSourceRecordContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.KeyBytes is null
            || context.KeyBytes.Length == 0)
        {
            throw new InvalidOperationException("Kafka record key is required.");
        }

        var key = Encoding.UTF8.GetString(context.KeyBytes);

        if (!Guid.TryParse(key, out var modelId))
        {
            throw new InvalidOperationException($"Kafka record key '{key}' is not a valid Guid.");
        }

        var eventType = ReadRequiredHeader(context.Headers, "event-type");
        var eventNumberRaw = ReadRequiredHeader(context.Headers, "event-number");

        if (!long.TryParse(eventNumberRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var eventNumber))
        {
            throw new InvalidOperationException($"Kafka header 'event-number' value '{eventNumberRaw}' is not a valid Int64.");
        }

        var createdOn = ReadCreatedOn(context);
        var streamId = ReadOptionalHeader(context.Headers, "stream-id")
                       ?? $"{context.Topic}-{modelId:D}";

        return new KafkaSourceRecord
        {
            EventType = eventType,
            EventNumber = eventNumber,
            ModelId = modelId,
            CreatedOn = createdOn,
            StreamId = streamId,
            EventData = context.ValueBytes,
        };
    }

    private static DateTime ReadCreatedOn(KafkaSourceRecordContext context)
    {
        var createdOnRaw = ReadOptionalHeader(context.Headers, "created-on");

        if (!string.IsNullOrWhiteSpace(createdOnRaw))
        {
            if (DateTime.TryParse(
                    createdOnRaw,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind,
                    out var createdOn))
            {
                return createdOn;
            }

            throw new InvalidOperationException($"Kafka header 'created-on' value '{createdOnRaw}' is not a valid timestamp.");
        }

        if (context.TimestampUtc.HasValue)
        {
            return context.TimestampUtc.Value;
        }

        throw new InvalidOperationException("Kafka record must include a valid 'created-on' header or timestamp.");
    }

    private static string ReadRequiredHeader(
        IReadOnlyDictionary<string, byte[]> headers,
        string headerName)
    {
        var value = ReadOptionalHeader(headers, headerName);

        if (!string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        throw new InvalidOperationException($"Kafka header '{headerName}' is required.");
    }

    private static string? ReadOptionalHeader(
        IReadOnlyDictionary<string, byte[]> headers,
        string headerName)
    {
        if (!headers.TryGetValue(headerName, out var valueBytes)
            || valueBytes.Length == 0)
        {
            return null;
        }

        return Encoding.UTF8.GetString(valueBytes);
    }
}
