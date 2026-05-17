using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Kuna.Projections.Source.Kafka;

public sealed class KurrentKafkaSourceTransformer : IKafkaSourceTransformer
{
    public KafkaSourceRecord Transform(KafkaSourceRecordContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        using var document = JsonDocument.Parse(context.ValueBytes);
        var root = document.RootElement;

        var streamId = ReadStreamId(context, root);
        var modelId = ResolveModelId(streamId);
        var eventType = ReadEventType(context, root);
        var eventNumber = ReadEventNumber(context, root);
        var createdOn = ReadCreatedOn(context, root);
        var eventData = ReadValuePayload(root);

        return new KafkaSourceRecord
        {
            EventType = eventType,
            EventNumber = eventNumber,
            ModelId = modelId,
            CreatedOn = createdOn,
            StreamId = streamId,
            EventData = eventData,
        };
    }

    private static string ReadStreamId(
        KafkaSourceRecordContext context,
        JsonElement root)
    {
        var streamId = ReadOptionalHeader(context.Headers, "esdb-record-stream-id")
                       ?? TryReadNestedString(root, "position", "streamId");

        if (!string.IsNullOrWhiteSpace(streamId))
        {
            return streamId;
        }

        throw new InvalidOperationException("Kurrent Kafka record does not contain a stream id.");
    }

    private static Guid ResolveModelId(string streamId)
    {
        var separatorIndex = streamId.IndexOf('-', StringComparison.Ordinal);

        if (separatorIndex >= 0
            && separatorIndex < streamId.Length - 1
            && Guid.TryParse(streamId[(separatorIndex + 1)..], out var modelId))
        {
            return modelId;
        }

        throw new InvalidOperationException(
            $"Kurrent Kafka stream id '{streamId}' does not contain a Guid model id suffix.");
    }

    private static string ReadEventType(
        KafkaSourceRecordContext context,
        JsonElement root)
    {
        var eventType = ReadOptionalHeader(context.Headers, "esdb-record-schema-subject")
                        ?? TryReadNestedString(root, "schemaInfo", "subject")
                        ?? TryReadNestedString(root, "schemaInfo", "Subject");

        if (!string.IsNullOrWhiteSpace(eventType))
        {
            return eventType;
        }

        throw new InvalidOperationException("Kurrent Kafka record does not contain an event type.");
    }

    private static long ReadEventNumber(
        KafkaSourceRecordContext context,
        JsonElement root)
    {
        var eventNumberRaw = ReadOptionalHeader(context.Headers, "esdb-record-stream-revision")
                             ?? TryReadNestedString(root, "headers", "esdb-record-stream-revision");

        if (!string.IsNullOrWhiteSpace(eventNumberRaw)
            && long.TryParse(eventNumberRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var eventNumber))
        {
            return eventNumber;
        }

        throw new InvalidOperationException("Kurrent Kafka record does not contain a valid stream revision.");
    }

    private static DateTime ReadCreatedOn(
        KafkaSourceRecordContext context,
        JsonElement root)
    {
        var createdOnRaw = ReadOptionalHeader(context.Headers, "esdb-record-timestamp")
                           ?? TryReadNestedString(root, "headers", "esdb-record-timestamp");

        if (!string.IsNullOrWhiteSpace(createdOnRaw)
            && DateTime.TryParse(
                createdOnRaw,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var createdOn))
        {
            return createdOn;
        }

        if (context.TimestampUtc.HasValue)
        {
            return context.TimestampUtc.Value;
        }

        throw new InvalidOperationException("Kurrent Kafka record does not contain a valid record timestamp.");
    }

    private static byte[] ReadValuePayload(JsonElement root)
    {
        if (!TryGetProperty(root, "value", out var valueElement))
        {
            throw new InvalidOperationException("Kurrent Kafka record does not contain a value payload.");
        }

        return Encoding.UTF8.GetBytes(valueElement.GetRawText());
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

    private static string? TryReadNestedString(
        JsonElement root,
        string objectPropertyName,
        string nestedPropertyName)
    {
        if (!TryGetProperty(root, objectPropertyName, out var objectElement)
            || objectElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        if (!TryGetProperty(objectElement, nestedPropertyName, out var valueElement))
        {
            return null;
        }

        return valueElement.ValueKind == JsonValueKind.String
                   ? valueElement.GetString()
                   : valueElement.ToString();
    }

    private static bool TryGetProperty(
        JsonElement element,
        string propertyName,
        out JsonElement value)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
