using System.Globalization;
using System.Text;

namespace Kuna.Projections.Source.Kafka;

/// <summary>
/// Converts a consumed Kafka record into the normalized source record shape used
/// by the projection pipeline.
/// </summary>
/// <remarks>
/// The built-in <see cref="SourceTransformer"/> supports this library's
/// Kafka record format: a Guid key for the model id, event metadata in headers,
/// and serialized event data in the record value. Applications that store events
/// in Kafka with a different shape can register their own keyed
/// <see cref="ISourceTransformer"/> for the projection.
/// </remarks>
public interface ISourceTransformer
{
    /// <summary>
    /// Maps the raw Kafka record context to the event metadata and payload
    /// required by the projection pipeline.
    /// </summary>
    /// <param name="context">The raw Kafka record context.</param>
    /// <returns>The normalized Kafka source record.</returns>
    SourceRecord Transform(SourceRecordContext context);
}

public sealed class SourceTransformer : ISourceTransformer
{
    public SourceRecord Transform(SourceRecordContext context)
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

        return new SourceRecord
        {
            EventType = eventType,
            EventNumber = eventNumber,
            ModelId = modelId,
            CreatedOn = createdOn,
            StreamId = streamId,
            EventData = context.ValueBytes,
        };
    }

    private static DateTime ReadCreatedOn(SourceRecordContext context)
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
