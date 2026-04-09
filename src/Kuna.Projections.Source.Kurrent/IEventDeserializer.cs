using System.Globalization;
using System.Text;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Deserializes raw source event payloads into projection event instances.
/// </summary>
public interface IEventDeserializer
{
    /// <summary>
    /// Deserializes one source event payload into a projection event instance.
    /// </summary>
    Event Deserialize(byte[] eventData, string eventTypeName, long globalEventNumber);
}

/// <summary>
/// Default JSON-based <see cref="IEventDeserializer"/> for Kurrent events.
/// It resolves event CLR types by source event name and returns
/// <see cref="UnknownEvent"/> when no registered type matches.
/// </summary>
public class EventDeserializer : IEventDeserializer
{
    private static readonly JsonSerializerSettings SerializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        Formatting = Formatting.None,
        NullValueHandling = NullValueHandling.Include,
    };

    private readonly ILogger logger;
    private readonly Dictionary<string, Type> eventTypes;

    /// <summary>
    /// Initializes the deserializer with the known event CLR types that may
    /// appear in the source stream.
    /// </summary>
    public EventDeserializer(
        Type[] eventTypes,
        ILogger<EventDeserializer> logger)
    {
        this.eventTypes = eventTypes.ToDictionary(x => x.Name, v => v, StringComparer.OrdinalIgnoreCase);
        this.logger = logger;
    }

    private static ReadOnlySpan<byte> Utf8Bom => [0xEF, 0xBB, 0xBF,];

    /// <summary>
    /// Deserializes one raw event payload into the corresponding projection
    /// event type, or <see cref="UnknownEvent"/> when the source event name is
    /// not mapped to a registered CLR event type.
    /// </summary>
    public Event Deserialize(byte[] eventData, string eventTypeName, long globalEventNumber)
    {
        this.eventTypes.TryGetValue(eventTypeName, out var eventType);

        if (eventType == null)
        {
            var unknownEvent = new UnknownEvent
            {
                TypeName = nameof(UnknownEvent),
                UnknownEventName = eventTypeName,
            };

            try
            {
                var obj = JObject.Parse(Encoding.UTF8.GetString(eventData));

                if (obj.TryGetValue(nameof(Event.CreatedOn), StringComparison.OrdinalIgnoreCase, out var createdOnToken))
                {
                    var createdOnRaw = createdOnToken?.ToString();

                    if (!string.IsNullOrWhiteSpace(createdOnRaw)
                        && DateTime.TryParse(
                            createdOnRaw,
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind,
                            out var parsed))
                    {
                        unknownEvent.CreatedOn = parsed;
                    }
                }
            }
            catch
            {
                // Keep defaults if payload is malformed; caller still gets UnknownEvent.
            }

            return unknownEvent;
        }

        try
        {
            var @event = JsonConvert.DeserializeObject(
                             Encoding.UTF8.GetString(eventData),
                             eventType!,
                             SerializerSettings) as Event;

            @event!.TypeName = eventType.Name;

            return @event;
        }
        catch (Exception ex)
        {
            this.logger.LogError(
                ex,
                "Could not deserialize event {EventName} at {EventNumber}",
                eventTypeName,
                globalEventNumber);

            var data = new ReadOnlySpan<byte>(eventData);

            if (data.StartsWith(Utf8Bom))
            {
                this.logger.LogWarning(
                    "Event {EventName} at {EventNumber} starts with BOM, trying to deserialize without BOM",
                    eventTypeName,
                    globalEventNumber);

                data = data[Utf8Bom.Length..];
                return this.Deserialize(data.ToArray(), eventTypeName, globalEventNumber);
            }

            this.logger.LogError(
                ex,
                "Cannot deserialize event {EventType} - {GlobalEventNumber}",
                eventType,
                globalEventNumber);

            throw;
        }
    }
}
